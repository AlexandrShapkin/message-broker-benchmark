import hashlib
import json
import subprocess
import sys
import tarfile
from pathlib import Path

from test_pipeline import fixture_run

ROOT = Path(__file__).resolve().parents[2]


def snapshot(tmp_path, compressed=False):
    base = tmp_path / 'source'
    run = fixture_run(base / 'message-broker-benchmark/results/a', 'a')
    if compressed:
        proc = run / 'metrics/broker-proc.jsonl'
        subprocess.run(['zstd', '-q', str(proc)], check=True)
        proc.unlink()
    partial = base / 'message-broker-benchmark/results/interrupted'
    partial.mkdir()
    (base / 'message-broker-benchmark/results/empty').mkdir()
    (partial / 'manifest.partial.json').write_text('{"status":"running"}')
    inventory = []
    for p in base.rglob('*'):
        if p.is_file():
            inventory.append({'path': str(p.relative_to(base)), 'size': p.stat().st_size,
                              'sha256': hashlib.sha256(p.read_bytes()).hexdigest()})
    inv = tmp_path / 'inventory.jsonl'
    inv.write_text(''.join(json.dumps(row) + '\n' for row in inventory))
    archive = tmp_path / 'source.tar.gz'
    with tarfile.open(archive, 'w:gz') as tar:
        tar.add(base / 'message-broker-benchmark', arcname='message-broker-benchmark')
    return archive, inv


def execute(archive, inventory, output):
    return subprocess.run([sys.executable, str(ROOT / 'scripts/baseline_archive.py'),
                           str(archive), str(inventory), str(output)], capture_output=True, text=True)


def test_compressed_snapshot_and_partial(tmp_path):
    archive, inventory = snapshot(tmp_path, compressed=True)
    output = tmp_path / 'out'
    result = execute(archive, inventory, output)
    assert result.returncode == 0, result.stderr
    audit = json.loads((output / 'audit.json').read_text())
    assert next(r for r in audit if r['run_id'] == 'a')['validated']
    assert next(r for r in audit if r['run_id'] == 'interrupted')['status'] == 'incomplete'
    assert json.loads((output / 'runs.json').read_text()) == []  # Smoke never becomes scientific.
    assert not json.loads((output / 'verification.json').read_text())['different']


def test_source_inventory_mismatch_fails(tmp_path):
    archive, inventory = snapshot(tmp_path)
    data = [json.loads(line) for line in inventory.read_text().splitlines()]
    data[0]['sha256'] = '0' * 64
    inventory.write_text(''.join(json.dumps(row) + '\n' for row in data))
    output = tmp_path / 'out'
    result = execute(archive, inventory, output)
    assert result.returncode != 0
    assert json.loads((output / 'verification.json').read_text())['different']


def test_publication_filters_dependencies_and_preserves_raw(tmp_path):
    base = tmp_path / 'source'
    raw = base / 'message-broker-benchmark/results/run/measurement.json'
    raw.parent.mkdir(parents=True)
    raw.write_bytes(b'{"unchanged": true}\n')
    private = base / 'message-broker-benchmark/.git/config'
    private.parent.mkdir()
    private.write_text('private local metadata')
    inventory = tmp_path / 'inventory.jsonl'
    inventory.write_text(''.join(json.dumps({'path': str(p.relative_to(base)), 'size': p.stat().st_size,
                                            'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}) + '\n'
                                 for p in base.rglob('*') if p.is_file()))
    archive = tmp_path / 'source.tar.gz'
    with tarfile.open(archive, 'w:gz') as t:
        t.add(base / 'message-broker-benchmark', arcname='message-broker-benchmark')
    output = tmp_path / 'published.tar.gz'
    result = subprocess.run([sys.executable, str(ROOT / 'scripts/package_baseline.py'), str(archive),
                             str(inventory), str(output)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    with tarfile.open(output) as t:
        assert 'message-broker-benchmark/.git/config' not in t.getnames()
        assert t.extractfile('message-broker-benchmark/results/run/measurement.json').read() == raw.read_bytes()
        assert json.loads(t.extractfile('INVENTORY.jsonl').read())['sha256'] == hashlib.sha256(raw.read_bytes()).hexdigest()
    restored = execute(output, output.with_suffix('.inventory.jsonl'), tmp_path / 'regenerated')
    assert restored.returncode == 0, restored.stderr
    directories = tmp_path / 'run-directories.txt'
    directories.write_text('run\nempty\n')
    verified = subprocess.run([sys.executable, str(ROOT / 'scripts/verify_baseline_dataset.py'),
                               '--inventory', str(output.with_suffix('.inventory.jsonl')),
                               '--run-directories', str(directories)], input=output.read_bytes(), capture_output=True)
    assert verified.returncode == 0, verified.stderr
    assert json.loads(verified.stdout)['empty_run_directories'] == ['empty']


def test_disk_alarm_retained_but_excluded_from_primary(tmp_path):
    import yaml
    archive, inventory = snapshot(tmp_path)
    base = tmp_path / 'source'
    run = base / 'message-broker-benchmark/results/a'
    scenario = yaml.safe_load((run / 'scenario.yaml').read_text())
    scenario['name'] = 'nir1_final_connection_density'
    (run / 'scenario.yaml').write_text(yaml.safe_dump(scenario))
    log = run / 'logs/broker.log'
    log.write_text('disk resource limit alarm set; Publishers will be blocked')
    manifest = json.loads((run / 'manifest.json').read_text())
    manifest['scenario'].update(name=scenario['name'], configuration=scenario,
                                sha256=hashlib.sha256((run / 'scenario.yaml').read_bytes()).hexdigest())
    manifest['artifacts']['scenario.yaml'] = manifest['scenario']['sha256']
    manifest['artifacts']['logs/broker.log'] = hashlib.sha256(log.read_bytes()).hexdigest()
    (run / 'manifest.json').write_text(json.dumps(manifest))
    inventory.write_text(''.join(json.dumps({'path': str(p.relative_to(base)), 'size': p.stat().st_size,
                                            'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}) + '\n'
                                 for p in base.rglob('*') if p.is_file()))
    with tarfile.open(archive, 'w:gz') as t:
        t.add(base / 'message-broker-benchmark', arcname='message-broker-benchmark')
    output = tmp_path / 'out'
    result = execute(archive, inventory, output)
    assert result.returncode == 0, result.stderr
    assert json.loads((output / 'runs.json').read_text()) == []
    observed = json.loads((output / 'observed-final-runs.json').read_text())
    assert observed[0]['host_disk_pressure']
    audit = json.loads((output / 'audit.json').read_text())
    assert next(r for r in audit if r['run_id'] == 'a')['validated']
    assert next(r for r in audit if r['run_id'] == 'empty')['status'] == 'incomplete'
