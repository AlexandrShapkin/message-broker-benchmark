"""Stream a server snapshot, verify its inventory, and analyze one run at a time.

No original file is changed. Needs zstd for historical compressed metrics.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import tarfile
import tempfile

import jsonschema
from pathlib import Path
from typing import Any

from broker_analysis.pipeline import ROOT, aggregate, load_json, run_summary, sha256, write_table



def validate_manifests(run: Path) -> list[dict[str, Any]]:
    checks = []
    schema = load_json(ROOT / 'schemas/manifest.schema.json')
    for name in ['manifest.json', 'manifest.partial.json']:
        path = run / name
        if not path.exists():
            continue
        check = {'run_id': run.name, 'file': name, 'sha256': sha256(path), 'schema_valid': False}
        try:
            data = load_json(path)
            check['status'] = data.get('status') if isinstance(data, dict) else None
            jsonschema.validate(data, schema)
            check['schema_valid'] = True
        except (ValueError, jsonschema.ValidationError) as exc:
            check['error'] = getattr(exc, 'message', str(exc))
        checks.append(check)
    return checks


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=Path)
    parser.add_argument('inventory', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--run-directories', type=Path, help='Preserved list including empty run directories')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    expected = {r['path']: r for line in args.inventory.read_text().splitlines() if (r := json.loads(line))}
    observed: dict[str, dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []
    timelines: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    secrets: list[str] = []
    pattern = re.compile(rb'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|gh[pousr]_[A-Za-z0-9]{30,}|AKIA[A-Z0-9]{16}|sk-proj-[A-Za-z0-9_-]{30,}')
    with tempfile.TemporaryDirectory(prefix='baseline-') as tmp:
        scratch = Path(tmp)
        current = None

        def finish() -> None:
            if current is None:
                return
            run = scratch / current
            run.mkdir(parents=True, exist_ok=True)
            record: dict[str, Any] = {'run_id': current, 'manifest_validation': validate_manifests(run)}
            manifest = run / 'manifest.json'
            try:
                m = json.loads(manifest.read_text())
                record.update(status=m['status'], failure_stage=m.get('failure_stage'),
                              failure_reason=m.get('failure_reason'), scenario=m['scenario']['name'],
                              broker=m['sut'].get('broker'), profile=m['sut'].get('profile'),
                              environment=m['host'], source=m['source'], sut=m['sut'],
                              workload=m.get('workload'), result=m.get('result'),
                              scenario_configuration=m['scenario'].get('configuration'))
                record['classification'] = ('synthetic' if m.get('result', {}).get('synthetic') else
                                            'scientific' if record['scenario'].startswith('nir1_final_') else
                                            'smoke' if record['scenario'] == 'smoke' else 'exploratory')
                proc = run / 'metrics/broker-proc.jsonl'
                if not proc.exists() and proc.with_suffix('.jsonl.zst').exists():
                    with proc.open('wb') as dest:
                        subprocess.run(['zstd', '-dc', str(proc.with_suffix('.jsonl.zst'))], stdout=dest, check=True)
                bad = []
                for name, digest in m.get('artifacts', {}).items():
                    artifact = (run / name).resolve()
                    if not artifact.is_relative_to(run.resolve()):
                        bad.append(name + ': unsafe path')
                    elif not artifact.exists() or sha256(artifact) != digest:
                        bad.append(name + ': missing or checksum mismatch')
                record['artifact_errors'] = bad
                if bad:
                    raise ValueError('; '.join(bad))
                row, timeline = run_summary(run)
                config = m['scenario']['configuration']
                row.update(duration_seconds=config['timing']['measurement_seconds'],
                           topology=config['topology']['type'], delivery=config['topology']['delivery'],
                           source_commit=m['source']['commit'], image_digest=m['sut'].get('image_digest'),
                           configuration_sha256=m['sut'].get('configuration_sha256'),
                           scenario_sha256=m['scenario']['sha256'],
                           generator_saturated=m['result'].get('generator_saturated'),
                           manifest_sha256=sha256(manifest),
                           **{k: m['result']['counters'].get(k) for k in [
                               'scheduled', 'queue_dropped', 'producer_accepted', 'consumer_acked',
                               'unique_delivered', 'unique_delivered_window']})
                row.update(hostname=m['host'].get('hostname'), kernel=m['host'].get('kernel'),
                           cpu_governor=m['host'].get('cpu_governor'), logical_cores=m['host'].get('logical_cores'),
                           client_library=m['sut'].get('client_library'),
                           client_library_version=m['sut'].get('client_library_version'),
                           warmup_seconds=config['timing']['warmup_seconds'],
                           cooldown_seconds=config['timing']['cooldown_seconds'], random_seed=config['random_seed'],
                           mode=config['sut']['mode'], ramp_clients_per_second=config['connection']['ramp_clients_per_second'])
                row.update({k: m['sut'].get(k) for k in ['publisher_ack', 'consumer_ack', 'persistence', 'replicas', 'protocol']})
                row['sut_mode'] = m['sut'].get('mode')
                scientific = record['scenario'].startswith('nir1_final_')
                record['host_disk_pressure'] = 'disk resource limit alarm set' in (
                    run / 'logs/broker.log').read_text(errors='replace') if (run / 'logs/broker.log').exists() else False
                row['host_disk_pressure'] = record['host_disk_pressure']
                record['classification'] = 'scientific' if scientific else (
                    'smoke' if record['scenario'] == 'smoke' else 'exploratory')
                record['included'] = scientific and not record['host_disk_pressure']
                record['reason'] = '' if scientific else 'Вспомогательный запуск; не финальная исследовательская серия'
                if record['host_disk_pressure']:
                    record['reason'] = 'Disk resource limit alarm; host disk pressure confounds broker capacity'
                record['validated'] = True
                rows.append(row)
                timelines.extend(timeline)
            except (ValueError, OSError, KeyError, TypeError, subprocess.CalledProcessError) as exc:
                record.update(included=False, validated=False, reason=str(exc))
                if not manifest.exists() or not record.get('status'):
                    record['status'] = 'incomplete'
            except Exception as exc:
                # Schema errors retain full diagnostics in audit, never become success.
                record.update(included=False, validated=False, reason=f'{type(exc).__name__}: {exc}')
            if record.get('classification') == 'synthetic':
                record['reason'] = 'Generator self-test is synthetic, not a broker measurement'
            if record.get('status') == 'incomplete':
                record['classification'] = 'incomplete'
                record['reason'] = ('Unreadable final manifest; incomplete attempt' if manifest.exists() else
                                    'No final manifest; incomplete attempt' if any(run.rglob('*')) else
                                    'Empty run directory; no manifest or measurements')
            audit.append(record)
            print(current, record.get('status'), record.get('included'), flush=True)
            shutil.rmtree(run)

        with tarfile.open(args.archive, 'r|gz') as archive:
            for member in archive:
                parts = Path(member.name).parts
                run_id = parts[2] if len(parts) >= 3 and parts[:2] == ('message-broker-benchmark', 'results') and not parts[2].startswith('.') else None
                if run_id != current:
                    finish()
                    current = run_id
                if not member.isfile():
                    continue
                stream = archive.extractfile(member)
                assert stream is not None
                destination = None
                if run_id and len(parts) > 3:
                    target = scratch.joinpath(run_id, *parts[3:])
                    if not target.resolve().is_relative_to(scratch.resolve()):
                        raise ValueError('unsafe archive path')
                    target.parent.mkdir(parents=True, exist_ok=True)
                    destination = target.open('wb')
                h = hashlib.sha256()
                tail = b''
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    h.update(block)
                    scanned = tail + block
                    if any(marker in scanned for marker in [b'-----BEGIN ', b'gh', b'AKIA', b'sk-proj-']) and pattern.search(scanned):
                        secrets.append(member.name)
                    tail = block[-256:]
                    if destination:
                        destination.write(block)
                if destination:
                    destination.close()
                if member.name == 'INVENTORY.jsonl':
                    if h.hexdigest() != sha256(args.inventory):
                        raise ValueError('Embedded inventory differs from supplied inventory')
                    continue
                observed[member.name] = {'path': member.name, 'size': member.size, 'sha256': h.hexdigest()}
            finish()
    if args.run_directories:
        known = {r['run_id'] for r in audit}
        for run_id in args.run_directories.read_text().splitlines():
            if run_id not in known:
                audit.append({'run_id': run_id, 'status': 'incomplete', 'included': False,
                              'validated': False, 'classification': 'incomplete', 'reason': 'Empty run directory; no manifest or measurements'})
    audit.sort(key=lambda r: r['run_id'])
    rows.sort(key=lambda r: r['run_id'])
    missing = sorted(set(expected) - set(observed))
    extra = sorted(set(observed) - set(expected))
    different = [p for p in expected.keys() & observed.keys() if expected[p] != observed[p]]
    verification = {'files': len(observed), 'bytes': sum(r['size'] for r in observed.values()),
                    'missing': missing, 'extra': extra, 'different': different,
                    'secret_pattern_files': sorted(set(secrets))}
    (args.output / 'verification.json').write_text(json.dumps(verification, indent=2) + '\n')
    (args.output / 'audit.json').write_text(json.dumps(audit, ensure_ascii=False, indent=2) + '\n')
    write_table(args.output / 'validated-runs', rows)
    included_ids = {r['run_id'] for r in audit if r['included']}
    included = [r for r in rows if r['run_id'] in included_ids]
    final_ids = {r['run_id'] for r in audit if r.get('classification') == 'scientific'}
    write_table(args.output / 'observed-final-runs', [r for r in rows if r['run_id'] in final_ids])
    write_table(args.output / 'runs', included)
    write_table(args.output / 'comparison', [
        {k: v for k, v in group.items() if '_ci95_' not in k} for group in aggregate(included)])
    write_table(args.output / 'timeline', [r for r in timelines if r['run_id'] in final_ids])
    if missing or extra or different:
        raise SystemExit('Archive verification failed; see verification.json')


if __name__ == '__main__':
    main()
