from copy import deepcopy
import json
from pathlib import Path

import pytest
import yaml

from broker_analysis.pipeline import ROOT, aggregate, analyze, histogram, run_summary, sha256


def fixture_run(path: Path, name: str = 'fixture') -> Path:
    path.mkdir(parents=True)
    s = yaml.safe_load((ROOT / 'scenarios/smoke/nats.yaml').read_text())
    s['traffic']['total_rate_messages_per_sec'] = 10
    counters = {k: 0 for k in ['sent', 'delivered', 'producer_accepted', 'delivered_window', 'consumer_acked',
                              'errors', 'lost', 'duplicate', 'unexpected', 'reordered', 'disconnects',
                              'connections_established', 'connections_failed']}
    counters.update(sent=100, producer_accepted=100, producer_accepted_window=100,
                    delivered=1000, unique_delivered=1000, delivered_window=1000, unique_delivered_window=1000,
                    connections_established=20)
    def write(name, value, lines=False):
        p = path / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(''.join(json.dumps(v) + '\n' for v in value) if lines else json.dumps(value))
    (path / 'scenario.yaml').write_text(yaml.safe_dump(s))
    write('correctness/summary.json', counters)
    hist = {'schema_version': 1, 'encoding': 'hdr-go-snapshot-v1', 'unit': 'microseconds',
            'count': 1000, 'distribution': [{'From': 1000, 'To': 1000, 'Count': 1000}]}
    write('latency/delivery.hdr.json', hist)
    write('latency/publish.hdr.json', dict(hist, count=100,
          distribution=[{'From': 1000, 'To': 1000, 'Count': 100}]))
    write('logs/runner.log', [{'stage': 'MEASURE', 'time': '2026-01-01T00:00:00Z'}], True)
    generators, brokers = [], []
    for i in range(1, 10):
        at = f'2026-01-01T00:00:0{i}Z'
        generators.append({'time': at, 'cpu_ticks': i * 10, 'rss_bytes': 100000, 'sockets': 20,
                           'counters': dict(counters, sent=i * 10, delivered_window=i * 100)})
        brokers.append({'time': at, 'files': {
            '/proc/1/status': 'VmRSS: 100 kB\nThreads: 4',
            '/proc/1/io': f'read_bytes: {i * 100}\nwrite_bytes: {i * 500}',
            '/sys/fs/cgroup/cpu.stat': f'usage_usec {i * 100000}',
            '/proc/net/dev': f'eth0: {i * 100} 10 0 0 0 0 0 0 {i * 1000} 100 0 0 0 0 0 0',
            '/proc/net/tcp': '0: 0100007F:107E 0100007F:C001 01',
        }})
    write('metrics/generator.jsonl', generators, True)
    write('metrics/broker-proc.jsonl', brokers, True)
    manifest = {'manifest_version': 1, 'run_id': name, 'status': 'completed', 'failure_stage': '',
                'failure_reason': '', 'started_at_utc': '2026-01-01T00:00:00Z',
                'finished_at_utc': '2026-01-01T00:00:11Z', 'warnings': [],
                'scenario': {'name': s['name'], 'sha256': sha256(path / 'scenario.yaml'),
                             'random_seed': s['random_seed'], 'repetition': 1, 'configuration': s},
                'source': {'commit': 'fixture', 'dirty': False},
                'sut': {'image_digest': 'nats@sha256:' + 'a' * 64, 'version': 'test',
                        'configuration_sha256': 'b' * 64, 'image': 'nats:test', 'broker': 'nats',
                        'profile': 'fast', 'protocol': 'NATS', 'client_library': 'nats.go',
                        'client_library_version': 'fixture'},
                'host': {'clock_ticks_per_second': 100},
                'workload': {'requested_clients': 20, 'actual_logical_clients': 20, 'actual_connections': 1,
                             'requested_rate': 10, 'actual_offered_rate': 10, 'payload_bytes': 256,
                             'timing': s['timing']},
                'result': {'counters': counters, 'requested_messages': 100, 'generator_saturated': False,
                           'broker_saturated': None, 'correctness_invalid_reason': ''},
                'artifacts': {str(p.relative_to(path)): sha256(p) for p in path.rglob('*') if p.is_file()}}
    write('manifest.json', manifest)
    return path


def test_raw_analysis_and_repetitions(tmp_path):
    a = fixture_run(tmp_path / 'a', 'a')
    b = fixture_run(tmp_path / 'b', 'b')
    row, points = run_summary(a)
    assert row['p99'] == 1
    assert row['p99_9'] is None
    assert row['connections'] == 1
    assert row['rss_mean'] == 102400
    assert row['cpu_mean'] == pytest.approx(10)
    assert points
    report = analyze([a, b], tmp_path / 'output')
    assert report['accepted_runs'] == 2
    groups = json.loads((tmp_path / 'output/comparison.json').read_text())
    assert groups[0]['repetitions_observed'] == 2
    assert groups[0]['p99_mean'] == 1
    assert (tmp_path / 'output/comparison.parquet').exists()
    assert list((tmp_path / 'output').glob('*.svg'))
    assert list((tmp_path / 'output').glob('*.png'))


def test_checksum_and_invalid_runs(tmp_path):
    run = fixture_run(tmp_path / 'a')
    (run / 'correctness/summary.json').write_text('{}')
    with pytest.raises(ValueError, match='checksum'):
        run_summary(run)
    report = analyze([run], tmp_path / 'out')
    assert report['excluded_runs'] == 1
    assert not list((tmp_path / 'out').glob('*.png'))


def test_failed_and_saturated_runs(tmp_path):
    run = fixture_run(tmp_path / 'a')
    path = run / 'manifest.json'
    m = json.loads(path.read_text())
    m['result']['generator_saturated'] = True
    path.write_text(json.dumps(m))
    with pytest.raises(ValueError, match='saturated'):
        run_summary(run)
    assert not run_summary(run, include_invalid=True)[0]['valid']
    m['status'] = 'failed'
    m['failure_stage'] = 'CONNECT'
    m['failure_reason'] = 'fixture connect failure'
    path.write_text(json.dumps(m))
    with pytest.raises(ValueError, match='CONNECT'):
        run_summary(run, include_invalid=True)


def test_no_write_inside_raw_run(tmp_path):
    run = fixture_run(tmp_path / 'a')
    with pytest.raises(ValueError, match='immutable'):
        analyze([run], run / 'summary')


def test_histogram_and_single_repetition(tmp_path):
    run = fixture_run(tmp_path / 'a')
    row, _ = run_summary(run)
    assert aggregate([row])[0]['p99_ci95_low'] is None
    wrong = deepcopy(json.loads((run / 'latency/delivery.hdr.json').read_text()))
    wrong['count'] += 1
    (tmp_path / 'bad.json').write_text(json.dumps(wrong))
    with pytest.raises(ValueError, match='count mismatch'):
        histogram(tmp_path / 'bad.json')


def reseal_fixture(run):
    path = run / 'manifest.json'
    m = json.loads(path.read_text())
    m['artifacts'] = {name: sha256(run / name) for name in m['artifacts']}
    path.write_text(json.dumps(m))


def test_missing_cpu_is_invalid_and_remains_null(tmp_path):
    run = fixture_run(tmp_path / 'a')
    path = run / 'metrics/broker-proc.jsonl'
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    for row in rows:
        row['files'].pop('/sys/fs/cgroup/cpu.stat')
    path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
    reseal_fixture(run)
    with pytest.raises(ValueError, match='cpu_seconds'):
        run_summary(run)
    row, _ = run_summary(run, include_invalid=True)
    assert row['cpu_mean'] is None
    assert row['cpu_peak'] is None
    json.dumps(row, allow_nan=False)


def test_repetitions_ignore_live_frequency_but_preserve_governor(tmp_path):
    runs = [fixture_run(tmp_path / str(i), str(i)) for i in range(3)]
    for i, run in enumerate(runs):
        path = run / 'manifest.json'
        m = json.loads(path.read_text())
        m['host'].update(cpu=json.dumps({'lscpu': [
            {'field': 'Model name:', 'data': 'fixture CPU'},
            {'field': 'CPU(s) scaling MHz:', 'data': str(50 + i)},
        ]}), cpu_governor='performance' if i < 2 else 'powersave')
        path.write_text(json.dumps(m))
    rows = [run_summary(run)[0] for run in runs]
    assert rows[0]['group_id'] == rows[1]['group_id']
    assert rows[1]['group_id'] != rows[2]['group_id']


def test_impossible_correctness_is_rejected_even_when_including_invalid(tmp_path):
    run = fixture_run(tmp_path / 'a')
    path = run / 'correctness/summary.json'
    c = json.loads(path.read_text())
    c['lost'] = 1
    path.write_text(json.dumps(c))
    manifest_path = run / 'manifest.json'
    m = json.loads(manifest_path.read_text())
    m['result']['counters'] = c
    manifest_path.write_text(json.dumps(m))
    reseal_fixture(run)
    with pytest.raises(ValueError, match='correctness counters'):
        run_summary(run, include_invalid=True)


@pytest.mark.parametrize('field', ['image_digest', 'version', 'protocol', 'client_library_version'])
def test_completed_manifest_requires_reproducibility_identity(tmp_path, field):
    import jsonschema
    run = fixture_run(tmp_path / 'a')
    path = run / 'manifest.json'
    m = json.loads(path.read_text())
    m['sut'].pop(field)
    path.write_text(json.dumps(m))
    with pytest.raises(jsonschema.ValidationError):
        run_summary(run, include_invalid=True)


def test_archived_runs_regenerate_tables_and_plots(tmp_path):
    run = fixture_run(tmp_path / 'original')
    archive = tmp_path / 'archive'
    run.rename(archive)
    before = {str(p.relative_to(archive)): sha256(p) for p in archive.rglob('*') if p.is_file()}
    first, second = tmp_path / 'first', tmp_path / 'second'
    analyze([archive], first)
    analyze([archive], second)
    for name in ['runs.json', 'comparison.json', 'timeline.json']:
        assert (first / name).read_bytes() == (second / name).read_bytes()
    assert {p.name for p in first.glob('*.png')} == {p.name for p in second.glob('*.png')}
    assert list(second.glob('*.svg'))
    assert before == {str(p.relative_to(archive)): sha256(p) for p in archive.rglob('*') if p.is_file()}
