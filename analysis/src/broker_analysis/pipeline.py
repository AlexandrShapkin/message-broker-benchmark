"""Version 1 raw-data reader. Every report is derived without modifying runs."""
from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import statistics
from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import jsonschema
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

ROOT = Path(__file__).resolve().parents[3]


def load_json(path: Path) -> Any:
    with path.open() as stream:
        return json.load(stream)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def jsonl(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
    with path.open() as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def timestamp(value: str) -> float:
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def histogram(path: Path) -> dict[str, float | int | None]:
    h = load_json(path)
    if h['schema_version'] != 1 or h['encoding'] != 'hdr-go-snapshot-v1' or h['unit'] != 'microseconds':
        raise ValueError('unsupported histogram encoding')
    bars = h['distribution'] or []
    counts = [int(b['Count']) for b in bars]
    if any(c < 0 for c in counts) or sum(counts) != h['count']:
        raise ValueError('histogram count mismatch')
    if any(b['From'] > b['To'] for b in bars):
        raise ValueError('invalid histogram bucket')
    if any(a['To'] >= b['From'] for a, b in zip(bars, bars[1:])):
        raise ValueError('histogram buckets must be sorted and disjoint')
    n = h['count']
    result: dict[str, float | int | None] = {'count': n}
    for key, percentile in [('p50', .5), ('p90', .9), ('p95', .95), ('p99', .99), ('p99_9', .999), ('max', 1.)]:
        result[key] = None
        if n == 0 or (key == 'p99_9' and n < 10000):
            continue
        target = max(1, math.ceil(percentile * n))
        cumulative = 0
        for bar in bars:
            cumulative += bar['Count']
            if cumulative >= target:
                result[key] = float(bar['To']) / 1000  # conservatively use HDR bucket upper bound, ms
                break
    return result


def key_values(text: str) -> dict[str, float]:
    out = {}
    for line in text.splitlines():
        fields = line.replace(':', '').split()
        if len(fields) >= 2:
            try:
                out[fields[0]] = float(fields[1]) * (1024 if len(fields) > 2 and fields[2] == 'kB' else 1)
            except ValueError:
                pass
    return out


def proc_row(row: dict[str, Any], broker: str, protocol_port: int | None = None) -> dict[str, Any]:
    out = {'time': row['time']}
    files = row.get('files', {})
    pid = files.get('broker_pid', '1').strip()
    raw_status = files.get(f'/proc/{pid}/status', '')
    out['process_name'] = next((line.split(':', 1)[1].strip() for line in raw_status.splitlines()
                                if line.startswith('Name:')), None)
    status = key_values(raw_status)
    io = key_values(files.get(f'/proc/{pid}/io', ''))
    cpu = key_values(files.get('/sys/fs/cgroup/cpu.stat', ''))
    mem = key_values(files.get('/sys/fs/cgroup/memory.stat', ''))
    out.update(rss_bytes=status.get('VmRSS'), threads=status.get('Threads'),
               cpu_seconds=cpu['usage_usec'] / 1e6 if 'usage_usec' in cpu else None,
               disk_read=io.get('read_bytes'), disk_write=io.get('write_bytes'),
               throttled_usec=cpu.get('throttled_usec'))
    # Cgroup I/O includes the whole broker process tree and does not require
    # ptrace access to a broker that dropped privileges after startup.
    if '/sys/fs/cgroup/io.stat' in files:
        disk = dict(rbytes=0, wbytes=0, rios=0, wios=0)
        for line in files['/sys/fs/cgroup/io.stat'].splitlines():
            for field in line.split()[1:]:
                key, _, value = field.partition('=')
                if key in disk:
                    disk[key] += int(value)
        out.update(disk_read=disk['rbytes'], disk_write=disk['wbytes'],
                   disk_read_operations=disk['rios'], disk_write_operations=disk['wios'])
    fd_count = files.get('fd_count', '').strip()
    out['file_descriptors'] = int(fd_count) if fd_count else None
    current = files.get('/sys/fs/cgroup/memory.current', '').strip()
    out['working_set_bytes'] = max(0, int(current) - mem.get('inactive_file', 0)) if current else None
    net = files.get('/proc/net/dev', '')
    if net:
        rx = tx = packets_rx = packets_tx = 0
        for line in net.splitlines():
            if ':' not in line:
                continue
            values = line.split(':', 1)[1].split()
            if len(values) >= 16:
                rx += int(values[0])
                packets_rx += int(values[1])
                tx += int(values[8])
                packets_tx += int(values[9])
        out.update(network_rx=rx, network_tx=tx, packets_rx=packets_rx, packets_tx=packets_tx)
    port = protocol_port or {'nats': 4222, 'mosquitto': 1883, 'rabbitmq': 5672, 'kafka': 9092}.get(broker)
    tcp = files.get('/proc/net/tcp', '') + files.get('/proc/net/tcp6', '')
    if tcp and port:
        count = 0
        for line in tcp.splitlines():
            fields = line.split()
            if len(fields) > 3 and fields[3] == '01' and int(fields[1].rsplit(':', 1)[1], 16) == port:
                count += 1
        out['connections'] = count
    return out


def network_controls(raw: Any) -> Any:
    """Canonicalize generated Docker link identities, retaining network controls."""
    links = json.loads(raw) if isinstance(raw, str) else raw
    if links is None:
        return None
    bridges = {link['ifname'] for link in links
               if re.fullmatch(r'br-[0-9a-f]{12}', link.get('ifname', ''))}
    normalized = []
    components: dict[str, dict[str, Any]] = {name: {'peers': []} for name in bridges}
    for link in links:
        row = dict(link)
        bridge = row.get('ifname') in bridges
        peer = (row.get('master') in bridges and 'link_netnsid' in row
                and row.get('ifname', '').startswith('veth'))
        if bridge or peer:
            row['ifname'] = '<docker-bridge>' if bridge else '<docker-veth>'
            if peer:
                row['master'] = '<docker-bridge>'
            for key in ['ifindex', 'link_index', 'link_netnsid', 'address']:
                row.pop(key, None)
        if bridge:
            components[link['ifname']]['bridge'] = row
        elif peer:
            components[link['master']]['peers'].append(row)
        else:
            normalized.append(row)
    for component in components.values():
        component['peers'].sort(key=lambda row: json.dumps(row, sort_keys=True))
        normalized.append(component)
    return sorted(normalized, key=lambda row: json.dumps(row, sort_keys=True))


def host_controls(host: dict[str, Any]) -> dict[str, Any]:
    """Exclude live CPU frequency/free memory from repetition identity."""
    stable = {k: host.get(k) for k in ['hostname', 'os', 'kernel', 'architecture', 'logical_cores',
                                     'cpu_governor', 'ulimit_nofile', 'disk', 'nic']}
    stable['nic'] = network_controls(host.get('nic'))
    raw_cpu = host.get('cpu')
    cpu = json.loads(raw_cpu) if isinstance(raw_cpu, str) else (raw_cpu or {})
    fields = {'Architecture', 'CPU(s)', 'Thread(s) per core', 'Core(s) per socket', 'Socket(s)',
              'Vendor ID', 'Model name', 'CPU family', 'Model', 'Stepping', 'Virtualization type'}
    stable['cpu'] = {r['field'].rstrip(':'): r['data'] for r in cpu.get('lscpu', [])
                     if r.get('field', '').rstrip(':') in fields}
    stable['memory_bytes'] = key_values(host.get('memory') or '').get('MemTotal')
    # /proc/swaps includes a changing Used column. Compare devices/type/size/priority only.
    stable['swap'] = [line.split()[:3] + line.split()[4:] for line in
                      (host.get('swap') or '').splitlines()[1:]]
    return stable


def finite(values: list[Any]) -> list[float]:
    return [float(x) for x in values if isinstance(x, (int, float)) and math.isfinite(x)]


def mean_peak(rows: list[dict[str, Any]], key: str) -> tuple[float | None, float | None]:
    values = finite([r.get(key) for r in rows])
    return (statistics.mean(values), max(values)) if values else (None, None)


def counter_total(rows: list[dict[str, Any]], key: str) -> float | None:
    values = finite([r.get(key) for r in rows])
    if len(values) < 2:
        return None
    # Negative deltas signal a process/container restart; preserve the new epoch.
    return sum(b - a if b >= a else b for a, b in zip(values, values[1:]))


def safe_ratio(a: float | None, b: float | None) -> float | None:
    return a / b if a is not None and b else None


def window_percentile(h: dict[str, Any], fraction: float) -> float | None:
    if not h.get('count'):
        return None
    target = math.ceil(h['count'] * fraction)
    seen = 0
    for bar in h['distribution']:
        seen += bar['Count']
        if seen >= target:
            return bar['To'] / 1000
    raise ValueError('window histogram count mismatch')


def recovery(points: list[dict[str, Any]], event_time: float) -> float | None:
    baseline = finite([p['delivered_rate'] for p in points if p['seconds'] < event_time])
    if not baseline or statistics.mean(baseline) <= 0:
        return None
    threshold = .9 * statistics.mean(baseline)
    after = [p for p in points if p['seconds'] >= event_time]
    for i in range(len(after) - 2):
        if all(p['delivered_rate'] >= threshold for p in after[i:i + 3]):
            return after[i]['seconds'] - event_time
    return None


def run_summary(run: Path, include_invalid: bool = False) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    m = load_json(run / 'manifest.json')
    jsonschema.validate(m, load_json(ROOT / 'schemas/manifest.schema.json'))
    if m['status'] != 'completed':
        raise ValueError(f"run status: {m['status']}; stage: {m['failure_stage']}; {m['failure_reason']}")
    if m['result'].get('synthetic'):
        raise ValueError('generator self-test is not a broker measurement')
    artifacts = m['artifacts']
    required = {'scenario.yaml', 'correctness/summary.json', 'latency/delivery.hdr.json',
                'latency/publish.hdr.json', 'metrics/generator.jsonl', 'metrics/broker-proc.jsonl', 'logs/runner.log'}
    if not required.issubset(artifacts):
        raise ValueError(f'missing sealed artifacts: {sorted(required - artifacts.keys())}')
    for name, expected in artifacts.items():
        path = (run / name).resolve()
        if not path.is_relative_to(run.resolve()):
            raise ValueError('artifact path escapes run directory')
        if sha256(path) != expected:
            raise ValueError(f'artifact checksum mismatch: {name}')
    s = yaml.safe_load((run / 'scenario.yaml').read_text())
    jsonschema.validate(s, load_json(ROOT / 'schemas/scenario.schema.json'))
    if sha256(run / 'scenario.yaml') != m['scenario']['sha256']:
        raise ValueError('scenario hash mismatch')
    reasons = []
    if m['result'].get('generator_saturated'):
        reasons.append('offering path saturated')
    if m['result'].get('correctness_invalid_reason'):
        reasons.append(m['result']['correctness_invalid_reason'])
    if not m['sut'].get('image_digest'):
        reasons.append('missing image digest')
    if m['source'].get('dirty') is not False:
        reasons.append('source checkout is dirty or its state is unknown')
    logs = jsonl(run / 'logs/runner.log')
    stages = {r['stage']: timestamp(r['time']) for r in logs}
    start = timestamp(m['workload']['measurement_started_at_utc']) if (
        'measurement_started_at_utc' in m['workload']) else stages['MEASURE']
    duration = s['timing']['measurement_seconds']
    end = start + duration
    broker_rows = [proc_row(r, s['sut']['broker'], m['sut'].get('protocol_port'))
                   for r in jsonl(run / 'metrics/broker-proc.jsonl')
                   if start <= timestamp(r['time']) <= end and 'error' not in r]
    generator_rows = [r for r in jsonl(run / 'metrics/generator.jsonl')
                      if start <= timestamp(r['time']) <= end and 'error' not in r]
    if len(broker_rows) < 2 or len(generator_rows) < 2:
        reasons.append('insufficient resource samples inside measurement')
    expected_process = m['sut'].get('process_name') or {'rabbitmq': 'beam.smp'}.get(s['sut']['broker'])
    if expected_process and any(r.get('process_name') not in (None, expected_process) for r in broker_rows):
        reasons.append('resource samples describe a launcher instead of the broker process')
    for key in ['cpu_seconds', 'rss_bytes', 'network_rx', 'network_tx', 'disk_read', 'disk_write',
                'connections']:
        if len(finite([r.get(key) for r in broker_rows])) < 2:
            reasons.append(f'insufficient broker {key} samples')
    if reasons and not include_invalid:
        raise ValueError('; '.join(reasons))
    c = load_json(run / 'correctness/summary.json')
    if c != m['result']['counters']:
        raise ValueError('manifest/correctness counters disagree')
    h = histogram(run / 'latency/delivery.hdr.json')
    if h['count'] != c['delivered']:
        raise ValueError('delivery histogram/counter mismatch')
    if histogram(run / 'latency/publish.hdr.json')['count'] != c['sent']:
        raise ValueError('publish histogram/counter mismatch')
    copies = (s['clients']['active_consumers'] or s['clients']['consumers']) if (
        s['topology']['delivery'] == 'broadcast') else 1
    if (c['unique_delivered'] > c['sent'] * copies or
            c['unique_delivered'] + c['duplicate'] != c['delivered'] or
            c['lost'] != c['sent'] * copies - c['unique_delivered']):
        raise ValueError('inconsistent delivery correctness counters')
    row = {'run_id': m['run_id'], 'broker': s['sut']['broker'], 'broker_version': m['sut'].get('version'),
           'profile': s['sut']['profile'], 'scenario': s['name'], 'experiment': s['experiment'],
           'clients': s['clients']['producers'] + s['clients']['consumers'],
           'producers': s['clients']['producers'], 'subscribers': s['clients']['consumers'],
           'active_producers': s['clients']['active_producers'] or s['clients']['producers'],
           'active_consumers': s['clients']['active_consumers'] or s['clients']['consumers'],
           'payload_bytes': s['traffic']['payload_bytes'],
           'requested_rate': m['result']['requested_messages'] / duration,
           'offered_rate': c['sent'] / duration, 'accepted_rate': c['producer_accepted_window'] / duration,
           'delivered_rate': c['delivered_window'] / duration,
           'useful_MB_s': c['unique_delivered_window'] * s['traffic']['payload_bytes'] / duration / 1e6,
           'valid': not reasons, 'invalid_reasons': '; '.join(reasons)}
    row.update({k: h[k] for k in ['p50', 'p95', 'p99', 'p99_9', 'max']})
    row.update({k: c[k] for k in ['sent', 'delivered', 'errors', 'lost', 'duplicate', 'unexpected',
                                'reordered', 'disconnects', 'connections_established', 'connections_failed']})
    row['duplicates'] = row.pop('duplicate')
    timeline = []
    for prev, current in zip(broker_rows, broker_rows[1:]):
        dt = timestamp(current['time']) - timestamp(prev['time'])
        values = finite([prev['cpu_seconds'], current['cpu_seconds']])
        # A reset is an unobserved interval across process restart, not zero CPU.
        delta = values[1] - values[0] if len(values) == 2 else -1
        current['cpu_percent'] = delta * 100 / dt if dt > 0 and delta >= 0 else None
    row['cpu_mean'], row['cpu_peak'] = mean_peak(broker_rows, 'cpu_percent')
    row['rss_mean'], row['rss_peak'] = mean_peak(broker_rows, 'rss_bytes')
    row['connections_mean'], row['connections'] = mean_peak(broker_rows, 'connections')
    for key in ['network_rx', 'network_tx', 'disk_read', 'disk_write']:
        row[key] = counter_total(broker_rows, key)
    row['rss_per_client'] = safe_ratio(row['rss_mean'], row['clients'])
    row['rss_per_connection'] = safe_ratio(row['rss_mean'], row['connections_mean'])
    row['network_bytes_per_delivery'] = safe_ratio(row['network_tx'], c['delivered_window'])
    row['disk_bytes_per_offered'] = safe_ratio(row['disk_write'], c['sent'])
    row['cpu_seconds_per_offered'] = safe_ratio(counter_total(broker_rows, 'cpu_seconds'), c['sent'])
    ticks = float(m['host'].get('clock_ticks_per_second') or 100)
    for prev, cur in zip(generator_rows, generator_rows[1:]):
        dt = timestamp(cur['time']) - timestamp(prev['time'])
        if dt <= 0:
            continue
        counters, before = cur['counters'], prev['counters']
        point = {'run_id': m['run_id'], 'seconds': timestamp(cur['time']) - start,
                 'offered_rate': (counters['sent'] - before['sent']) / dt,
                 'delivered_rate': (counters['delivered_window'] - before['delivered_window']) / dt,
                 'errors': counters['errors'] - before['errors'],
                 'generator_cpu_percent': (cur['cpu_ticks'] - prev['cpu_ticks']) / ticks / dt * 100,
                 'generator_rss_bytes': cur['rss_bytes'], 'generator_sockets': cur['sockets']}
        point['p99'] = window_percentile(cur.get('delivery_latency', {}), .99)
        if broker_rows:
            nearest = min(broker_rows, key=lambda r: abs(timestamp(r['time']) - timestamp(cur['time'])))
            for key in ['connections', 'cpu_percent', 'rss_bytes', 'network_tx']:
                point[key] = nearest.get(key)
        timeline.append(point)
    row['generator_cpu_mean'], row['generator_cpu_peak'] = mean_peak(timeline, 'generator_cpu_percent')
    row['generator_rss_mean'], row['generator_rss_peak'] = mean_peak(timeline, 'generator_rss_bytes')
    events = list(jsonl(run / 'metrics/events.jsonl'))
    row['fault_duration_seconds'] = None
    row['throughput_recovery_seconds'] = None
    if events:
        event_time = timestamp(events[0]['started_at_utc']) - start
        row['fault_duration_seconds'] = events[0]['duration_seconds']
        row['throughput_recovery_seconds'] = recovery(timeline, event_time)
    # Group only controlled configurations, software and host. Run ID and repetition are not factors.
    config = dict(s, sut=dict(s['sut'], image_digest=m['sut']['image_digest']))
    config.pop('repetitions')
    config.pop('matrix')
    host = host_controls(m['host'])
    group_key = {'scenario': config, 'image_digest': m['sut']['image_digest'],
           'config_sha256': m['sut']['configuration_sha256'], 'source': m['source'].get('commit'),
           'dirty': m['source'].get('dirty'), 'host': host, 'valid': row['valid']}
    if m['source'].get('dirty') is not False:
        group_key['unreproducible_source_run'] = m['run_id']
    row['group_id'] = hashlib.sha256(json.dumps(group_key, sort_keys=True).encode()).hexdigest()
    return row, timeline


def aggregate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[row['group_id']].append(row)
    out = []
    for key, group in sorted(groups.items()):
        first = group[0]
        item = {k: first[k] for k in ['broker', 'broker_version', 'profile', 'scenario', 'experiment',
                                     'clients', 'payload_bytes', 'requested_rate', 'group_id', 'valid']}
        item['repetitions_observed'] = len(group)
        for metric in sorted(first):
            if metric in item or metric in ['run_id', 'invalid_reasons']:
                continue
            values = finite([r.get(metric) for r in group])
            if not values:
                continue
            data = np.asarray(values)
            rng = np.random.default_rng(42)
            ci = [None, None]
            if len(values) >= 2:
                means = rng.choice(data, size=(10000, len(values)), replace=True).mean(axis=1)
                ci = np.quantile(means, [.025, .975]).tolist()
            for stat, value in {'mean': statistics.mean(values), 'median': statistics.median(values),
                                'min': min(values), 'max': max(values),
                                'stddev': statistics.stdev(values) if len(values) >= 2 else None,
                                'ci95_low': ci[0], 'ci95_high': ci[1], 'n': len(values)}.items():
                item[f'{metric}_{stat}'] = value
        out.append(item)
    return out


def write_table(path: Path, rows: list[dict[str, Any]]) -> None:
    path.with_suffix('.json').write_text(json.dumps(rows, indent=2, allow_nan=False) + '\n')
    columns = sorted({k for row in rows for k in row})
    with path.with_suffix('.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)
    if rows:
        pq.write_table(pa.Table.from_pylist(rows), path.with_suffix('.parquet'))


def analyze(runs: list[Path], output: Path, include_invalid: bool = False) -> dict[str, Any]:
    for run in runs:
        if output.resolve().is_relative_to(run.resolve()):
            raise ValueError('derived output must be outside immutable run directories')
    output.mkdir(parents=True, exist_ok=True)
    rows, timeline, rejected = [], [], []
    seen_ids: set[str] = set()
    for run in sorted(runs):
        try:
            row, points = run_summary(run, include_invalid)
            if row['run_id'] in seen_ids:
                raise ValueError(f"duplicate archived run_id: {row['run_id']}")
            seen_ids.add(row['run_id'])
            rows.append(row)
            timeline.extend(points)
        except (ValueError, OSError, KeyError, TypeError, jsonschema.ValidationError) as exc:
            rejected.append({'run': str(run), 'reason': str(exc)})
    summary = aggregate(rows)
    write_table(output / 'runs', rows)
    write_table(output / 'comparison', summary)
    write_table(output / 'timeline', timeline)
    (output / 'excluded.json').write_text(json.dumps(rejected, indent=2) + '\n')
    report = {'accepted_runs': len(rows), 'excluded_runs': len(rejected), 'groups': len(summary),
              'method': 'run-level statistics; deterministic percentile bootstrap mean CI95, seed 42, B=10000',
              'latency': 'HDR bucket upper bound in ms; per-run percentiles, not pooled percentile',
              'source_manifest_sha256': {str(p): sha256(p / 'manifest.json') for p in runs
                                         if (p / 'manifest.json').exists()}}
    (output / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    if rows:
        from .plots import plots
        plots(rows, timeline, output)
    return report
