"""Normalize baseline derived tables and apply the documented host-pressure exclusion.

Uses preserved, checksum-verified metadata and broker logs. This is also useful
for reclassifying a derived report without rereading the large resource streams.
"""
import argparse
import json
from pathlib import Path

from broker_analysis.pipeline import aggregate, sha256, write_table
from baseline_archive import validate_manifests


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    parser.add_argument('metadata', type=Path)
    parser.add_argument('inventory', type=Path)
    args = parser.parse_args()
    inventory = {r['path']: r for line in args.inventory.read_text().splitlines() if (r := json.loads(line))}
    audit = json.loads((args.output / 'audit.json').read_text())
    rows = json.loads((args.output / 'validated-runs.json').read_text())
    for record in audit:
        run = args.metadata / 'results' / record['run_id']
        record['manifest_validation'] = validate_manifests(run)
        for check in record['manifest_validation']:
            key = 'message-broker-benchmark/results/' + record['run_id'] + '/' + check['file']
            assert check['sha256'] == inventory[key]['sha256']
        manifest = run / 'manifest.json'
        if manifest.exists():
            name = 'message-broker-benchmark/' + str(manifest.relative_to(args.metadata))
            assert sha256(manifest) == inventory[name]['sha256']
            try:
                m = json.loads(manifest.read_text())
                record.update(status=m['status'], scenario=m['scenario']['name'],
                              broker=m['sut'].get('broker'), profile=m['sut'].get('profile'),
                              environment=m['host'], source=m['source'], sut=m['sut'],
                              workload=m.get('workload'), result=m.get('result'),
                              scenario_configuration=m['scenario'].get('configuration'),
                              failure_stage=m.get('failure_stage'), failure_reason=m.get('failure_reason'))
                record['classification'] = ('synthetic' if m.get('result', {}).get('synthetic') else
                                            'scientific' if record['scenario'].startswith('nir1_final_') else
                                            'smoke' if record['scenario'] == 'smoke' else 'exploratory')
                if record['classification'] == 'synthetic':
                    record['artifact_errors'] = [name + ': missing or checksum mismatch'
                        for name, digest in m.get('artifacts', {}).items()
                        if inventory.get('message-broker-benchmark/results/' + record['run_id'] + '/' + name, {}).get('sha256') != digest]
                    record['reason'] = 'Generator self-test is synthetic, not a broker measurement'
            except ValueError:
                pass
        if record.get('status') == 'incomplete':
            record['classification'] = 'incomplete'
            record['reason'] = ('Unreadable final manifest; incomplete attempt' if manifest.exists() else
                                'No final manifest; incomplete attempt' if any(run.rglob('*')) else
                                'Empty run directory; no manifest or measurements')
        log = run / 'logs/broker.log'
        pressure = False
        if log.exists():
            name = 'message-broker-benchmark/' + str(log.relative_to(args.metadata))
            assert sha256(log) == inventory[name]['sha256']
            pressure = 'disk resource limit alarm set' in log.read_text(errors='replace')
        record['host_disk_pressure'] = pressure
        if pressure:
            record.update(included=False, reason='Disk resource limit alarm; host disk pressure confounds broker capacity')
    by_id = {r['run_id']: r for r in audit}
    for row in rows:
        record = by_id[row['run_id']]
        row['host_disk_pressure'] = record['host_disk_pressure']
        host, sut, config = record['environment'], record['sut'], record['scenario_configuration']
        row.update(hostname=host.get('hostname'), kernel=host.get('kernel'),
                   cpu_governor=host.get('cpu_governor'), logical_cores=host.get('logical_cores'),
                   client_library=sut.get('client_library'), client_library_version=sut.get('client_library_version'),
                   warmup_seconds=config['timing']['warmup_seconds'], cooldown_seconds=config['timing']['cooldown_seconds'],
                   random_seed=config['random_seed'], mode=config['sut']['mode'],
                   ramp_clients_per_second=config['connection']['ramp_clients_per_second'])
        row.update({k: sut.get(k) for k in ['publisher_ack', 'consumer_ack', 'persistence', 'replicas', 'protocol']})
        row['sut_mode'] = sut.get('mode')
        for k in ['scheduled', 'queue_dropped', 'producer_accepted', 'consumer_acked',
                  'unique_delivered', 'unique_delivered_window']:
            row[k] = record['result']['counters'].get(k)
        key = 'message-broker-benchmark/results/' + row['run_id'] + '/manifest.json'
        row['manifest_sha256'] = inventory[key]['sha256']
    audit.sort(key=lambda r: r['run_id'])
    rows.sort(key=lambda r: r['run_id'])
    (args.output / 'audit.json').write_text(json.dumps(audit, ensure_ascii=False, indent=2) + '\n')
    final = [r for r in rows if by_id[r['run_id']].get('classification') == 'scientific']
    included = [r for r in rows if by_id[r['run_id']]['included']]
    write_table(args.output / 'validated-runs', rows)
    write_table(args.output / 'observed-final-runs', final)
    write_table(args.output / 'runs', included)
    write_table(args.output / 'comparison', [
        {k: v for k, v in group.items() if '_ci95_' not in k} for group in aggregate(included)])
    # Keep all final observations' timeline so host-pressure effects remain inspectable.
    # Primary rows are selected by joining run_id against runs.csv.


if __name__ == '__main__':
    main()
