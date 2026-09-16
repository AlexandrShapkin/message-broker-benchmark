from __future__ import annotations
from collections import defaultdict
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def plots(rows: list[dict[str, Any]], timeline: list[dict[str, Any]], output: Path) -> None:
    axes = {
        'connection_density': ('clients', ['rss_mean', 'rss_per_client', 'cpu_mean', 'p99', 'connections_failed']),
        'mostly_idle': ('clients', ['rss_mean', 'rss_per_client', 'cpu_mean', 'connections_failed']),
        'message_rate': ('offered_rate', ['delivered_rate', 'p50', 'p95', 'p99', 'cpu_mean', 'rss_mean', 'errors']),
        'payload_size': ('payload_bytes', ['delivered_rate', 'p99', 'cpu_mean']),
        'fan_out': ('subscribers', ['cpu_mean', 'network_tx', 'p99']),
        'many_to_one': ('offered_rate', ['delivered_rate', 'p99']),
        'many_to_many': ('offered_rate', ['delivered_rate', 'p99']),
        'slow_consumer': ('offered_rate', ['delivered_rate', 'lost', 'p99']),
    }
    units = {'clients': 'Logical clients', 'rss_mean': 'Broker RSS (bytes)',
             'rss_per_client': 'Broker RSS / logical client (bytes)', 'cpu_mean': 'Broker CPU (% of one core)',
             'offered_rate': 'Actual offered messages/s', 'delivered_rate': 'Delivered copies/s',
             'payload_bytes': 'Message envelope + payload (bytes)', 'network_tx': 'Broker network TX (bytes)',
             'subscribers': 'Independent subscribers', 'p50': 'Delivery p50 (ms)',
             'p95': 'Delivery p95 (ms)', 'p99': 'Delivery p99 (ms)'}
    families: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        families[row['experiment']].append(row)
    for family, group in families.items():
        if family not in axes:
            continue
        x, ys = axes[family]
        for y in ys:
            fig, ax = plt.subplots(figsize=(7, 4.5))
            groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in group:
                if row.get(x) is not None and row.get(y) is not None:
                    label = f"{row['broker']} {row['broker_version']} / {row['profile']}"
                    if not row['valid']:
                        label += ' / INVALID'
                    groups[label].append(row)
            for label, samples in sorted(groups.items()):
                # Individual repetitions remain visible. No line implies equivalence of other controls.
                ax.scatter([r[x] for r in samples], [r[y] for r in samples], label=label)
            ax.set(xlabel=units.get(x, x), ylabel=units.get(y, y), title=family.replace('_', ' '))
            ax.grid(alpha=.25)
            if groups:
                ax.legend(fontsize=7)
            fig.tight_layout()
            for extension in ('png', 'svg'):
                fig.savefig(output / f'{family}-{y}.{extension}', dpi=150)
            plt.close(fig)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for point in timeline:
        grouped[point['run_id']].append(point)
    for run_id, points in grouped.items():
        for y in ['connections', 'cpu_percent', 'delivered_rate', 'p99', 'errors', 'generator_cpu_percent']:
            valid = [p for p in points if p.get(y) is not None]
            if not valid:
                continue
            fig, ax = plt.subplots(figsize=(7, 4))
            ax.plot([p['seconds'] for p in valid], [p[y] for p in valid])
            ax.set(xlabel='Seconds since MEASURE', ylabel=y, title=run_id)
            ax.grid(alpha=.25)
            fig.tight_layout()
            for extension in ('png', 'svg'):
                fig.savefig(output / f'{run_id}-{y}.{extension}', dpi=120)
            plt.close(fig)
