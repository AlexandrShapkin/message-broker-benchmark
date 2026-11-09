"""Render Russian baseline tables and charts from validated per-run results."""
from __future__ import annotations

import argparse
import json
import hashlib
from copy import deepcopy
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
matplotlib.rcParams['svg.hashsalt'] = 'baseline-srv0-v1'

COLORS = {'nats': '#0072B2', 'mosquitto': '#009E73', 'rabbitmq': '#D55E00', 'kafka': '#CC79A7'}
NAMES = {'nats': 'NATS', 'mosquitto': 'Mosquitto', 'rabbitmq': 'RabbitMQ', 'kafka': 'Kafka'}
BASE = 'https://github.com/AlexandrShapkin/message-broker-benchmark'
TAG = 'baseline-srv0-20261109-v1'


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    output = args.output
    audit = json.loads((output / 'audit.json').read_text())
    rows = json.loads((output / 'runs.json').read_text())
    groups = json.loads((output / 'comparison.json').read_text())
    observed = json.loads((output / 'observed-final-runs.json').read_text())
    plotted = rows + [dict(r, profile=r['profile'] + '-disk-pressure')
                      for r in observed if r.get('host_disk_pressure')]
    counts = Counter(r.get('status', 'unknown') for r in audit)
    (output / 'manifest-validation.json').write_text(json.dumps(
        [check for r in audit for check in r.get('manifest_validation', [])], ensure_ascii=False, indent=2) + '\n')
    captions = []
    units = [('p99', 'p99 задержки доставки, мс', 1),
             ('offered_rate', 'Фактически предложено, сообщений/с', 1),
             ('accepted_rate', 'Успешные publish по клиентскому API, сообщений/с', 1),
             ('delivered_rate', 'Доставлено в окне MEASURE, копий/с', 1),
             ('rss_mean', 'Средний RSS процесса брокера, МиБ', 1 / 2**20),
             ('cpu_mean', 'Средняя загрузка брокера, % одного ядра', 1),
             ('connections', 'TCP-соединения брокера (максимум выборок)', 1),
             ('generator_cpu_mean', 'Средняя загрузка генератора, % одного ядра', 1),
             ('lost', 'Недоставленные к концу cooldown копии', 1),
             ('queue_dropped', 'Задания, отброшенные до Publish, сообщений', 1)]
    for profile in sorted({r['profile'] for r in plotted}):
        selected = [r for r in plotted if r['profile'] == profile]
        # Refuse to draw one curve for workloads with different non-axis controls.
        cohort_controls = {(r['scenario'], r['requested_rate'], r['payload_bytes'], r['duration_seconds'],
                     r['active_producers'], r['active_consumers'], r['topology'], r['delivery'],
                     r['source_commit']) for r in selected}
        if len(cohort_controls) != 1:
            raise ValueError('Multiple workloads require separate figures: ' + profile)
        xs = sorted({r['clients'] for r in selected})
        profile_title = (profile.replace('-disk-pressure', '').upper() + ' · НЕХВАТКА ДИСКА, вне основного сравнения'
                         if 'disk-pressure' in profile else profile.upper())
        for metric, label, scale in units:
            if not any(r.get(metric) is not None for r in selected):
                continue
            fig, ax = plt.subplots(figsize=(9, 5.2))
            for bi, broker in enumerate(sorted({r['broker'] for r in selected})):
                broker_rows = [r for r in selected if r['broker'] == broker and r.get(metric) is not None]
                by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for r in broker_rows:
                    by_group[r['group_id']].append(r)
                legend_first = True
                for samples in by_group.values():
                    index = xs.index(samples[0]['clients'])
                    # Categorical x gives readable spacing without a misleading log axis.
                    center = index + (bi - 1) * .20
                    jitter = [(i - (len(samples) - 1) / 2) * .025 for i in range(len(samples))]
                    vals = [r[metric] * scale for r in samples]
                    ax.scatter([center + j for j in jitter], vals, s=23, alpha=.7,
                               color=COLORS[broker], label=NAMES[broker] if legend_first else None)
                    legend_first = False
                    ax.plot([center, center], [min(vals), max(vals)], color=COLORS[broker], alpha=.55)
                    ax.annotate(f'n={len(samples)}', (center, max(vals)), xytext=(0, 6),
                                textcoords='offset points', ha='center', fontsize=7, color=COLORS[broker])
            ax.set_xticks(range(len(xs)), [f'{x:,}'.replace(',', ' ') for x in xs])
            ax.set(xlabel='Логические клиенты (категории, не физические соединения)', ylabel=label,
                   title=f'Плотность клиентов · {profile_title}\n100 сообщений/с · 256 байт')
            ax.grid(axis='y', alpha=.22)
            ax.margins(y=.18)
            ax.set_ylim(bottom=0)
            ax.legend(title='Нативные гарантии различаются', fontsize=9, title_fontsize=9)
            fig.tight_layout()
            name = f'density-{profile}-{metric}'
            for extension in ['svg', 'png']:
                fig.savefig(output / f'{name}.{extension}', dpi=240,
                            metadata={'Date': None} if extension == 'svg' else {})
                if extension == 'svg':
                    svg = output / f'{name}.svg'
                    svg.write_text('\n'.join(line.rstrip() for line in svg.read_text().splitlines()) + '\n')
            plt.close(fig)
            captions.append(f'### {label} — {profile_title}\n\n'
                            f'![{label}]({name}.png)\n\n'
                            f'[SVG]({name}.svg). Точки — отдельные проверенные запуски; '
                            'вертикальный отрезок — минимум–максимум внутри строгой группы повторений. '
                            'n — число запусков, не сообщений. Категории по горизонтали расположены равномерно. '
                            + ('Это распределение **per-run p99**, а не p99 объединённого потока. '
                               if metric == 'p99' else '')
                            + 'Сравнение относится к указанному workload и нативным гарантиям профиля. '
                            'При нулевой доставке latency отсутствует; отсутствие точки не означает нулевую задержку.\n')
    auxiliary = json.loads((output / 'validated-runs.json').read_text())
    by_id = {r['run_id']: r for r in audit}
    rate_families: dict[str, list[dict[str, Any]]] = defaultdict(list)
    from broker_analysis.pipeline import host_controls
    for row in auxiliary:
        record = by_id[row['run_id']]
        if record.get('classification') != 'exploratory' or row['experiment'] != 'message_rate' or row.get('host_disk_pressure'):
            continue
        config = deepcopy(record['scenario_configuration'])
        config['traffic'].pop('total_rate_messages_per_sec')
        config.pop('matrix', None)
        config.pop('repetitions', None)
        controls = {'scenario': config, 'host': host_controls(record['environment']),
                    'source': record['source'], 'config_hash': row['configuration_sha256']}
        # PID is an instance identifier, not a source-version control.
        controls['source'] = {k: record['source'].get(k) for k in ['commit', 'dirty', 'go_version']}
        key = hashlib.sha256(json.dumps(controls, sort_keys=True).encode()).hexdigest()
        rate_families[key].append(row)
    for key, samples in rate_families.items():
        xs = sorted({r['requested_rate'] for r in samples})
        if len(xs) < 2:
            continue
        first = samples[0]
        name = f'exploratory-rate-{first["broker"]}-{first["profile"]}-{key[:8]}'
        fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
        for metric, marker, label in [('offered_rate', 'o', 'Предложено'), ('delivered_rate', 'x', 'Доставлено')]:
            axes[0].scatter([xs.index(r['requested_rate']) for r in samples], [r[metric] for r in samples],
                            marker=marker, color=COLORS[first['broker']], label=label)
        valid = [r for r in samples if r.get('p99') is not None]
        axes[1].scatter([xs.index(r['requested_rate']) for r in valid], [r['p99'] for r in valid], color=COLORS[first['broker']])
        for ax, ylabel in zip(axes, ['Сообщений или доставленных копий/с', 'p99 доставки по запускам, мс']):
            ax.set_xticks(range(len(xs)), [f'{x:g}' for x in xs])
            ax.set(xlabel='Запрошено, сообщений/с (категории)', ylabel=ylabel)
            ax.grid(alpha=.2)
        axes[0].legend()
        fig.suptitle(f'Разведочная серия · {NAMES[first["broker"]]} · {first["profile"].upper()} · '
                     f'{first["clients"]} клиентов · измерение {first["duration_seconds"]} с')
        fig.tight_layout()
        for extension in ['svg', 'png']:
            fig.savefig(output / f'{name}.{extension}', dpi=240,
                            metadata={'Date': None} if extension == 'svg' else {})
            if extension == 'svg':
                svg = output / f'{name}.svg'
                svg.write_text('\n'.join(line.rstrip() for line in svg.read_text().splitlines()) + '\n')
        plt.close(fig)
        captions.append(f'### Разведочная интенсивность — {NAMES[first["broker"]]}\n\n'
                        f'![Разведочная интенсивность]({name}.png)\n\n[SVG]({name}.svg). '
                        f'Всего n={len(samples)} отдельных проверенных запусков с одинаковыми параметрами вне запрошенного rate. '
                        'Это короткая вспомогательная серия, не финальная baseline и не доказательство capacity. '
                        'Показаны только прошедшие строгую проверку запуски; другие попытки и причины исключения есть в реестре.\n')
    (output / 'charts.md').write_text('# Графики baseline srv0\n\n' + '\n'.join(captions))
    def span(g, key, scale=1):
        lo, hi = g.get(key + '_min'), g.get(key + '_max')
        if lo is None:
            return 'нет данных'
        return f'{lo * scale:.3g}' if lo == hi else f'{lo * scale:.3g}–{hi * scale:.3g}'
    ordered = sorted(groups, key=lambda r: (r['profile'], r['broker'], r['clients'], r['group_id']))
    sections = []
    specifications: list[tuple[str, list[tuple[str, str, float]]]] = [
        ('Прикладной поток и доставка', [('offered_rate', 'Offered, сообщ./с', 1),
         ('delivered_rate', 'Delivered, копий/с', 1), ('lost', 'Потери, копий', 1)]),
        ('Задержка доставки', [('p50', 'p50, мс', 1), ('p95', 'p95, мс', 1),
         ('p99', 'p99, мс', 1), ('p99_9', 'p99.9, мс', 1), ('max', 'Максимум, мс', 1)]),
        ('Ресурсы', [('rss_mean', 'RSS, МиБ', 1 / 2**20), ('cpu_mean', 'CPU брокера, % ядра', 1),
         ('generator_cpu_mean', 'CPU генератора, % ядра', 1), ('generator_rss_mean', 'RSS генератора, МиБ', 1 / 2**20), ('connections', 'TCP endpoints, пик', 1)])]
    for title, metrics in specifications:
        header = ['Брокер / профиль', 'Клиенты', 'n'] + [label for _, label, _ in metrics]
        lines = ['| ' + ' | '.join(header) + ' |', '|' + '---|' * len(header)]
        for g in ordered:
            values = [f"{NAMES[g['broker']]} / {g['profile']}", str(g['clients']), str(g['repetitions_observed'])]
            values += [span(g, key, scale) for key, _, scale in metrics]
            lines.append('| ' + ' | '.join(values) + ' |')
        sections.append('## ' + title + '\n\n' + '\n'.join(lines))
    (output / 'tables.md').write_text('# Основная сравнительная серия\n\nДиапазоны — минимум–максимум по запускам; '
        'каждая строка — строгая группа повторений. Процентили не объединены между запусками. '
        'Полные group_id и controls сохранены в comparison.csv и audit архива таблиц.\n\n' + '\n\n'.join(sections) + '\n')
    compromised = [r for r in observed if r.get('host_disk_pressure')]
    lines = ['| Run ID | Клиенты | Offered, сообщ./с | Delivered, копий/с | Потери | Ошибки операций | Queue drops |',
             '|---|---:|---:|---:|---:|---:|---:|']
    for r in compromised:
        lines.append(f"| {r['run_id']} | {r['clients']} | {r['offered_rate']:.3f} | {r['delivered_rate']:.3f} | "
                     f"{r['lost']} | {r['errors']} | {r['queue_dropped']} |")
    (output / 'observations.md').write_text('# Наблюдения при disk alarm\n\nЭти completed RabbitMQ FAST runs исключены '
        'из основной агрегации: broker log сообщает о disk alarm и блокировке publishers. '
        'Это данные о поведении при заполнении диска, а не оценка предела брокера.\n\n' + '\n'.join(lines) + '\n')
    flat_audit = [{k: r.get(k) for k in ['run_id', 'status', 'scenario', 'broker', 'profile',
                                       'included', 'validated', 'classification', 'host_disk_pressure', 'reason']} for r in audit]
    from broker_analysis.pipeline import write_table
    write_table(output / 'run-register', flat_audit)
    summary = {'discovered': len(audit), 'included': len(rows), 'excluded': len(audit) - len(rows),
               'status_counts': dict(counts), 'strict_groups': len(groups),
               'observed_final': len(observed), 'host_confounded_completed': sum(r.get('host_disk_pressure', False) for r in observed),
               'validated_auxiliary': sum(bool(r.get('validated')) and r.get('classification') in {'smoke', 'exploratory'} for r in audit),
               'classification_counts': dict(Counter(r.get('classification', 'unknown') for r in audit))}
    (output / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
    failures = [{'run_id': r['run_id'], 'scenario': r.get('scenario'), 'broker': r.get('broker'),
                 'stage': r.get('failure_stage'), 'reason': r.get('failure_reason')}
                for r in audit if r.get('status') == 'failed']
    (output / 'failures.json').write_text(json.dumps(failures, ensure_ascii=False, indent=2) + '\n')
    (output / 'host-confounded.json').write_text(json.dumps([{'run_id': r['run_id'],
        'status': r.get('status'), 'scenario': r.get('scenario'), 'reason': r['reason']}
        for r in audit if r.get('host_disk_pressure')], ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == '__main__':
    main()
