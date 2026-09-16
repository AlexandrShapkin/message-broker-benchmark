"""Verify real smoke archives and regenerate diagnostics, including in a dirty checkout."""
import argparse
import json
from pathlib import Path

from .pipeline import analyze, run_summary


def verify(run: Path) -> None:
    row, points = run_summary(run, include_invalid=True)
    # A development checkout is allowed for infrastructure verification only.
    reasons = row['invalid_reasons'].split('; ') if row['invalid_reasons'] else []
    reasons = [r for r in reasons if r != 'source checkout is dirty or its state is unknown']
    if reasons:
        raise ValueError('; '.join(reasons))
    for key in ['lost', 'duplicates', 'unexpected', 'errors', 'connections_failed']:
        if row[key]:
            raise ValueError(f'smoke {key}={row[key]}')
    if row['sent'] <= 0 or row['delivered'] <= 0 or not points:
        raise ValueError('smoke requires offered messages, deliveries and resource timeline')
    if row['connections_established'] != row['clients']:
        raise ValueError('smoke did not establish every requested logical client')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    for run in args.run:
        verify(run)
    report = analyze(args.run, args.output, include_invalid=True)
    if report['accepted_runs'] != len(args.run):
        raise ValueError('smoke analysis did not accept all verified archives')
    report['purpose'] = 'infrastructure correctness only; not scientific performance evidence'
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
