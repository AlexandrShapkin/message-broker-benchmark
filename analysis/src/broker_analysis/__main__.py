import argparse
import json
from pathlib import Path
from .pipeline import analyze


def main() -> None:
    parser = argparse.ArgumentParser(description='Derive tables and PNG/SVG from sealed raw broker runs')
    parser.add_argument('--results', type=Path, default=Path('results'))
    parser.add_argument('--run', type=Path)
    parser.add_argument('--output', type=Path, default=Path('analysis/output'))
    parser.add_argument('--include-invalid', action='store_true', help='include flagged completed measurements')
    args = parser.parse_args()
    runs = [args.run] if args.run else [p.parent for p in args.results.rglob('manifest.json')]
    report = analyze(runs, args.output, args.include_invalid)
    print(json.dumps(report, indent=2))
    if not report['accepted_runs']:
        parser.exit(1, 'No valid broker runs; see excluded.json. No performance plots generated.\n')


if __name__ == '__main__':
    main()
