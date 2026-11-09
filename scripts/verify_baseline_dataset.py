"""Verify a downloaded dataset from stdin without expanding it on disk."""
import argparse
import hashlib
import json
import sys
import tarfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--run-directories', type=Path, required=True)
    args = parser.parse_args()
    inventory_bytes = args.inventory.read_bytes()
    expected = {r['path']: r for line in inventory_bytes.splitlines() if (r := json.loads(line))}
    seen = {}
    runs = set()
    embedded = False
    with tarfile.open(fileobj=sys.stdin.buffer, mode='r|gz') as archive:
        for member in archive:
            if not member.isfile():
                continue
            stream = archive.extractfile(member)
            assert stream is not None
            h = hashlib.sha256()
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                h.update(block)
            if member.name == 'INVENTORY.jsonl':
                if h.hexdigest() != hashlib.sha256(inventory_bytes).hexdigest():
                    raise ValueError('Embedded inventory mismatch')
                embedded = True
                continue
            if member.name in seen:
                raise ValueError('Duplicate archive path: ' + member.name)
            seen[member.name] = {'path': member.name, 'size': member.size, 'sha256': h.hexdigest()}
            parts = Path(member.name).parts
            if len(parts) > 3 and parts[:2] == ('message-broker-benchmark', 'results'):
                runs.add(parts[2])
    if not embedded or seen != expected:
        raise ValueError('Incomplete or corrupt downloaded archive')
    directories = set(args.run_directories.read_text().splitlines())
    if runs - directories:
        raise ValueError('Unregistered run directories')
    print(json.dumps({'verified': True, 'files': len(seen), 'bytes': sum(r['size'] for r in seen.values()),
                      'run_directories': len(directories), 'runs_with_files': len(runs),
                      'empty_run_directories': sorted(directories - runs)}, indent=2))


if __name__ == '__main__':
    main()
