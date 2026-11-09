"""Create a publication archive without local dependencies or Git metadata.

Checks every published file against the server inventory. Raw run files are copied
byte-for-byte, including Zstandard files. No measurements are redacted.
"""
import argparse
import hashlib
import io
import json
import re
import tarfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('inventory', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    expected = {r['path']: r for line in args.inventory.read_text().splitlines() if (r := json.loads(line))}
    pattern = re.compile(rb'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|gh[pousr]_[A-Za-z0-9]{30,}|AKIA[A-Z0-9]{16}|sk-proj-[A-Za-z0-9_-]{30,}')
    excluded = []
    inventory = []
    with tarfile.open(args.source, 'r|gz') as source, tarfile.open(args.output, 'w|gz', compresslevel=1) as dest:
        for member in source:
            parts = Path(member.name).parts
            omitted = any(p in {'.git', '.venv', '.cache', '__pycache__', '.pytest_cache',
                               '.ruff_cache', '.mypy_cache', 'bin'} for p in parts)
            if omitted or not member.isfile():
                if member.isfile():
                    excluded.append({'path': member.name, 'reason': 'Generated dependencies, binary, cache or Git metadata'})
                continue
            stream = source.extractfile(member)
            assert stream is not None
            # Raw files can exceed RAM. Hash while tar streams them unchanged.
            class Reader:
                def __init__(self):
                    self.h = hashlib.sha256()
                    self.tail = b''

                def read(self, size=-1):
                    block = stream.read(size)
                    self.h.update(block)
                    scanned = self.tail + block
                    if any(marker in scanned for marker in [b'-----BEGIN ', b'gh', b'AKIA', b'sk-proj-']) and pattern.search(scanned):
                        raise ValueError('Secret pattern requires review: ' + member.name)
                    self.tail = block[-256:]
                    return block
            reader = Reader()
            dest.addfile(member, reader)
            row = {'path': member.name, 'size': member.size, 'sha256': reader.h.hexdigest()}
            if row != expected.get(member.name):
                raise ValueError('Source inventory mismatch: ' + member.name)
            inventory.append(row)
        content = ''.join(json.dumps(r) + '\n' for r in inventory).encode()
        info = tarfile.TarInfo('INVENTORY.jsonl')
        info.size = len(content)
        dest.addfile(info, io.BytesIO(content))
    args.output.with_suffix('.inventory.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in inventory))
    args.output.with_suffix('.omissions.json').write_text(json.dumps(excluded, indent=2) + '\n')


if __name__ == '__main__':
    main()
