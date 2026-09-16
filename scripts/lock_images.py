"""Resolve configured public Docker Hub tags before modifying any lock files."""
import argparse
import json
from pathlib import Path
import re
import subprocess
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import yaml


def resolve_local(image: str) -> str:
    """Use the pulled image's repository digest, never its mutable tag or image ID."""
    image = image.split('@', 1)[0]
    repo, separator, tag = image.rpartition(':')
    if not separator or tag == 'latest':
        raise ValueError('a pinned Docker Hub image tag is required')
    def canonical(name: str) -> str:
        name = name.removeprefix('docker.io/')
        return 'library/' + name if '/' not in name else name
    result = subprocess.run(
        ['docker', 'image', 'inspect', '--format', '{{json .RepoDigests}}', image],
        check=True, capture_output=True, text=True, timeout=30,
    )
    matches = {digest for ref in json.loads(result.stdout) or []
               for name, sep, digest in [ref.partition('@')]
               if sep and canonical(name) == canonical(repo)
               and re.fullmatch(r'sha256:[0-9a-f]{64}', digest)}
    if len(matches) != 1:
        raise ValueError(f'{image}: require exactly one matching pulled repository digest')
    return matches.pop()


def resolve(image: str) -> str:
    image = image.split('@', 1)[0]
    repo, separator, tag = image.rpartition(':')
    if not separator or tag == 'latest':
        raise ValueError('a pinned Docker Hub image tag is required')
    if '/' not in repo:
        repo = 'library/' + repo
    with urlopen(f'https://auth.docker.io/token?service=registry.docker.io&scope=repository:{repo}:pull',
                 timeout=30) as response:
        token = json.load(response)['token']
    request = Request(f'https://registry-1.docker.io/v2/{repo}/manifests/{tag}', method='HEAD', headers={
        'Authorization': 'Bearer ' + token,
        'Accept': 'application/vnd.oci.image.index.v1+json, '
                  'application/vnd.docker.distribution.manifest.list.v2+json, '
                  'application/vnd.oci.image.manifest.v1+json, '
                  'application/vnd.docker.distribution.manifest.v2+json',
    })
    with urlopen(request, timeout=30) as response:
        digest = response.headers['Docker-Content-Digest']
    if not digest or not re.fullmatch(r'sha256:[0-9a-f]{64}', digest):
        raise ValueError('registry returned an invalid digest')
    return digest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true', help='resolve and print without writing')
    parser.add_argument('--local-images', action='store_true',
                        help='resolve RepoDigests from images already pulled by Docker')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    path = root / 'versions.lock.yaml'
    lock = yaml.safe_load(path.read_text())
    updates = {}
    # Resolve every broker before touching configurations, including on HTTP errors.
    for broker, settings in lock['brokers'].items():
        try:
            digest = (resolve_local if args.local_images else resolve)(settings['image'])
        except HTTPError as exc:
            raise SystemExit(f'{broker}: registry HTTP {exc.code}; lock files unchanged') from None
        settings['digest'] = digest
        compose = root / 'deploy' / broker / 'compose.yaml'
        replacement = '    image: ${BENCH_IMAGE:-' + settings['image'] + '@' + digest + '}'
        lines = [replacement if line.strip().startswith('image:') else line
                 for line in compose.read_text().splitlines()]
        updates[compose] = '\n'.join(lines) + '\n'
    encoded = yaml.safe_dump(lock, sort_keys=False)
    if args.dry_run:
        print(encoded, end='')
        return
    for compose, contents in updates.items():
        compose.write_text(contents)
    path.write_text(encoded)
    for broker, settings in lock['brokers'].items():
        print(broker, settings['digest'])


if __name__ == '__main__':
    main()
