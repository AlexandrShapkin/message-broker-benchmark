import importlib.util
import io
import json
from pathlib import Path
from urllib.error import HTTPError

import pytest
import yaml


@pytest.fixture
def locker():
    path = Path(__file__).resolve().parents[2] / 'scripts/lock_images.py'
    spec = importlib.util.spec_from_file_location('image_locker', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_digest_resolution(locker, monkeypatch):
    class Response(io.BytesIO):
        headers = {'Docker-Content-Digest': 'sha256:' + 'a' * 64}
    requests = []
    def open_request(request, timeout):
        requests.append(request)
        assert timeout == 30
        return Response(json.dumps({'token': 'public-test-fixture'}).encode())
    monkeypatch.setattr(locker, 'urlopen', open_request)
    assert locker.resolve('nats:2.11.8-alpine') == 'sha256:' + 'a' * 64
    assert requests[-1].method == 'HEAD'
    assert requests[-1].full_url.endswith('/library/nats/manifests/2.11.8-alpine')


def test_unpinned_image_rejected(locker):
    with pytest.raises(ValueError, match='pinned'):
        locker.resolve('nats:latest')


def test_local_digest_uses_matching_repository(locker, monkeypatch):
    from types import SimpleNamespace
    refs = ['unrelated@sha256:' + 'b' * 64, 'nats@sha256:' + 'a' * 64]
    monkeypatch.setattr(locker.subprocess, 'run', lambda *a, **kw: SimpleNamespace(stdout=json.dumps(refs)))
    assert locker.resolve_local('nats:2.11.8-alpine') == 'sha256:' + 'a' * 64
    refs.pop()
    with pytest.raises(ValueError, match='matching'):
        locker.resolve_local('nats:2.11.8-alpine')
    with pytest.raises(ValueError, match='pinned'):
        locker.resolve_local('nats:latest')


def test_partial_registry_failure_preserves_all_configs(locker, monkeypatch, tmp_path):
    lock = {'brokers': {b: {'image': b + ':1.0', 'version': '1.0', 'digest': None}
                        for b in ['nats', 'mosquitto']}}
    path = tmp_path / 'versions.lock.yaml'
    path.write_text(yaml.safe_dump(lock, sort_keys=False))
    for broker in lock['brokers']:
        compose = tmp_path / 'deploy' / broker / 'compose.yaml'
        compose.parent.mkdir(parents=True)
        compose.write_text(f'services:\n  broker:\n    image: {broker}:1.0\n')
    before = {p: p.read_bytes() for p in tmp_path.rglob('*.yaml')}
    def resolve(image):
        if image.startswith('mosquitto'):
            raise HTTPError('https://registry.invalid', 403, 'Forbidden', {}, None)
        return 'sha256:' + 'a' * 64
    monkeypatch.setattr(locker, 'resolve', resolve)
    monkeypatch.setattr(locker, '__file__', str(tmp_path / 'scripts/lock_images.py'))
    monkeypatch.setattr('sys.argv', ['lock_images.py'])
    with pytest.raises(SystemExit, match='403'):
        locker.main()
    assert before == {p: p.read_bytes() for p in before}
