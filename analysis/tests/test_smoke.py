import json

import pytest

from broker_analysis.smoke import verify
from test_pipeline import fixture_run


def test_smoke_allows_dirty_source_but_rejects_saturation(tmp_path):
    run = fixture_run(tmp_path / 'run')
    path = run / 'manifest.json'
    m = json.loads(path.read_text())
    m['source']['dirty'] = True
    path.write_text(json.dumps(m))
    verify(run)
    m['result']['generator_saturated'] = True
    path.write_text(json.dumps(m))
    with pytest.raises(ValueError, match='saturated'):
        verify(run)


def test_smoke_requires_resource_evidence(tmp_path):
    from broker_analysis.pipeline import sha256
    run = fixture_run(tmp_path / 'run')
    (run / 'metrics/broker-proc.jsonl').write_text('')
    path = run / 'manifest.json'
    m = json.loads(path.read_text())
    m['artifacts']['metrics/broker-proc.jsonl'] = sha256(run / 'metrics/broker-proc.jsonl')
    path.write_text(json.dumps(m))
    with pytest.raises(ValueError, match='resource samples'):
        verify(run)
