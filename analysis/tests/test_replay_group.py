import json

import yaml

from broker_analysis.pipeline import analyze, run_summary, sha256
from test_pipeline import fixture_run


def test_explicit_replay_digest_groups_with_same_resolved_image(tmp_path):
    a = fixture_run(tmp_path / 'a', 'a')
    b = fixture_run(tmp_path / 'b', 'b')
    scenario_path = b / 'scenario.yaml'
    scenario = yaml.safe_load(scenario_path.read_text())
    scenario['sut']['image_digest'] = 'sha256:' + 'a' * 64
    scenario_path.write_text(yaml.safe_dump(scenario))
    manifest_path = b / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    manifest['scenario']['sha256'] = sha256(scenario_path)
    manifest['scenario']['configuration'] = scenario
    manifest['artifacts']['scenario.yaml'] = sha256(scenario_path)
    manifest_path.write_text(json.dumps(manifest))
    assert run_summary(a)[0]['group_id'] == run_summary(b)[0]['group_id']
    manifest['sut']['image_digest'] = 'nats@sha256:' + 'b' * 64
    manifest_path.write_text(json.dumps(manifest))
    assert run_summary(a)[0]['group_id'] != run_summary(b)[0]['group_id']


def test_duplicate_archive_is_not_an_extra_repetition(tmp_path):
    import shutil
    run = fixture_run(tmp_path / 'original')
    copy = tmp_path / 'copied-archive'
    shutil.copytree(run, copy)
    report = analyze([run, copy], tmp_path / 'output')
    assert report['accepted_runs'] == 1
    assert report['excluded_runs'] == 1
    reasons = json.loads((tmp_path / 'output/excluded.json').read_text())
    assert 'duplicate archived run_id' in reasons[0]['reason']
