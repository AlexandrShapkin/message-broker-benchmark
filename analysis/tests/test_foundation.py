from pathlib import Path
import json
import jsonschema
import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_examples():
    schema = json.loads((ROOT / 'schemas/scenario.schema.json').read_text())
    for path in (ROOT / 'scenarios').rglob('*.yaml'):
        jsonschema.validate(yaml.safe_load(path.read_text()), schema)
