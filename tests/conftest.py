from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SAMPLE = ROOT / "examples" / "sample-app" / "template.yaml"


@pytest.fixture
def sample_specs():
    from lambda2eks.loaders.template import load_template
    return {s.name: s for s in load_template(SAMPLE)}
