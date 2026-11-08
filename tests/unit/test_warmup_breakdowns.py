"""A warm-up resets a resource's `downtime` and `failures` with every other statistic."""

import re
from pathlib import Path

from desflow.analysis.models import run_once

ROOT = Path(__file__).resolve().parents[2]


def test_downtime_and_failures_count_from_the_end_of_the_warm_up(tmp_path):
    text = (ROOT / "examples" / "yaml" / "breakdowns.yaml").read_text()
    model = tmp_path / "m.yaml"
    model.write_text(re.sub(r"egress:.*?(?=\n[a-z_]+:|\Z)", "", text, flags=re.DOTALL))
    whole = run_once(str(model), seed=3, until=300, warmup=0).readings
    warm = run_once(str(model), seed=3, until=300, warmup=250).readings
    key = "Shop.stats.resources.station."
    assert whole[key + "failures"] > warm[key + "failures"]
    assert whole[key + "downtime"] > warm[key + "downtime"]
    assert warm[key + "downtime"] <= 50
