"""The recommender of the Call Python from YAML page ranks with models in a real Valkey.

It needs the odctl `valkey` profile (`odctl up valkey`). tests/docs runs the same
blueprint with a stand-in client.
"""

import json
import shutil
import sys
from pathlib import Path

import pytest
import redis

from desflow import SimulationContext

pytestmark = [pytest.mark.integration, pytest.mark.services]

ROOT = Path(__file__).resolve().parents[2]


def test_the_recommender_ranks_with_the_models_in_valkey(
    redis_container, tmp_path, monkeypatch, capsys
):
    """Three products have a stored model and two have none; every visit shows the
    three products the separate computation of the score ranks first."""
    for source in (
        ROOT / "docs/snippets/guides/call_python_ranking.py",
        ROOT / "docs/snippets/guides/call_python_seed.py",
        ROOT / "docs/snippets/yaml/call_python.yaml",
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("VALKEY_URL", redis_container)
    for name in ("call_python_ranking", "call_python_seed"):
        monkeypatch.delitem(sys.modules, name, raising=False)
    import call_python_ranking
    import call_python_seed

    client = redis.Redis.from_url(redis_container, decode_responses=True)
    keys = [f"linucb:{pid}" for pid in call_python_ranking.PRODUCTS]
    client.delete(*keys)
    try:
        call_python_seed.seed(client)
        SimulationContext.from_yaml(tmp_path / "call_python.yaml").run()
        records = [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if line.startswith("{")
        ]
    finally:
        client.delete(*keys)
    assert len(records) == 10
    for record in records:
        x = record["value"]["features"]
        scores = {}
        for pid in call_python_ranking.PRODUCTS:
            model = call_python_seed.MODELS.get(pid)
            a_inv = model["A_inv"] if model else [[1, 0, 0], [0, 1, 0], [0, 0, 1]]
            b = model["b"] if model else [0, 0, 0]
            ax = [sum(a * v for a, v in zip(row, x)) for row in a_inv]
            scores[pid] = (
                sum(a * c for a, c in zip(ax, b))
                + sum(a * v for a, v in zip(ax, x)) ** 0.5
            )
        assert (
            record["value"]["shown"] == sorted(scores, key=scores.get, reverse=True)[:3]
        )
