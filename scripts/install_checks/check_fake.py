"""Imports and constructs what the `fake` extra exists to enable, installed alone."""

from faker import Faker

from desflow.values import Template

# gen: fake imports Faker when it is first used
Template({"name": {"gen": "fake", "method": "name"}}, name="t")
assert Faker("en_US").name()
