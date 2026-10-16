# Changelog

Each release, newest first: what changed, then under Upgrading what to change in code, commands or installs that use the previous release.

## Version 0.20.0

The package dynamic-des is renamed desflow. The classes, the YAML format, the records and the extras are the same as in 0.19.0.

### Upgrading

- Install `desflow` instead of `dynamic-des`, with the same extras: `pip install desflow`, `uv add desflow`, or for example `pip install "desflow[kafka]"`. The last dynamic-des release, 0.20.0, installs desflow with the same extras. To install desflow beside dynamic-des 0.19.0 instead, uninstall dynamic-des first: with pip, uninstalling it afterwards also removes `dynamic_des` and `ddes`, and `pip install --force-reinstall --no-deps desflow` puts them back.
- Import `desflow` instead of `dynamic_des`, for example `from desflow import SimulationContext`.
- Run the `desflow` command instead of `ddes`, for example `desflow run shop.yaml` or `desflow examples`.
- `import dynamic_des` still works in this release. It shows a `FutureWarning` and gives the desflow modules, submodules such as `dynamic_des.connectors.egress.kafka` included. The `ddes` command still works too: it prints a warning and runs `desflow`. Both are removed in a later release, which this page will name.
- The docs are at [jaehyeon.me/desflow](https://jaehyeon.me/desflow/), and the repository is [jaehyeon-kim/desflow](https://github.com/jaehyeon-kim/desflow) on GitHub.
