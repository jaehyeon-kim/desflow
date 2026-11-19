# Kestra

Run a desflow study as a Kestra flow, and keep its result as an output file. The flow has two tasks: a Python script task that installs desflow in a container and runs the shop, and a task that logs the result. Kestra runs the flow on its own server, so this page starts one in Docker.

---

## Before You Start

- [Integrations](index.md#one-shop-for-every-orchestrator) shows the shop, the command and the figures the result file holds.
- You need Docker. Kestra installs desflow inside the task's container, so your machine needs no Python package.
- Put `kestra_flow.yaml` in a folder and run the commands below from it.

---

## Steps

### Write the Flow

The `run_shop` task is an `io.kestra.plugin.scripts.python.Commands` task. Kestra runs it in a new `python:3.13-slim` container, with the folder Kestra prepares for the task as the working folder:

- `beforeCommands` installs desflow with pip, which puts the `desflow` command on the container's `PATH`. The task's `dependencies` list is not used, because it installs packages for Python code and does not put their commands on the `PATH`.
- `inputFiles` writes `shop.yaml` into the working folder. It holds the blueprint of the overview page, so the flow needs no other file.
- `commands` runs the command, and a non-zero exit fails the task.
- `outputFiles` keeps `result.json` in Kestra's storage after the container is removed.

The `read_result` task reads that file with `read()` and logs it:

```yaml title="docs/snippets/orchestrators/kestra_flow.yaml" hl_lines="13-16 33-38"
# A Kestra flow that runs the shop with desflow and logs the result. The first task
# installs desflow in a python:3.13-slim container, writes shop.yaml into its working
# folder and keeps result.json as an output file. The second task reads that file.
# pip install puts the desflow command on the container's PATH; the task's
# `dependencies` list installs packages for Python code, without their commands.
id: shop_study
namespace: desflow.examples

tasks:
  - id: run_shop
    type: io.kestra.plugin.scripts.python.Commands
    containerImage: python:3.13-slim
    beforeCommands:
      - pip install --quiet "desflow>=1.1"
    inputFiles:
      shop.yaml: |
        simulation: {sim_id: Shop, factor: 0, random_seed: 42}

        resources:
          packers: {current_cap: "${PACKERS:-2}", max_cap: 5}

        services:
          packing: {dist: normal, mean: 3 s, std: 0.5 s}

        arrivals:
          orders: {dist: exponential, rate: 0.5, spawn: pack}

        tasks:
          pack:
            service: packing
            resource: packers
            payload: {event_type: order_packed}
    commands:
      - >-
        desflow run shop.yaml --var PACKERS=3 --until 3600 --seed 7
        --summary result.json
    outputFiles:
      - result.json

  - id: read_result
    type: io.kestra.plugin.core.log.Log
    message: "{{ read(outputs.run_shop.outputFiles['result.json']) }}"
```

### Start Kestra

This starts Kestra in one container, with a login for its API and its web page:

```bash title="docs/snippets/orchestrators/commands.sh:kestra-start"
# Kestra 2.0.10 on http://localhost:8080, with a login: the user name must be an email
# address, and the password at least 8 characters with an upper-case letter, a
# lower-case letter and a digit. The Docker socket and /tmp let it start task containers.
docker run --pull=always --rm -d --name kestra -p 8080:8080 --user=root \
  -e 'KESTRA_CONFIGURATION=kestra:
  server:
    basic-auth:
      username: admin@example.com
      password: Desflow-shop-1' \
  -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp \
  kestra/kestra:v2.0.10-slim server local
```

`server local` keeps Kestra's data inside the container and installs the plugins a flow uses when it first needs them, so the first run of the flow downloads the Python script plugin.

### Run the Flow Once

These commands create the flow through Kestra's API, then run it and wait until it ends:

```bash title="docs/snippets/orchestrators/commands.sh:kestra-run"
LOGIN=admin@example.com:Desflow-shop-1
# Create the flow, then run it and wait until it ends
curl -sf -u "$LOGIN" -X POST -H "Content-Type: application/x-yaml" \
  --data-binary @kestra_flow.yaml http://localhost:8080/api/v1/main/flows > /dev/null
curl -sf -u "$LOGIN" -X POST \
  "http://localhost:8080/api/v1/main/executions/desflow.examples/shop_study?wait=true" |
  python3 -c 'import json, sys; e = json.load(sys.stdin); print(e["id"], e["state"]["current"])'
```

---

## Check the Result

The last command prints the execution's id and its state, which is `SUCCESS` when both tasks succeed. The id differs on each run:

```text
2wANPoP5mcql0TtBrQBXIq SUCCESS
```

Kestra's web page at http://localhost:8080 shows the execution, with the log of `read_result`, which holds `result.json`, and the file itself under the outputs of `run_shop`. The three figures in that file are the figures of the command run in a terminal. They are these three entries of its `statistics` list:

```json
{"statistic": "resources.packers.utilization_avg", "unit": "percent", "population": "time average", "value": 49.47475468873524}
{"statistic": "tasks.pack.finished", "unit": "count", "population": "count over the horizon", "value": 1775}
{"statistic": "tasks.pack.wait_mean", "unit": "s", "population": "started instances", "value": 0.25614739363776756}
```

Stop Kestra when you are done. `--rm` removes the container, with the flow and its executions:

```bash title="docs/snippets/orchestrators/commands.sh:kestra-stop"
docker stop kestra
```

---

## Sources

The task's properties are those of `io.kestra.plugin.scripts.python.Commands` in `kestra-io/plugin-scripts` 1.18.9 (`Commands.java` and `AbstractPythonExecScript.java`). The API paths are those of `FlowController.java` and `ExecutionController.java` in Kestra 2.0.10. The Docker command and the login rules follow Kestra's [Docker](https://kestra.io/docs/installation/docker) installation page.
