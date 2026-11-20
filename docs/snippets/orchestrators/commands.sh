# Commands from docs/integrations/*.md. tests/docs/test_docs_orchestrators.py runs the
# run commands of each page in a folder of its own, with the orchestrator installed
# from its dependency group; the install commands were run once by hand in an empty
# virtual environment.

# --8<-- [start:direct]
desflow run shop.yaml --var PACKERS=3 --until 3600 --seed 7 --summary result.json
# --8<-- [end:direct]

# --8<-- [start:airflow-install]
AIRFLOW_VERSION=3.4.0
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
pip install "desflow>=1.1" "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# --8<-- [end:airflow-install]

# --8<-- [start:airflow-run]
# From the DAGs folder, ~/airflow/dags unless AIRFLOW_HOME names another home
airflow db migrate
python airflow_dag.py
# --8<-- [end:airflow-run]

# --8<-- [start:dagster-install]
pip install "desflow>=1.1" dagster
# --8<-- [end:dagster-install]

# --8<-- [start:dagster-run]
python dagster_asset.py
# --8<-- [end:dagster-run]

# --8<-- [start:prefect-install]
pip install "desflow>=1.1" prefect
# --8<-- [end:prefect-install]

# --8<-- [start:prefect-run]
python prefect_flow.py
# --8<-- [end:prefect-run]

# --8<-- [start:kestra-start]
# Kestra 2.0.10 on http://localhost:8080, published on the loopback address, with
# a login: the user name must be an email address, and the password at least 8
# characters with an upper-case letter, a lower-case letter and a digit. The
# Docker socket and /tmp let it start task containers.
docker run --pull=always --rm -d --name kestra -p 127.0.0.1:8080:8080 --user=root \
  -e 'KESTRA_CONFIGURATION=kestra:
  server:
    basic-auth:
      username: admin@example.com
      password: Desflow-shop-1' \
  -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp \
  kestra/kestra:v2.0.10-slim server local
# --8<-- [end:kestra-start]

# --8<-- [start:kestra-run]
LOGIN=admin@example.com:Desflow-shop-1
# Create the flow, then run it and wait until it ends
curl -sf -u "$LOGIN" -X POST -H "Content-Type: application/x-yaml" \
  --data-binary @kestra_flow.yaml http://localhost:8080/api/v1/main/flows > /dev/null
curl -sf -u "$LOGIN" -X POST \
  "http://localhost:8080/api/v1/main/executions/desflow.examples/shop_study?wait=true" |
  python3 -c 'import json, sys; e = json.load(sys.stdin); print(e["id"], e["state"]["current"])'
# --8<-- [end:kestra-run]

# --8<-- [start:kestra-stop]
docker stop kestra
# --8<-- [end:kestra-stop]
