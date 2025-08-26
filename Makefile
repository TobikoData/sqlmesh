.PHONY: docs

ifdef UV
    PIP := uv pip
else
    PIP := pip3
endif

install-dev:
	$(PIP) install -e ".[dev,web,slack,dlt,lsp]" ./examples/custom_materializations

install-doc:
	$(PIP) install -r ./docs/requirements.txt

install-pre-commit:
	pre-commit install

style:
	pre-commit run --all-files

py-style:
	SKIP=prettier,eslint pre-commit run --all-files

ui-style:
	pnpm run lint

doc-test:
	python -m pytest --doctest-modules sqlmesh/core sqlmesh/utils

package:
	$(PIP) install build && python3 -m build

publish: package
	$(PIP) install twine && python3 -m twine upload dist/*

package-tests:
	$(PIP) install build && cp pyproject.toml tests/sqlmesh_pyproject.toml && python3 -m build tests/

publish-tests: package-tests
	$(PIP) install twine && python3 -m twine upload -r tobiko-private tests/dist/*

docs-serve:
	mkdocs serve

api-docs:
	python pdoc/cli.py -o docs/_readthedocs/html/

api-docs-serve:
	python pdoc/cli.py

ui-up:
	docker compose -f ./web/docker-compose.yml up --build -d && $(if $(shell which open), open http://localhost:8001, echo "Open http://localhost:8001 in your browser.")

ui-down:
	docker compose -f ./web/docker-compose.yml down

ui-build:
	docker compose -f ./web/docker-compose.yml -f ./web/docker-compose.build.yml run app

clean-build:
	rm -rf build/ && rm -rf dist/ && rm -rf *.egg-info

dev-publish: ui-build clean-build publish

jupyter-example:
	jupyter lab tests/slows/jupyter/example_outputs.ipynb

engine-up: engine-clickhouse-up engine-mssql-up engine-mysql-up engine-postgres-up engine-spark-up engine-trino-up

engine-down: engine-clickhouse-down engine-mssql-down engine-mysql-down engine-postgres-down engine-spark-down engine-trino-down

fast-test:
	pytest -n auto -m "fast and not cicdonly" --junitxml=test-results/junit-fast-test.xml && pytest -m "isolated" && pytest -m "registry_isolation"

slow-test:
	pytest -n auto -m "(fast or slow) and not cicdonly" && pytest -m "isolated" && pytest -m "registry_isolation"

cicd-test:
	pytest -n auto -m "fast or slow" --junitxml=test-results/junit-cicd.xml && pytest -m "isolated" && pytest -m "registry_isolation"

core-fast-test:
	pytest -n auto -m "fast and not web and not github and not dbt and not jupyter"

core-slow-test:
	pytest -n auto -m "(fast or slow) and not web and not github and not dbt and not jupyter"

engine-slow-test:
	pytest -n auto -m "(fast or slow) and engine"

engine-docker-test:
	pytest -n auto -m "docker and engine"

engine-remote-test:
	pytest -n auto -m "remote and engine"

engine-test:
	pytest -n auto -m "engine"

dbt-test:
	pytest -n auto -m "dbt and not cicdonly"

dbt-fast-test:
	pytest -n auto -m "dbt and fast" --retries 3

github-test:
	pytest -n auto -m "github"

jupyter-test:
	pytest -n auto -m "jupyter"

web-test:
	pytest -n auto -m "web"

guard-%:
	@ if [ "${${*}}" = "" ]; then \
		echo "Environment variable $* not set"; \
		exit 1; \
	fi

engine-%-install:
	$(PIP) install -e ".[dev,web,slack,lsp,${*}]" ./examples/custom_materializations

engine-docker-%-up:
	docker compose -f ./tests/core/engine_adapter/integration/docker/compose.${*}.yaml up -d
	./.circleci/wait-for-db.sh ${*}

engine-%-up: engine-%-install engine-docker-%-up
	@echo "Engine '${*}' is up and running"

engine-%-down:
	docker compose -f ./tests/core/engine_adapter/integration/docker/compose.${*}.yaml down -v

##################
# Docker Engines #
##################

clickhouse-test: engine-clickhouse-up
	pytest -n auto -m "clickhouse" --retries 3 --junitxml=test-results/junit-clickhouse.xml

duckdb-test: engine-duckdb-install
	pytest -n auto -m "duckdb" --retries 3 --junitxml=test-results/junit-duckdb.xml

mssql-test: engine-mssql-up
	pytest -n auto -m "mssql" --retries 3 --junitxml=test-results/junit-mssql.xml

mysql-test: engine-mysql-up
	pytest -n auto -m "mysql" --retries 3 --junitxml=test-results/junit-mysql.xml

postgres-test: engine-postgres-up
	pytest -n auto -m "postgres" --retries 3 --junitxml=test-results/junit-postgres.xml

spark-test: engine-spark-up
	pytest -n auto -m "spark" --retries 3 --junitxml=test-results/junit-spark.xml

trino-test: engine-trino-up
	pytest -n auto -m "trino" --retries 3 --junitxml=test-results/junit-trino.xml

risingwave-test: engine-risingwave-up
	pytest -n auto -m "risingwave" --retries 3 --junitxml=test-results/junit-risingwave.xml

#################
# Cloud Engines #
#################

snowflake-test: guard-SNOWFLAKE_ACCOUNT guard-SNOWFLAKE_WAREHOUSE guard-SNOWFLAKE_DATABASE guard-SNOWFLAKE_USER guard-SNOWFLAKE_PASSWORD engine-snowflake-install
	pytest -n auto -m "snowflake" --retries 3 --junitxml=test-results/junit-snowflake.xml

bigquery-test: guard-BIGQUERY_KEYFILE engine-bigquery-install
	$(PIP) install -e ".[bigframes]"
	pytest -n auto -m "bigquery" --retries 3 --junitxml=test-results/junit-bigquery.xml

databricks-test: guard-DATABRICKS_CATALOG guard-DATABRICKS_SERVER_HOSTNAME guard-DATABRICKS_HTTP_PATH guard-DATABRICKS_ACCESS_TOKEN guard-DATABRICKS_CONNECT_VERSION engine-databricks-install
	$(PIP) install 'databricks-connect==${DATABRICKS_CONNECT_VERSION}'
	pytest -n auto -m "databricks" --retries 3 --junitxml=test-results/junit-databricks.xml

redshift-test: guard-REDSHIFT_HOST guard-REDSHIFT_USER guard-REDSHIFT_PASSWORD guard-REDSHIFT_DATABASE engine-redshift-install
	pytest -n auto -m "redshift" --retries 3 --junitxml=test-results/junit-redshift.xml

clickhouse-cloud-test: guard-CLICKHOUSE_CLOUD_HOST guard-CLICKHOUSE_CLOUD_USERNAME guard-CLICKHOUSE_CLOUD_PASSWORD engine-clickhouse-install
	pytest -n 1 -m "clickhouse_cloud" --retries 3 --junitxml=test-results/junit-clickhouse-cloud.xml

athena-test: guard-AWS_ACCESS_KEY_ID guard-AWS_SECRET_ACCESS_KEY guard-ATHENA_S3_WAREHOUSE_LOCATION engine-athena-install
	pytest -n auto -m "athena" --retries 3 --junitxml=test-results/junit-athena.xml

fabric-test: guard-FABRIC_HOST guard-FABRIC_CLIENT_ID guard-FABRIC_CLIENT_SECRET guard-FABRIC_DATABASE engine-fabric-install
	pytest -n auto -m "fabric" --retries 3 --junitxml=test-results/junit-fabric.xml

gcp-postgres-test: guard-GCP_POSTGRES_INSTANCE_CONNECTION_STRING guard-GCP_POSTGRES_USER guard-GCP_POSTGRES_PASSWORD guard-GCP_POSTGRES_KEYFILE_JSON engine-gcppostgres-install
	pytest -n auto -m "gcp_postgres" --retries 3 --junitxml=test-results/junit-gcp-postgres.xml

vscode_settings:
	mkdir -p .vscode
	cp -r ./tooling/vscode/*.json .vscode/

vscode-generate-openapi:
	python3 web/server/openapi.py --output vscode/openapi.json
	pnpm run fmt
	cd vscode/react && pnpm run generate:api

benchmark-ci:
	python benchmarks/lsp_render_model_bench.py --debug-single-value
