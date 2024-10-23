.PHONY: docs

install-dev:
	pip3 install -e ".[dev,web,slack,dlt]"

install-cicd-test:
	pip3 install -e ".[dev,web,slack,cicdtest,dlt]"

install-doc:
	pip3 install -r ./docs/requirements.txt

install-engine-test:
	pip3 install -e ".[dev,web,slack,mysql,postgres,databricks,redshift,bigquery,snowflake,trino,mssql,clickhouse,athena]"

install-pre-commit:
	pre-commit install

style:
	pre-commit run --all-files

py-style:
	SKIP=prettier,eslint pre-commit run --all-files

ui-style:
	SKIP=ruff,ruff-format,mypy pre-commit run --all-files

doc-test:
	PYTEST_PLUGINS=tests.common_fixtures pytest --doctest-modules sqlmesh/core sqlmesh/utils

package:
	pip3 install wheel && python3 setup.py sdist bdist_wheel

publish: package
	pip3 install twine && python3 -m twine upload dist/*

package-tests:
	pip3 install wheel && python3 tests/setup.py sdist bdist_wheel

publish-tests: package-tests
	pip3 install twine && python3 -m twine upload -r tobiko-private tests/dist/*

develop:
	python3 setup.py develop

airflow-init:
	export AIRFLOW_ENGINE_OPERATOR=spark && make -C ./examples/airflow init

airflow-run:
	make -C ./examples/airflow run

airflow-stop:
	make -C ./examples/airflow stop

airflow-clean:
	make -C ./examples/airflow clean

airflow-psql:
	make -C ./examples/airflow psql

airflow-spark-sql:
	make -C ./examples/airflow spark-sql

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
	pytest -n auto -m "fast and not cicdonly" && pytest -m "isolated"

slow-test:
	pytest -n auto -m "(fast or slow) and not cicdonly" && pytest -m "isolated"

cicd-test:
	pytest -n auto -m "fast or slow" --junitxml=test-results/junit-cicd.xml && pytest -m "isolated"

core-fast-test:
	pytest -n auto -m "fast and not web and not github and not dbt and not airflow and not jupyter"

core-slow-test:
	pytest -n auto -m "(fast or slow) and not web and not github and not dbt and not airflow and not jupyter"

airflow-fast-test:
	pytest -n auto -m "fast and airflow"

airflow-test:
	pytest -n auto -m "(fast or slow) and airflow"

airflow-local-test:
	export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost/airflow && \
		pytest -n 1 -m "docker and airflow"

airflow-docker-test:
	make -C ./examples/airflow docker-test

airflow-local-test-with-env: develop airflow-clean airflow-init airflow-run airflow-local-test airflow-stop

airflow-docker-test-with-env: develop airflow-clean airflow-init airflow-run airflow-docker-test airflow-stop

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
	pip3 install -e ".[dev,web,slack,${*}]"

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
	pytest -n auto -x -m "clickhouse" --retries 3 --junitxml=test-results/junit-clickhouse.xml

clickhouse-cluster-test: engine-clickhouse-up
	pytest -n auto -x -m "clickhouse_cluster" --retries 3 --junitxml=test-results/junit-clickhouse-cluster.xml

duckdb-test: engine-duckdb-install
	pytest -n auto -x -m "duckdb" --retries 3 --junitxml=test-results/junit-duckdb.xml

mssql-test: engine-mssql-up
	pytest -n auto -x -m "mssql" --retries 3 --junitxml=test-results/junit-mssql.xml

mysql-test: engine-mysql-up
	pytest -n auto -x -m "mysql" --retries 3 --junitxml=test-results/junit-mysql.xml

postgres-test: engine-postgres-up
	pytest -n auto -x -m "postgres" --retries 3 --junitxml=test-results/junit-postgres.xml

spark-test: engine-spark-up
	pytest -n auto -x -m "spark or pyspark" --retries 3 --junitxml=test-results/junit-spark.xml

trino-test: engine-trino-up
	pytest -n auto -x -m "trino or trino_iceberg or trino_delta" --retries 3 --junitxml=test-results/junit-trino.xml

#################
# Cloud Engines #
#################

snowflake-test: guard-SNOWFLAKE_ACCOUNT guard-SNOWFLAKE_WAREHOUSE guard-SNOWFLAKE_DATABASE guard-SNOWFLAKE_USER guard-SNOWFLAKE_PASSWORD engine-snowflake-install
	pytest -n auto -x -m "snowflake" --retries 3 --junitxml=test-results/junit-snowflake.xml

bigquery-test: guard-BIGQUERY_KEYFILE engine-bigquery-install
	pytest -n auto -x -m "bigquery" --retries 3 --junitxml=test-results/junit-bigquery.xml

databricks-test: guard-DATABRICKS_CATALOG guard-DATABRICKS_SERVER_HOSTNAME guard-DATABRICKS_HTTP_PATH guard-DATABRICKS_ACCESS_TOKEN guard-DATABRICKS_CONNECT_VERSION engine-databricks-install
	pip install 'databricks-connect==${DATABRICKS_CONNECT_VERSION}'
	pytest -n auto -x -m "databricks" --retries 3 --junitxml=test-results/junit-databricks.xml

redshift-test: guard-REDSHIFT_HOST guard-REDSHIFT_USER guard-REDSHIFT_PASSWORD guard-REDSHIFT_DATABASE engine-redshift-install
	pytest -n auto -x -m "redshift" --retries 3 --junitxml=test-results/junit-redshift.xml

clickhouse-cloud-test: guard-CLICKHOUSE_CLOUD_HOST guard-CLICKHOUSE_CLOUD_USERNAME guard-CLICKHOUSE_CLOUD_PASSWORD engine-clickhouse-install
	pytest -n auto -x -m "clickhouse_cloud" --retries 3 --junitxml=test-results/junit-clickhouse-cloud.xml

athena-test: guard-AWS_ACCESS_KEY_ID guard-AWS_SECRET_ACCESS_KEY guard-ATHENA_S3_WAREHOUSE_LOCATION engine-athena-install
	pytest -n auto -x -m "athena" --retries 3 --retry-delay 10 --junitxml=test-results/junit-athena.xml