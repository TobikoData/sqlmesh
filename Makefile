.PHONY: docs

install-dev:
	pip3 install -e ".[dev,web,slack]"

install-cicd-test:
	pip3 install -e ".[dev,web,slack,cicdtest]"

install-doc:
	pip3 install -r ./docs/requirements.txt

install-engine-test:
	pip3 install -e ".[dev,web,slack,mysql,postgres,databricks,redshift,bigquery,snowflake,trino,mssql,motherduck]"

install-pre-commit:
	pre-commit install

style:
	pre-commit run --all-files

py-style:
	SKIP=prettier,eslint pre-commit run --all-files

ui-style:
	SKIP=autoflake,isort,black,mypy pre-commit run --all-files

doc-test:
	PYTEST_PLUGINS=tests.common_fixtures pytest --doctest-modules sqlmesh/core sqlmesh/utils

package:
	pip3 install wheel && python3 setup.py sdist bdist_wheel

publish: package
	pip3 install twine && python3 -m twine upload dist/*

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
	docker-compose up --build -d && $(if $(shell which open), open http://localhost:8001, echo "Open http://localhost:8001 in your browser.")

ui-down:
	docker-compose down

ui-build:
	docker-compose -f docker-compose.yml -f docker-compose.build.yml run app

clean-build:
	rm -rf build/ && rm -rf dist/ && rm -rf *.egg-info

dev-publish: ui-build clean-build publish

jupyter-example:
	jupyter lab tests/slows/jupyter/example_outputs.ipynb

engine-up:
	docker-compose -f ./tests/core/engine_adapter/docker-compose.yaml up -d

engine-down:
	docker-compose -f ./tests/core/engine_adapter/docker-compose.yaml down

fast-test:
	pytest -n auto -m "fast and not cicdonly"

slow-test:
	pytest -n auto -m "(fast or slow) and not cicdonly"

cicd-test:
	pytest -n auto -m "fast or slow"

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

bigquery-test:
	pytest -n auto -m "bigquery"

databricks-test:
	pytest -n auto -m "databricks"

duckdb-test:
	pytest -n auto -m "duckdb"

mssql-test:
	pytest -n auto -m "mssql"

mysql-test:
	pytest -n auto -m "mysql"

postgres-test:
	pytest -n auto -m "postgres"

redshift-test:
	pytest -n auto -m "redshift"

snowflake-test:
	pytest -n auto -m "snowflake"

spark-test:
	pytest -n auto -m "spark"

spark-pyspark-test:
	pytest -n auto -m "spark_pyspark"

trino-test:
	pytest -n auto -m "trino or trino_iceberg"