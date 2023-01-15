.PHONY: docs

install-dev:
	pip install -e ".[dev,web]"

install-pre-commit:
	pre-commit install

style:
	pre-commit run --all-files

unit-test:
	pytest -m "not integration"

doc-test:
	PYTEST_PLUGINS=tests.common_fixtures pytest --doctest-modules sqlmesh/core sqlmesh/utils

core-it-test:
	pytest -m "core_integration"

it-test: core-it-test airflow-it-test-with-env

it-test-docker: core-it-test airflow-it-test-docker-with-env

test: unit-test it-test doc-test

airflow-init:
	export AIRFLOW_ENGINE_OPERATOR=spark && make -C ./example/airflow init

airflow-run:
	make -C ./example/airflow run

airflow-stop:
	make -C ./example/airflow stop

airflow-clean:
	make -C ./example/airflow clean

airflow-psql:
	make -C ./example/airflow psql

airflow-spark-sql:
	make -C ./example/airflow spark-sql

airflow-it-test:
	export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost/airflow && \
		pytest -m "airflow_integration"

airflow-it-test-docker:
	make -C ./example/airflow it-test-docker

airflow-it-test-with-env: airflow-clean airflow-init airflow-run airflow-it-test airflow-stop

airflow-it-test-docker-with-env: airflow-clean airflow-init airflow-run airflow-it-test-docker airflow-stop

docs-serve:
	mkdocs serve

api-docs:
	pdoc/cli.py -o docs

api-docs-serve:
	pdoc/cli.py

ui:
	docker compose -f ./web/docker-compose.yml up -d
