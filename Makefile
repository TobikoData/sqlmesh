.PHONY: docs

install-dev:
	pip3 install -e ".[dev,web,slack]"

install-engine-integration:
	pip3 install -e ".[dev,web,slack,mysql,postgres,databricks,redshift,bigquery,snowflake]"

install-pre-commit:
	pre-commit install

style:
	pre-commit run --all-files

py-style:
	SKIP=prettier,eslint pre-commit run --all-files

ui-style:
	SKIP=autoflake,isort,black,mypy pre-commit run --all-files

unit-test:
	pytest -m "not integration"

doc-test:
	PYTEST_PLUGINS=tests.common_fixtures pytest --doctest-modules sqlmesh/core sqlmesh/utils

core-it-test:
	pytest -m "core_integration"

core_engine_it_test:
	pytest -m "engine_integration"

core_engine_it_test_docker:
	docker-compose -f ./tests/core/engine_adapter/docker-compose.yaml up -d

engine_it_test: core_engine_it_test_docker core_engine_it_test

it-test: core-it-test airflow-it-test-with-env

it-test-docker: core-it-test airflow-it-test-docker-with-env

test: unit-test doc-test it-test

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

airflow-it-test:
	export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost/airflow && \
		pytest -m "airflow_integration"

airflow-it-test-docker:
	make -C ./examples/airflow it-test-docker

airflow-it-test-with-env: develop airflow-clean airflow-init airflow-run airflow-it-test airflow-stop

airflow-it-test-docker-with-env: develop airflow-clean airflow-init airflow-run airflow-it-test-docker airflow-stop

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

dev-publish: clean-build ui-build publish