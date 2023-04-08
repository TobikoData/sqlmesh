AIRFLOW_VERSION ?= 2.3.3
AIRFLOW_IMAGE_NAME ?= airflow-sqlmesh
AIRFLOW_UID ?= $(shell id -u)

install-requirements:
	pip3 install -r requirements.txt

download-docker-compose:
	curl -LfO 'https://airflow.apache.org/docs/apache-airflow/$(AIRFLOW_VERSION)/docker-compose.yaml'

decorate-docker-compose: install-requirements download-docker-compose
	python3 ./docker_compose_decorator.py

download-cli:
	curl -LfO 'https://airflow.apache.org/docs/apache-airflow/$(AIRFLOW_VERSION)/airflow.sh' && chmod +x airflow.sh

package-sqlmesh:
	make -C ../../ package

init-folders:
	mkdir -p ./dags ./logs ./plugins ./warehouse

init-airflow-dockerfile:
	export AIRFLOW_VERSION=$(AIRFLOW_VERSION) AIRFLOW_UID=$(AIRFLOW_UID) && cat Dockerfile.template | envsubst '$$AIRFLOW_VERSION,$$AIRFLOW_UID' > Dockerfile

build-airflow-image: init-airflow-dockerfile
	cd ../../ && docker build -t $(AIRFLOW_IMAGE_NAME) -f ./examples/airflow/Dockerfile .

create-metastore-db: build-airflow-image decorate-docker-compose
	export AIRFLOW_IMAGE_NAME=$(AIRFLOW_IMAGE_NAME) AIRFLOW_UID=$(AIRFLOW_UID) && docker-compose up create-metastore-db

provision-metastore-tables: build-airflow-image decorate-docker-compose create-metastore-db
	export AIRFLOW_IMAGE_NAME=$(AIRFLOW_IMAGE_NAME) AIRFLOW_UID=$(AIRFLOW_UID) && docker-compose up provision-metastore-tables

init-airflow: decorate-docker-compose
	export AIRFLOW_IMAGE_NAME=$(AIRFLOW_IMAGE_NAME) AIRFLOW_UID=$(AIRFLOW_UID) && docker-compose up airflow-init

init: decorate-docker-compose download-cli package-sqlmesh init-folders build-airflow-image init-airflow provision-metastore-tables

run: build-airflow-image
	export AIRFLOW_IMAGE_NAME=$(AIRFLOW_IMAGE_NAME) AIRFLOW_UID=$(AIRFLOW_UID) && docker-compose up -d

stop:
	docker-compose down

clean: decorate-docker-compose
	docker-compose down --volumes --remove-orphans && docker rmi -f $(AIRFLOW_IMAGE_NAME) && rm -rf ./logs/* && rm -rf ./warehouse/*

psql:
	docker exec -it airflow-postgres-1 psql -U airflow airflow

spark-sql:
	docker exec -it airflow-airflow-worker-1 spark-sql

it-test-docker: decorate-docker-compose
	docker-compose up --exit-code-from sqlmesh-tests sqlmesh-tests
