# Contribute to development
SQLMesh is licensed under [Apache 2.0](https://github.com/TobikoData/sqlmesh/blob/main/LICENSE). We encourage community contribution and would love for you to get involved.

## Prerequisites
* Docker
* Docker Compose V2
* OpenJDK >= 11

## Commands reference

Install dev dependencies:
```bash
make install-dev
```
Run linters and formatters:
```bash
make style
```
Run faster tests for quicker local feedback:
```bash
make fast-test
```
Run more comprehensive tests that run on each commit:
```bash
make slow-test
```
Run Airflow tests that will run when PR is merged to main:
```bash
make airflow-docker-test-with-env
```
Install docs dependencies:
```bash
make install-doc
```
Run docs server:
```bash
make docs-serve
```
Run docs tests:
```bash
make doc-test
```
Run ide:
```bash
make ui-up
```
(Optional) Use pre-commit to automatically run linters/formatters:
```bash
make install-pre-commit
```
