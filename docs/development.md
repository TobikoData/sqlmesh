# Contribute to development
SQLMesh is licensed under [Apache 2.0](https://github.com/TobikoData/sqlmesh/blob/main/LICENSE). We encourage community contribution and would love for you to get involved.

## Prerequisites
* Docker
* Docker Compose V2
* OpenJDK >= 11

## Commands reference

Install dev dependencies:
```
make install-dev
```
Run linters and formatters:
```
make style
```
Run tests:
```
make test
```
Run docs server:
```
make docs-serve
```
Run ide:
```
make web-serve
```
(Optional) Use pre-commit to automatically run linters/formatters:
```
make install-pre-commit
```
