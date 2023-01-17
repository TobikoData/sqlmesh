FROM python:3.11

WORKDIR /sqlmesh

COPY . .

RUN pip install -e .[dev,web]
