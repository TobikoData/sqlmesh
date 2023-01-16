FROM python:3.11

RUN useradd -ms /bin/bash admin

USER admin

WORKDIR /api

RUN chown -R admin:admin /api

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .
