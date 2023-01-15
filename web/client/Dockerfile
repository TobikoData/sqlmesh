FROM node:19

RUN useradd -ms /bin/bash admin

USER admin

WORKDIR /app

RUN chown -R admin:admin /app

COPY package*.json ./

RUN npm install --no-optional ---no-audit --no-fund --no-package-lock

COPY . .
