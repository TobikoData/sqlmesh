FROM node:19-alpine

WORKDIR /app

COPY web/client/package*.json .

RUN npm install -g npm@latest && \
    npm install --no-audit --no-fund --no-package-lock
