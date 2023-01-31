FROM node:19-alpine

WORKDIR /app

COPY web/client/package*.json .

RUN npm install -g npm@latest && \
    npm run init
