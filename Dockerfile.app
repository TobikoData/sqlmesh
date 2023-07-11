FROM mcr.microsoft.com/playwright:v1.36.0-jammy

WORKDIR /app

ENV PATH /app/node_modules/.bin:$PATH

RUN apt-get update && apt-get -y install libnss3 libatk-bridge2.0-0 libdrm-dev libxkbcommon-dev libgbm-dev libasound-dev libatspi2.0-0 libxshmfence-dev

COPY web/client/package*.json .

RUN npm install -g npm@latest && \
    npm install --no-audit --no-fund --no-package-lock
