FROM mcr.microsoft.com/playwright:v1.49.0-jammy 

WORKDIR /app

ENV PATH /app/node_modules/.bin:$PATH

RUN apt-get update && apt-get -y install libnss3 libatk-bridge2.0-0 libdrm-dev libxkbcommon-dev libgbm-dev libasound-dev libatspi2.0-0 libxshmfence-dev

# Copy package files for workspaces
COPY package.json /app/package.json
COPY package-lock.json /app/package-lock.json
COPY web/client/package.json /app/web/client/package.json
COPY vscode/extension/package.json /app/vscode/extension/package.json

RUN npm install -g npm@latest && \
    npm install --no-audit --no-fund

# Copy the rest of the application
COPY web/client/ /app/web/client/

# Install dependencies
RUN npm install --no-audit --no-fund
