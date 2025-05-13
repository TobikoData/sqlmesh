FROM mcr.microsoft.com/playwright:v1.49.0-jammy 

WORKDIR /app

RUN apt-get update && apt-get -y install libnss3 libatk-bridge2.0-0 libdrm-dev libxkbcommon-dev libgbm-dev libasound-dev libatspi2.0-0 libxshmfence-dev

# Copy package files for workspaces
COPY package.json /app/package.json
COPY pnpm-lock.yaml /app/pnpm-lock.yaml
COPY pnpm-workspace.yaml /app/pnpm-workspace.yaml
COPY web/client/package.json /app/web/client/package.json
COPY vscode/extension/package.json /app/vscode/extension/package.json
COPY vscode/bus/package.json /app/vscode/bus/package.json

RUN npm install -g pnpm@latest && \
    pnpm install --frozen-lockfile

# Copy the rest of the application
COPY web/client/ /app/web/client/

# Install dependencies
RUN pnpm install --frozen-lockfile
