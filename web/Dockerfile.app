FROM mcr.microsoft.com/playwright:v1.49.0-jammy 

WORKDIR /app

RUN apt-get update && apt-get -y install libnss3 libatk-bridge2.0-0 libdrm-dev libxkbcommon-dev libgbm-dev libasound-dev libatspi2.0-0 libxshmfence-dev

# Install pnpm globally
RUN npm install -g pnpm@latest

# Copy package files for workspaces
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY web/client/package.json ./web/client/
COPY vscode/extension/package.json ./vscode/extension/
COPY vscode/bus/package.json ./vscode/bus/

# Copy source files
COPY web/client/ ./web/client/
COPY vscode/extension/ ./vscode/extension/
COPY vscode/bus/ ./vscode/bus/

# Install dependencies
RUN pnpm install --frozen-lockfile
