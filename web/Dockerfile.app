FROM mcr.microsoft.com/playwright:v1.49.0-jammy 

WORKDIR /app

RUN apt-get update && apt-get -y install libnss3 libatk-bridge2.0-0 libdrm-dev libxkbcommon-dev libgbm-dev libasound-dev libatspi2.0-0 libxshmfence-dev

# Install pnpm globally
RUN npm install -g pnpm@latest

# Copy package files for workspaces
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY web/client/package.json ./web/client/

# Install dependencies
RUN pnpm install --frozen-lockfile

# Copy source files (excluding node_modules which were installed above)
COPY web/client/ ./web/client/
