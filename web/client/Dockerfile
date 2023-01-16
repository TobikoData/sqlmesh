FROM node:19

WORKDIR /app

COPY package*.json ./

RUN npm install --no-optional ---no-audit --no-fund --no-package-lock

COPY . .
