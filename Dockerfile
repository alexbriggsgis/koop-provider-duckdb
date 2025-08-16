# From https://nodejs.org/en/docs/guides/nodejs-docker-webapp/
FROM node:18-slim

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
COPY docker-demo/ ./
RUN npm install

#copy duckdb provider and install dependencies
WORKDIR /usr/src/app/duckdb
COPY src/ ./src/
COPY package*.json ./
RUN npm install

# change to the app directory
WORKDIR /usr/src/app

EXPOSE 9001
CMD [ "node", "src/index.js" ]
