FROM node:20-bookworm-slim

WORKDIR /app

COPY packages/ingestion/package*.json ./
RUN npm install

COPY packages/ingestion/ ./

ENV NODE_ENV=production

CMD ["npm", "run", "worker"]
