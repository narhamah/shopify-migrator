FROM node:22-bookworm-slim AS base

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates openssl \
  && rm -rf /var/lib/apt/lists/*

FROM base AS build

COPY package*.json ./
RUN npm ci

COPY . .

RUN npx prisma generate \
  && npm run build \
  && npm prune --omit=dev

FROM base AS runtime

ENV NODE_ENV=production
ENV HOST=0.0.0.0

COPY --from=build /app/package*.json ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/build ./build
COPY --from=build /app/prisma ./prisma

EXPOSE 3000

CMD ["sh", "-c", "npm run start:railway"]
