FROM node:24-alpine AS deps

WORKDIR /app

RUN corepack enable && corepack prepare pnpm@latest --activate

COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

# --- Dev stage: Vite HMR server ---
FROM deps AS dev
COPY . .
CMD ["pnpm", "dev", "--host"]

# --- Production stage ---
FROM deps AS build
COPY . .
RUN pnpm build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
