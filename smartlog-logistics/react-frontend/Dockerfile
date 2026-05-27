# Stage 1: Build React app
FROM node:20-alpine AS build
WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install

COPY . .
RUN npm run build

# Stage 2: Serve bằng Nginx + proxy API
FROM nginx:alpine AS runtime

COPY --from=build /app/dist /usr/share/nginx/html

# Nginx config: serve React SPA + proxy /api → gateway
RUN printf 'server {\n\
    listen 80;\n\
\n\
    # Proxy API calls sang smartlog-gateway\n\
    location /api/ {\n\
        proxy_pass http://nginx/api/;\n\
        proxy_set_header Host $host;\n\
        proxy_set_header X-Real-IP $remote_addr;\n\
    }\n\
\n\
    # Serve React SPA\n\
    location / {\n\
        root /usr/share/nginx/html;\n\
        index index.html;\n\
        try_files $uri $uri/ /index.html;\n\
    }\n\
}\n' > /etc/nginx/conf.d/default.conf

EXPOSE 80