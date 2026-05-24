# 🚀 Smartlog Logistics Platform

**.NET 8 Microservices + Kafka + PostgreSQL + AWS S3 + Docker + Angular**

---

## ⚡ Chạy ngay (2 cách)

### Cách 1 — Local dev (VSCode/VS)
```bash
# Bước 1: Setup một lần duy nhất
chmod +x setup.sh && ./setup.sh

# Bước 2: Chạy services (mở 2 terminal trong VSCode)
# Terminal 1:
cd OrderService && dotnet run

# Terminal 2:
cd TrackingService && dotnet run
```

### Cách 2 — Full Docker (không cần .NET trên máy)
```bash
chmod +x run-docker.sh && ./run-docker.sh
```

---

## 🗺️ Architecture

```
Angular :4200 ──┐
React   :3000 ──┼──► Nginx :80 (API Gateway)
Mobile  (RN)  ──┘        │
                    ┌─────┴──────────────┐
                    │                    │
             OrderService          TrackingService
                :5001                   :5002
             [JWT Auth]           [Kafka Consumer]
             [S3 Upload]          [Timeline API]
                    │                    │
                    └────── Kafka ───────┘
                        order.created
                        order.status.updated
                    │                    │
               PostgreSQL          PostgreSQL
                orderdb             trackingdb
                    │
                 AWS S3
              (file attachments)
```

---

## 🌐 URLs

| Service | URL |
|---|---|
| Order API Swagger | http://localhost:5001/swagger |
| Tracking API Swagger | http://localhost:5002/swagger |
| Kafka UI (monitor) | http://localhost:8090 |
| Nginx Gateway | http://localhost |

---

## 🔑 Demo Credentials

| Username | Password | Role |
|---|---|---|
| admin | smartlog123 | Admin (full access) |
| driver | driver123 | Driver (limited) |

---

## 📡 API Reference

### Auth
```
POST /api/auth/login
{ "username": "admin", "password": "smartlog123" }
→ { "token": "eyJ...", "expiresIn": 86400 }
```

### Orders (Bearer token required)
```
GET    /api/orders?page=1&pageSize=10
GET    /api/orders/{id}
POST   /api/orders
PATCH  /api/orders/{id}/status  { "status": "Confirmed" }
POST   /api/orders/{id}/attachments  (multipart file → S3)
DELETE /api/orders/{id}
```

Valid statuses: `Pending` → `Confirmed` → `PickedUp` → `InTransit` → `Delivered` / `Cancelled`

### Tracking (public, no auth)
```
GET /api/tracking/{orderCode}
GET /api/tracking?page=1&size=10
```

---

## 🔄 Kafka Event Flow

```
1. POST /api/orders
   → Save to orderdb (EF Core)
   → Kafka.Publish("order.created", event)
   → TrackingService receives → creates TrackingRecord

2. PATCH /api/orders/{id}/status
   → Update orderdb
   → Kafka.Publish("order.status.updated", event)
   → TrackingService receives → appends to Timeline
```

Monitor events live at: **http://localhost:8090**

---

## 🅰️ Connect Angular Frontend

```bash
cd frontend-angular
npm install
ng serve   # → http://localhost:4200
```

API already configured in `src/environments/environment.ts`:
- Login: `POST http://localhost:5001/api/auth/login`
- Orders: `GET/POST http://localhost:5001/api/orders`
- Tracking: `GET http://localhost:5002/api/tracking/{code}`

Pages ready: `LoginComponent`, `OrdersComponent`, `TrackingComponent`

---

## ☁️ AWS S3 Setup (optional)

Edit `.env`:
```
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
AWS_S3_BUCKET=your-bucket
```

Without S3 credentials, file upload still works (returns local placeholder URL).

---

## 🚀 CI/CD (GitHub Actions)

```
git push main
  → dotnet build + test
  → docker build + push to GHCR
  → AWS ECS update-service
  → wait services-stable
```

Required GitHub Secrets: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`

---

## 🛠️ VSCode Dev Setup

Extensions to install:
- **C# Dev Kit** (Microsoft)
- **Docker** (Microsoft)
- **REST Client** (test .http files)
- **Thunder Client** (API testing)
- **GitLens**

---

## 📁 Project Structure

```
smartlog-logistics/
├── setup.sh                    ← Run this first!
├── run-docker.sh               ← Or this for full Docker
├── docker-compose.yml
├── nginx/nginx.conf            ← API Gateway
├── .env / .env.example         ← All config here
│
├── Shared/                     ← Shared library
│   ├── Auth/JwtSettings.cs
│   ├── Config/AwsSettings.cs
│   ├── Events/                 ← Kafka event DTOs
│   └── Contracts/KafkaTopics.cs
│
├── OrderService/               ← Microservice 1
│   ├── Controllers/            ← Auth + Orders endpoints
│   ├── Services/               ← Business logic
│   ├── Repositories/           ← Repository pattern
│   ├── Data/Migrations/        ← EF Core migrations
│   ├── Infrastructure/
│   │   ├── Kafka/Producer      ← Publish events
│   │   └── AWS/S3Service       ← File upload
│   └── Middleware/             ← Global error handler
│
├── TrackingService/            ← Microservice 2
│   ├── Controllers/            ← Tracking API
│   ├── Infrastructure/Kafka/   ← Consumer (BackgroundService)
│   └── Data/                   ← TrackingRecord schema
│
└── frontend-angular/           ← Angular 17 ready to connect
    ├── src/app/
    │   ├── services/api.service.ts    ← All API calls
    │   ├── guards/auth.guard.ts       ← Route protection
    │   ├── interceptors/auth.interceptor.ts ← Auto JWT
    │   └── pages/
    │       ├── login/                 ← Login page
    │       ├── orders/                ← Order list
    │       └── tracking/              ← Track shipment
    └── src/environments/
        ├── environment.ts             ← Dev URLs
        └── environment.prod.ts        ← Prod URLs (Nginx)
```
