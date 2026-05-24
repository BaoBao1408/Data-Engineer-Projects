#!/bin/bash
set -e

echo "╔══════════════════════════════════════════════════╗"
echo "║   Smartlog Logistics — Setup & Run               ║"
echo "╚══════════════════════════════════════════════════╝"

# ── Check requirements ────────────────────────────────────
command -v dotnet >/dev/null || { echo "❌ .NET 8 SDK required. Install: https://dotnet.microsoft.com/download"; exit 1; }
command -v docker >/dev/null || { echo "❌ Docker required. Install: https://docker.com"; exit 1; }
command -v docker-compose >/dev/null 2>&1 || docker compose version >/dev/null || { echo "❌ Docker Compose required"; exit 1; }

echo "✅ Requirements OK"
echo ""

# ── Install EF tools ──────────────────────────────────────
echo "📦 Installing dotnet-ef tool..."
dotnet tool install --global dotnet-ef 2>/dev/null || dotnet tool update --global dotnet-ef
export PATH="$PATH:$HOME/.dotnet/tools"

# ── Restore NuGet packages ────────────────────────────────
echo "📦 Restoring NuGet packages..."
dotnet restore SmartlogLogistics.sln

# ── Build solution ────────────────────────────────────────
echo "🔨 Building solution..."
dotnet build SmartlogLogistics.sln -c Release --no-restore

# ── Start infrastructure ──────────────────────────────────
echo ""
echo "🐳 Starting infrastructure (Postgres + Kafka + Nginx)..."
docker-compose up -d postgres zookeeper kafka kafka-ui nginx

echo "⏳ Waiting for PostgreSQL to be ready..."
until docker exec smartlog-postgres pg_isready -U smartlog 2>/dev/null; do
    printf "."
    sleep 2
done
echo ""
echo "✅ PostgreSQL ready"

echo "⏳ Waiting for Kafka to be ready..."
sleep 20
echo "✅ Kafka ready"

# ── Apply EF Migrations ───────────────────────────────────
echo ""
echo "🗄️  Applying database migrations..."

cd OrderService
ConnectionStrings__DefaultConnection="Host=localhost;Port=5432;Database=orderdb;Username=smartlog;Password=smartlog123" \
    dotnet ef database update --no-build
cd ..

cd TrackingService
ConnectionStrings__DefaultConnection="Host=localhost;Port=5432;Database=trackingdb;Username=smartlog;Password=smartlog123" \
    dotnet ef database update --no-build
cd ..

echo "✅ Migrations applied"

# ── Print summary ─────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║   ✅ Setup Complete!                              ║"
echo "╠══════════════════════════════════════════════════╣"
echo "║   Next: Run services in VSCode or terminal:      ║"
echo "║                                                  ║"
echo "║   Terminal 1:                                    ║"
echo "║   cd OrderService && dotnet run                  ║"
echo "║                                                  ║"
echo "║   Terminal 2:                                    ║"
echo "║   cd TrackingService && dotnet run               ║"
echo "║                                                  ║"
echo "╠══════════════════════════════════════════════════╣"
echo "║   URLs:                                          ║"
echo "║   Order API:    http://localhost:5001/swagger    ║"
echo "║   Tracking API: http://localhost:5002/swagger    ║"
echo "║   Kafka UI:     http://localhost:8090            ║"
echo "║   Nginx GW:     http://localhost:80              ║"
echo "╚══════════════════════════════════════════════════╝"
