#!/bin/bash
set -e

echo "🐳 Starting full stack with Docker..."

# Build and start everything
docker-compose up -d --build

echo "⏳ Waiting for services..."
sleep 40

# Check health
echo ""
echo "Checking service health..."
curl -sf http://localhost/health/orders && echo "✅ OrderService OK" || echo "❌ OrderService not ready"
curl -sf http://localhost/health/tracking && echo "✅ TrackingService OK" || echo "❌ TrackingService not ready"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║   🚀 All services running!                       ║"
echo "╠══════════════════════════════════════════════════╣"
echo "║   Gateway:      http://localhost                 ║"
echo "║   Order API:    http://localhost:5001/swagger    ║"
echo "║   Tracking API: http://localhost:5002/swagger    ║"
echo "║   Kafka UI:     http://localhost:8090            ║"
echo "╚══════════════════════════════════════════════════╝"
