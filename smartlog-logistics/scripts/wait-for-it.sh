#!/bin/bash
# Wait for a host:port to be available
TIMEOUT=60
HOST=$1
PORT=$2
shift 2

for i in $(seq 1 $TIMEOUT); do
    nc -z $HOST $PORT && echo "✅ $HOST:$PORT is ready" && exec "$@"
    echo "⏳ Waiting for $HOST:$PORT... ($i/$TIMEOUT)"
    sleep 1
done
echo "❌ Timeout waiting for $HOST:$PORT"
exit 1
