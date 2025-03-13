#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

# Stop and remove containers
echo "🚀 Stopping and removing containers..."
docker-compose down

# Start containers in detached mode
echo "🚀 Starting containers..."
docker-compose up -d

# Wait for LocalStack to be ready
echo "🕒 Waiting for LocalStack to be ready..."
until curl -s http://localhost:4566/_localstack/health | jq -e '.services.s3 == "available"' > /dev/null; do
    echo "⏳ Waiting for S3 service in LocalStack..."
    sleep 3
done
echo "✅ LocalStack S3 is ready!"

# Prune unused images
echo "🗑️ Pruning unused Docker images..."
docker image prune -f

# Create S3 buckets in LocalStack
echo "🛠️ Creating S3 buckets..."
aws --endpoint-url=http://localhost:4566 s3 mb s3://raw
aws --endpoint-url=http://localhost:4566 s3 mb s3://staging
aws --endpoint-url=http://localhost:4566 s3 mb s3://curated
aws --endpoint-url=http://localhost:4566 s3 mb s3://cache

echo "✅ Deployment complete!"