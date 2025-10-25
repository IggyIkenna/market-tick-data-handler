# Multi-stage Dockerfile for Market Data Streaming Service
# Supports both Python (candle processing) and Node.js (real-time streaming)

FROM node:18-alpine AS nodejs-base

# Install Node.js dependencies
WORKDIR /app/nodejs
COPY live_streaming/nodejs/package*.json ./
RUN npm ci --only=production

# Copy Node.js streaming service
COPY live_streaming/nodejs/ ./

# Python stage
FROM python:3.9-slim AS python-base

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js for real-time streaming
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs

# Set working directory
WORKDIR /app

# Copy Python requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Node.js artifacts from previous stage
COPY --from=nodejs-base /app/nodejs /app/nodejs

# Copy Python application code
COPY src/ ./src/
COPY config.py ./
COPY *.json ./

# Copy streaming service entry points
COPY docker/streaming-service/entrypoint.sh ./
COPY docker/streaming-service/streaming-service.py ./

# Make scripts executable
RUN chmod +x entrypoint.sh

# Create directories for logs and data
RUN mkdir -p /app/logs /app/data

# Set environment variables
ENV PYTHONPATH=/app
ENV NODE_ENV=production
ENV STREAMING_MODE=candles
ENV STREAMING_SYMBOL=BTC-USDT
ENV STREAMING_DURATION=3600

# Expose ports
EXPOSE 8080 8081

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Use entrypoint script
ENTRYPOINT ["./entrypoint.sh"]
CMD ["python", "streaming-service.py"]
