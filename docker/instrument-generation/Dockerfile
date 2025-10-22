# Multi-stage Dockerfile for Instrument Generation
# Stage 1: Build dependencies
FROM python:3.11-slim as builder

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy requirements first for dependency caching
COPY requirements.txt .

# Install Python dependencies with optimizations
RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Stage 2: Runtime image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# Install only runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy Python packages from builder stage
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy service account key (separate layer for better caching)
COPY central-element-323112-e35fb0ddafe2.json /app/
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/central-element-323112-e35fb0ddafe2.json

# Copy application code (this layer will rebuild on code changes)
COPY . .

# Create necessary directories
RUN mkdir -p /app/logs /app/data /app/temp

# Create non-root user for security
RUN useradd -m -s /bin/bash tickdata && \
    chown -R tickdata:tickdata /app

# Switch to non-root user
USER tickdata

# Default command - run instrument generation using main.py
CMD ["python", "-m", "src.main", "--mode", "instruments", "--start-date", "2023-05-23", "--end-date", "2023-05-25"]
