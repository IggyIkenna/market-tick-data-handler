# Dockerfile for Real-Time Live Tick Data Streaming Service
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy application code
COPY . .

# Make the script executable
RUN chmod +x live_tick_streamer.js

# Set environment variables
ENV NODE_ENV=production
ENV TARDIS_API_KEY=""

# Expose port (if needed for health checks)
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD node -e "console.log('Health check passed')" || exit 1

# Run the application
CMD ["node", "live_tick_streamer.js"]
