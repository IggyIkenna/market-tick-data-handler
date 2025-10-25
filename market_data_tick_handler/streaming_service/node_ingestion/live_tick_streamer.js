#!/usr/bin/env node
/**
 * Real-Time Live Market Data Streaming Service
 * 
 * Node.js ingestion layer using Tardis.dev for live streaming
 * Integrates with Python processing layer via WebSocket/HTTP
 * 
 * Based on: https://docs.tardis.dev/api/node-js#streaming-real-time-market-data
 */

const { streamNormalized, normalizeTrades, normalizeBookChanges, normalizeDerivativeTickers } = require('tardis-dev');
const { BigQuery } = require('@google-cloud/bigquery');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

class LiveTickStreamer {
    constructor(options = {}) {
        this.symbol = options.symbol || 'BTC-USDT';
        this.exchange = options.exchange || 'binance';
        this.mode = options.mode || 'candles'; // 'ticks' or 'candles'
        this.dataTypes = options.dataTypes || ['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations'];
        this.timeframes = options.timeframes || ['15s', '1m', '5m', '15m'];
        this.tardisSymbol = this.symbol.toLowerCase().replace('-', ''); // BTC-USDT -> btcusdt
        this.running = false;
        this.stats = {
            totalMessages: 0,
            totalTrades: 0,
            totalCandles: 0,
            startTime: null,
            lastMessageTime: null
        };
        
        // Python processing integration
        this.pythonWebSocketUrl = options.pythonWebSocketUrl || 'ws://localhost:8765';
        this.pythonWebSocket = null;
        this.pythonConnected = false;
        
        // BigQuery for direct persistence (if needed)
        this.bigquery = null;
        this.bigqueryEnabled = options.bigqueryEnabled || false;
        
        if (this.bigqueryEnabled) {
            this.bigquery = new BigQuery({
                projectId: process.env.GCP_PROJECT_ID || 'central-element-323112',
                keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS
            });
            this.datasetId = process.env.BIGQUERY_DATASET || 'market_data_streaming';
        }
        
        console.log('🚀 LiveTickStreamer initialized:', {
            symbol: this.symbol,
            exchange: this.exchange,
            mode: this.mode,
            dataTypes: this.dataTypes,
            timeframes: this.timeframes,
            pythonWebSocketUrl: this.pythonWebSocketUrl,
            bigqueryEnabled: this.bigqueryEnabled
        });
    }

    async start() {
        console.log('🔄 Starting live tick streamer...');
        this.running = true;
        this.stats.startTime = new Date();
        
        try {
            // Connect to Python processing layer
            await this.connectToPython();
            
            // Start Tardis.dev streaming
            await this.startTardisStreaming();
            
        } catch (error) {
            console.error('❌ Error starting streamer:', error);
            await this.stop();
            throw error;
        }
    }

    async stop() {
        console.log('🛑 Stopping live tick streamer...');
        this.running = false;
        
        if (this.pythonWebSocket) {
            this.pythonWebSocket.close();
        }
        
        console.log('📊 Final stats:', this.stats);
    }

    async connectToPython() {
        return new Promise((resolve, reject) => {
            console.log(`🔌 Connecting to Python processing layer at ${this.pythonWebSocketUrl}...`);
            
            this.pythonWebSocket = new WebSocket(this.pythonWebSocketUrl);
            
            this.pythonWebSocket.on('open', () => {
                console.log('✅ Connected to Python processing layer');
                this.pythonConnected = true;
                resolve();
            });
            
            this.pythonWebSocket.on('error', (error) => {
                console.error('❌ Python WebSocket connection error:', error);
                this.pythonConnected = false;
                reject(error);
            });
            
            this.pythonWebSocket.on('close', () => {
                console.log('🔌 Disconnected from Python processing layer');
                this.pythonConnected = false;
            });
        });
    }

    async startTardisStreaming() {
        console.log('📡 Starting Tardis.dev streaming...');
        
        try {
            // Create normalized stream based on data types
            const streamOptions = {
                exchange: this.exchange,
                symbols: [this.tardisSymbol]
            };
            
            // Choose normalizers based on data types
            const normalizers = [];
            if (this.dataTypes.includes('trades')) {
                normalizers.push(normalizeTrades);
            }
            if (this.dataTypes.includes('book_snapshot_5') || this.dataTypes.includes('book_snapshot_25')) {
                normalizers.push(normalizeBookChanges);
            }
            if (this.dataTypes.includes('derivative_ticker')) {
                normalizers.push(normalizeDerivativeTickers);
            }
            
            console.log('🔧 Stream configuration:', {
                exchange: this.exchange,
                symbols: [this.tardisSymbol],
                normalizers: normalizers.map(n => n.name)
            });
            
            // Create stream
            const messages = streamNormalized(streamOptions, ...normalizers);
            
            // Process messages
            for await (const message of messages) {
                if (!this.running) {
                    break;
                }
                
                await this.processMessage(message);
            }
            
        } catch (error) {
            console.error('❌ Tardis streaming error:', error);
            throw error;
        }
    }

    async processMessage(message) {
        this.stats.totalMessages++;
        this.stats.lastMessageTime = new Date();
        
        // Log message type for debugging
        if (this.stats.totalMessages % 1000 === 0) {
            console.log(`📊 Processed ${this.stats.totalMessages} messages, last type: ${message.type}`);
        }
        
        // Send to Python processing layer
        if (this.pythonConnected && this.pythonWebSocket.readyState === WebSocket.OPEN) {
            try {
                const messageData = {
                    type: 'tardis_message',
                    data: message,
                    timestamp: new Date().toISOString(),
                    source: 'node_ingestion'
                };
                
                this.pythonWebSocket.send(JSON.stringify(messageData));
                
            } catch (error) {
                console.error('❌ Error sending to Python:', error);
                this.pythonConnected = false;
            }
        }
        
        // Direct BigQuery persistence (if enabled)
        if (this.bigqueryEnabled && this.bigquery) {
            await this.persistToBigQuery(message);
        }
        
        // Update stats based on message type
        if (message.type === 'trade') {
            this.stats.totalTrades++;
        }
    }

    async persistToBigQuery(message) {
        try {
            const tableId = `${message.type}_${this.exchange}_${this.symbol.toLowerCase().replace('-', '_')}`;
            
            // Prepare data for BigQuery
            const row = {
                symbol: message.symbol,
                exchange: message.exchange,
                type: message.type,
                timestamp: message.timestamp,
                local_timestamp: new Date().toISOString(),
                data: JSON.stringify(message)
            };
            
            // Insert into BigQuery
            await this.bigquery
                .dataset(this.datasetId)
                .table(tableId)
                .insert([row]);
                
        } catch (error) {
            console.error('❌ BigQuery persistence error:', error);
        }
    }

    getStats() {
        return {
            ...this.stats,
            uptime: this.stats.startTime ? Date.now() - this.stats.startTime.getTime() : 0,
            pythonConnected: this.pythonConnected,
            running: this.running
        };
    }
}

// CLI interface
if (require.main === module) {
    const args = process.argv.slice(2);
    const options = {};
    
    // Parse command line arguments
    for (let i = 0; i < args.length; i += 2) {
        const key = args[i].replace('--', '');
        const value = args[i + 1];
        
        if (key === 'dataTypes') {
            options[key] = value.split(',');
        } else if (key === 'timeframes') {
            options[key] = value.split(',');
        } else if (key === 'bigqueryEnabled') {
            options[key] = value === 'true';
        } else {
            options[key] = value;
        }
    }
    
    const streamer = new LiveTickStreamer(options);
    
    // Handle graceful shutdown
    process.on('SIGINT', async () => {
        console.log('\n🛑 Received SIGINT, shutting down gracefully...');
        await streamer.stop();
        process.exit(0);
    });
    
    process.on('SIGTERM', async () => {
        console.log('\n🛑 Received SIGTERM, shutting down gracefully...');
        await streamer.stop();
        process.exit(0);
    });
    
    // Start streaming
    streamer.start().catch(error => {
        console.error('❌ Fatal error:', error);
        process.exit(1);
    });
}

module.exports = LiveTickStreamer;
