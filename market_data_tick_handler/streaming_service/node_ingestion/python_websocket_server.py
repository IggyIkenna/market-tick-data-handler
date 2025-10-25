#!/usr/bin/env python3
"""
Python WebSocket Server for Node.js Integration

Receives live data from Node.js Tardis.dev streamer and processes it
using the existing Python processing pipeline.
"""

import asyncio
import json
import logging
import websockets
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from market_data_tick_handler.streaming_service.tick_processor.tick_handler import TickHandler
from market_data_tick_handler.streaming_service.candle_processor.live_candle_processor import LiveCandleProcessor
from market_data_tick_handler.streaming_service.hft_features.feature_calculator import HFTFeatureCalculator
from market_data_tick_handler.streaming_service.modes.serve_mode import ServeMode, ServeConfig
from market_data_tick_handler.streaming_service.modes.persist_mode import PersistMode, PersistConfig
from market_data_tick_handler.utils.logger import setup_structured_logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PythonWebSocketServer:
    """
    WebSocket server that receives data from Node.js and processes it
    using the existing Python streaming service components.
    """
    
    def __init__(self, 
                 host: str = "localhost", 
                 port: int = 8765,
                 symbol: str = "BTC-USDT",
                 exchange: str = "binance",
                 timeframes: list = None,
                 enable_serve_mode: bool = True,
                 enable_persist_mode: bool = True):
        
        self.host = host
        self.port = port
        self.symbol = symbol
        self.exchange = exchange
        self.timeframes = timeframes or ['15s', '1m', '5m', '15m']
        
        # Initialize processing components
        self.tick_handler = TickHandler()
        self.candle_processor = LiveCandleProcessor(symbol=symbol)
        self.hft_calculator = HFTFeatureCalculator()
        
        # Initialize modes
        self.serve_mode = None
        self.persist_mode = None
        
        if enable_serve_mode:
            serve_config = ServeConfig(transport="inmemory")
            self.serve_mode = ServeMode(serve_config)
            
        if enable_persist_mode:
            persist_config = PersistConfig(
                project_id="central-element-323112",
                dataset_id="market_data_streaming"
            )
            self.persist_mode = PersistMode(persist_config)
        
        # Statistics
        self.stats = {
            'total_messages': 0,
            'total_trades': 0,
            'total_candles': 0,
            'start_time': None,
            'last_message_time': None
        }
        
        logger.info(f"🚀 Python WebSocket Server initialized for {symbol} on {exchange}")
    
    async def start(self):
        """Start the WebSocket server"""
        logger.info(f"🔄 Starting WebSocket server on {self.host}:{self.port}")
        
        # Start modes
        if self.serve_mode:
            await self.serve_mode.start()
            logger.info("✅ Serve mode started")
            
        if self.persist_mode:
            await self.persist_mode.start()
            logger.info("✅ Persist mode started")
        
        self.stats['start_time'] = datetime.utcnow()
        
        # Start WebSocket server
        async with websockets.serve(self.handle_client, self.host, self.port):
            logger.info(f"✅ WebSocket server listening on {self.host}:{self.port}")
            await asyncio.Future()  # Run forever
    
    async def stop(self):
        """Stop the WebSocket server and modes"""
        logger.info("🛑 Stopping Python WebSocket server...")
        
        if self.serve_mode:
            await self.serve_mode.stop()
            
        if self.persist_mode:
            await self.persist_mode.stop()
        
        logger.info("✅ Python WebSocket server stopped")
    
    async def handle_client(self, websocket, path):
        """Handle incoming WebSocket connections"""
        client_address = websocket.remote_address
        logger.info(f"🔌 New client connected: {client_address}")
        
        try:
            async for message in websocket:
                await self.process_message(message)
                
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"🔌 Client disconnected: {client_address}")
        except Exception as e:
            logger.error(f"❌ Error handling client {client_address}: {e}")
    
    async def process_message(self, message: str):
        """Process incoming message from Node.js"""
        try:
            data = json.loads(message)
            
            if data.get('type') == 'tardis_message':
                await self.process_tardis_message(data['data'])
            else:
                logger.warning(f"Unknown message type: {data.get('type')}")
                
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode error: {e}")
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
    
    async def process_tardis_message(self, message: Dict[str, Any]):
        """Process Tardis.dev message"""
        self.stats['total_messages'] += 1
        self.stats['last_message_time'] = datetime.utcnow()
        
        # Log progress
        if self.stats['total_messages'] % 1000 == 0:
            logger.info(f"📊 Processed {self.stats['total_messages']} messages")
        
        # Process based on message type
        if message.get('type') == 'trade':
            await self.process_trade(message)
        elif message.get('type') == 'book_change':
            await self.process_book_change(message)
        elif message.get('type') == 'derivative_ticker':
            await self.process_derivative_ticker(message)
        else:
            logger.debug(f"Unhandled message type: {message.get('type')}")
    
    async def process_trade(self, trade_message: Dict[str, Any]):
        """Process trade message"""
        self.stats['total_trades'] += 1
        
        try:
            # Convert to internal format
            tick_data = {
                'symbol': trade_message.get('symbol'),
                'exchange': trade_message.get('exchange'),
                'price': trade_message.get('price'),
                'amount': trade_message.get('amount'),
                'side': trade_message.get('side'),
                'timestamp': trade_message.get('timestamp'),
                'trade_id': trade_message.get('id')
            }
            
            # Process with tick handler
            processed_tick = await self.tick_handler.process_tick(tick_data)
            
            if processed_tick:
                # Process with candle processor
                candles = await self.candle_processor.process_tick(processed_tick)
                
                for candle in candles:
                    # Add HFT features
                    candle_with_features = await self.hft_calculator.calculate_features(candle)
                    
                    # Send to modes
                    if self.serve_mode:
                        await self.serve_mode.publish_candle(candle_with_features)
                    
                    if self.persist_mode:
                        await self.persist_mode.persist_candle(candle_with_features)
                    
                    self.stats['total_candles'] += 1
                    
        except Exception as e:
            logger.error(f"❌ Error processing trade: {e}")
    
    async def process_book_change(self, book_message: Dict[str, Any]):
        """Process book change message"""
        try:
            # Process book snapshot
            book_data = {
                'symbol': book_message.get('symbol'),
                'exchange': book_message.get('exchange'),
                'timestamp': book_message.get('timestamp'),
                'bids': book_message.get('bids', []),
                'asks': book_message.get('asks', [])
            }
            
            # Process with tick handler
            processed_book = await self.tick_handler.process_book_snapshot(book_data)
            
            if processed_book and self.serve_mode:
                await self.serve_mode.publish_book_snapshot(processed_book)
                
        except Exception as e:
            logger.error(f"❌ Error processing book change: {e}")
    
    async def process_derivative_ticker(self, ticker_message: Dict[str, Any]):
        """Process derivative ticker message"""
        try:
            ticker_data = {
                'symbol': ticker_message.get('symbol'),
                'exchange': ticker_message.get('exchange'),
                'timestamp': ticker_message.get('timestamp'),
                'last_price': ticker_message.get('lastPrice'),
                'funding_rate': ticker_message.get('fundingRate'),
                'open_interest': ticker_message.get('openInterest'),
                'mark_price': ticker_message.get('markPrice')
            }
            
            if self.serve_mode:
                await self.serve_mode.publish_derivative_ticker(ticker_data)
                
        except Exception as e:
            logger.error(f"❌ Error processing derivative ticker: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics"""
        uptime = 0
        if self.stats['start_time']:
            uptime = (datetime.utcnow() - self.stats['start_time']).total_seconds()
        
        return {
            **self.stats,
            'uptime_seconds': uptime,
            'messages_per_second': self.stats['total_messages'] / uptime if uptime > 0 else 0
        }

async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Python WebSocket Server for Node.js Integration')
    parser.add_argument('--host', default='localhost', help='WebSocket host')
    parser.add_argument('--port', type=int, default=8765, help='WebSocket port')
    parser.add_argument('--symbol', default='BTC-USDT', help='Trading symbol')
    parser.add_argument('--exchange', default='binance', help='Exchange name')
    parser.add_argument('--timeframes', nargs='+', default=['15s', '1m', '5m', '15m'], help='Timeframes')
    parser.add_argument('--no-serve', action='store_true', help='Disable serve mode')
    parser.add_argument('--no-persist', action='store_true', help='Disable persist mode')
    
    args = parser.parse_args()
    
    # Create server
    server = PythonWebSocketServer(
        host=args.host,
        port=args.port,
        symbol=args.symbol,
        exchange=args.exchange,
        timeframes=args.timeframes,
        enable_serve_mode=not args.no_serve,
        enable_persist_mode=not args.no_persist
    )
    
    try:
        await server.start()
    except KeyboardInterrupt:
        logger.info("🛑 Received keyboard interrupt")
    finally:
        await server.stop()

if __name__ == "__main__":
    asyncio.run(main())
