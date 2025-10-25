#!/usr/bin/env python3
"""
Test Integration between Node.js and Python

Tests the WebSocket communication and data processing pipeline.
"""

import asyncio
import json
import websockets
import logging
from datetime import datetime, timezone
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from market_data_tick_handler.streaming_service.node_ingestion.python_websocket_server import PythonWebSocketServer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_websocket_connection():
    """Test WebSocket connection to Python server"""
    try:
        uri = "ws://localhost:8765"
        async with websockets.connect(uri) as websocket:
            logger.info("✅ WebSocket connection successful")
            
            # Send test message
            test_message = {
                "type": "tardis_message",
                "data": {
                    "type": "trade",
                    "symbol": "btcusdt",
                    "exchange": "binance",
                    "price": 50000.0,
                    "amount": 0.001,
                    "side": "buy",
                    "timestamp": datetime.utcnow().isoformat(),
                    "id": "test_trade_1"
                },
                "timestamp": datetime.utcnow().isoformat(),
                "source": "test"
            }
            
            await websocket.send(json.dumps(test_message))
            logger.info("✅ Test message sent")
            
            # Wait for response (if any)
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                logger.info(f"✅ Received response: {response}")
            except asyncio.TimeoutError:
                logger.info("ℹ️ No response received (expected for one-way communication)")
            
            return True
            
    except Exception as e:
        logger.error(f"❌ WebSocket connection failed: {e}")
        return False

async def test_python_server():
    """Test Python WebSocket server functionality"""
    logger.info("🧪 Testing Python WebSocket server...")
    
    # Create server instance
    server = PythonWebSocketServer(
        host="localhost",
        port=8765,
        symbol="BTC-USDT",
        exchange="binance",
        timeframes=['15s', '1m'],
        enable_serve_mode=True,
        enable_persist_mode=False  # Disable for testing
    )
    
    # Start server in background
    server_task = asyncio.create_task(server.start())
    
    # Wait for server to start
    await asyncio.sleep(2)
    
    try:
        # Test WebSocket connection
        success = await test_websocket_connection()
        
        if success:
            logger.info("✅ Python server test passed")
        else:
            logger.error("❌ Python server test failed")
            
        return success
        
    finally:
        # Stop server
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass

async def test_data_processing():
    """Test data processing pipeline"""
    logger.info("🧪 Testing data processing pipeline...")
    
    # Create server instance
    server = PythonWebSocketServer(
        host="localhost",
        port=8766,  # Different port to avoid conflicts
        symbol="BTC-USDT",
        exchange="binance",
        timeframes=['1m'],
        enable_serve_mode=True,
        enable_persist_mode=False
    )
    
    # Start server in background
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(2)
    
    try:
        # Connect and send test data
        uri = "ws://localhost:8766"
        async with websockets.connect(uri) as websocket:
            
            # Send multiple test trades
            for i in range(5):
                test_message = {
                    "type": "tardis_message",
                    "data": {
                        "type": "trade",
                        "symbol": "btcusdt",
                        "exchange": "binance",
                        "price": 50000.0 + i * 10,  # Varying prices
                        "amount": 0.001,
                        "side": "buy" if i % 2 == 0 else "sell",
                        "timestamp": datetime.utcnow().isoformat(),
                        "id": f"test_trade_{i}"
                    },
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": "test"
                }
                
                await websocket.send(json.dumps(test_message))
                await asyncio.sleep(0.1)  # Small delay between messages
            
            logger.info("✅ Test data sent")
            
            # Check server stats
            stats = server.get_stats()
            logger.info(f"📊 Server stats: {stats}")
            
            if stats['total_messages'] > 0:
                logger.info("✅ Data processing test passed")
                return True
            else:
                logger.error("❌ No messages processed")
                return False
                
    except Exception as e:
        logger.error(f"❌ Data processing test failed: {e}")
        return False
    finally:
        # Stop server
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass

async def main():
    """Run all tests"""
    logger.info("🚀 Starting integration tests...")
    
    tests = [
        ("Python WebSocket Server", test_python_server),
        ("Data Processing Pipeline", test_data_processing)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        logger.info(f"\n🧪 Running {test_name} test...")
        try:
            result = await test_func()
            results.append((test_name, result))
            if result:
                logger.info(f"✅ {test_name} test passed")
            else:
                logger.error(f"❌ {test_name} test failed")
        except Exception as e:
            logger.error(f"❌ {test_name} test error: {e}")
            results.append((test_name, False))
    
    # Summary
    logger.info("\n📊 Test Results Summary:")
    logger.info("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{test_name}: {status}")
        if result:
            passed += 1
    
    logger.info("=" * 50)
    logger.info(f"Total: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed!")
        return True
    else:
        logger.error("❌ Some tests failed")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
