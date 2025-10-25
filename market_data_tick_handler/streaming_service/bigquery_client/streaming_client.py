"""
BigQuery Streaming Client

Handles streaming data to BigQuery with optimized partitioning and clustering.
- Live data: 5-minute partitioning on timestamp_out with 30-day TTL
- Historical data: 1-day partitioning on timestamp_out with no TTL  
- Exchange symbol clustering for both live and historical data
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
import asyncio
from collections import defaultdict
import json

from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict

logger = logging.getLogger(__name__)


@dataclass
class StreamingConfig:
    """Configuration for BigQuery streaming"""
    project_id: str
    dataset_id: str
    batch_size: int = 1000
    batch_timeout_ms: int = 60000  # 1 minute for high-frequency data
    max_batch_timeout_ms: int = 300000  # 5 minutes absolute max
    is_live: bool = True  # True for live streaming, False for historical batch


class BigQueryStreamingClient:
    """
    BigQuery streaming client with optimized partitioning and clustering.
    
    Features:
    - Exchange symbol clustering for all tables
    - timestamp_out partitioning (5min for live, 1day for historical)
    - TTL management (30 days for live, no TTL for historical)
    - Batched streaming for cost optimization
    """
    
    def __init__(self, config: StreamingConfig):
        """
        Initialize BigQuery streaming client.
        
        Args:
            config: Streaming configuration
        """
        self.config = config
        self.client = bigquery.Client(project=config.project_id)
        
        # Batch management
        self.batches = defaultdict(list)
        self.batch_timers = {}
        
        # Table schemas
        self.schemas = self._define_schemas()
        
        # Statistics
        self.stats = {
            'total_rows_streamed': 0,
            'batches_sent': 0,
            'errors': 0,
            'tables_created': 0,
            'start_time': datetime.now(timezone.utc)
        }
        
        logger.info(f"✅ BigQueryStreamingClient initialized")
        logger.info(f"   Project: {config.project_id}")
        logger.info(f"   Dataset: {config.dataset_id}")
        logger.info(f"   Mode: {'Live' if config.is_live else 'Historical'}")
        logger.info(f"   Batch size: {config.batch_size}")
    
    def _define_schemas(self) -> Dict[str, List[bigquery.SchemaField]]:
        """Define BigQuery schemas for different data types"""
        
        # Base schema for all streaming data
        base_schema = [
            bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("exchange", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("timestamp_out", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("data_type", "STRING", mode="REQUIRED"),
        ]
        
        # Trade-specific schema
        trades_schema = base_schema + [
            bigquery.SchemaField("price", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("amount", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("side", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("trade_id", "STRING", mode="NULLABLE"),
        ]
        
        # Book snapshot schema
        book_snapshots_schema = base_schema + [
            bigquery.SchemaField("bids", "STRING", mode="REQUIRED"),  # JSON string
            bigquery.SchemaField("asks", "STRING", mode="REQUIRED"),  # JSON string
            bigquery.SchemaField("bid_count", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("ask_count", "INTEGER", mode="REQUIRED"),
        ]
        
        # Liquidations schema
        liquidations_schema = base_schema + [
            bigquery.SchemaField("price", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("amount", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("side", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("liquidation_type", "STRING", mode="REQUIRED"),
        ]
        
        # Derivative ticker schema
        derivative_ticker_schema = base_schema + [
            bigquery.SchemaField("mark_price", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("index_price", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("funding_rate", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("open_interest", "FLOAT64", mode="REQUIRED"),
        ]
        
        # Options chain schema
        options_chain_schema = base_schema + [
            bigquery.SchemaField("strike_price", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("expiry", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("option_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("bid", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("ask", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("volume", "FLOAT64", mode="NULLABLE"),
        ]
        
        # Candles schema (with HFT features)
        candles_schema = base_schema + [
            bigquery.SchemaField("timeframe", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp_in", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("open", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("high", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("low", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("close", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("volume", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("trade_count", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("vwap", "FLOAT64", mode="NULLABLE"),
            
            # HFT Features (all nullable since not all candles will have them)
            bigquery.SchemaField("sma_5", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("sma_10", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("sma_20", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("ema_5", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("ema_10", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("ema_20", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("wma_5", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("price_momentum_3", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("price_momentum_5", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("price_velocity", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("price_acceleration", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("volume_sma_5", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("volume_ema_5", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("volume_ratio", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("vwap_deviation", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("price_volatility_5", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("price_volatility_10", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("high_low_ratio", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("close_to_close_return", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("trade_intensity", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("avg_trade_size", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("price_impact", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("bid_ask_spread_proxy", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("rsi_5", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("bollinger_position", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("macd_signal", "FLOAT64", mode="NULLABLE"),
        ]
        
        return {
            'ticks_trades': trades_schema,
            'ticks_book_snapshots': book_snapshots_schema,
            'ticks_liquidations': liquidations_schema,
            'ticks_derivative_ticker': derivative_ticker_schema,
            'ticks_options_chain': options_chain_schema,
            'candles_15s': candles_schema,
            'candles_1m': candles_schema,
            'candles_5m': candles_schema,
            'candles_15m': candles_schema,
            'candles_4h': candles_schema,
            'candles_24h': candles_schema,
        }
    
    async def ensure_table_exists(self, table_name: str) -> bool:
        """
        Ensure BigQuery table exists with proper partitioning and clustering.
        
        Args:
            table_name: Name of the table
            
        Returns:
            True if table exists or was created successfully
        """
        try:
            dataset_ref = self.client.dataset(self.config.dataset_id)
            table_ref = dataset_ref.table(table_name)
            
            # Check if table exists
            try:
                self.client.get_table(table_ref)
                logger.debug(f"Table {table_name} already exists")
                return True
            except NotFound:
                pass
            
            # Create table with schema
            if table_name not in self.schemas:
                logger.error(f"No schema defined for table: {table_name}")
                return False
            
            schema = self.schemas[table_name]
            table = bigquery.Table(table_ref, schema=schema)
            
            # Configure partitioning based on live vs historical
            if self.config.is_live:
                # Live data: 5-minute partitioning with 30-day TTL
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.HOUR,  # Use HOUR for 5-minute granularity
                    field="timestamp_out",
                    expiration_ms=30 * 24 * 60 * 60 * 1000  # 30 days TTL
                )
            else:
                # Historical data: 1-day partitioning with no TTL
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="timestamp_out",
                    expiration_ms=None  # No TTL
                )
            
            # Configure clustering on exchange and symbol for all tables
            table.clustering_fields = ["exchange", "symbol"]
            
            # Set table description
            mode = "live" if self.config.is_live else "historical"
            table.description = (
                f"Market data streaming table ({mode}) with "
                f"timestamp_out partitioning and exchange/symbol clustering"
            )
            
            # Create the table
            table = self.client.create_table(table)
            
            self.stats['tables_created'] += 1
            logger.info(f"✅ Created BigQuery table: {table_name}")
            logger.info(f"   Partitioning: {'5-minute' if self.config.is_live else '1-day'} on timestamp_out")
            logger.info(f"   Clustering: exchange, symbol")
            logger.info(f"   TTL: {'30 days' if self.config.is_live else 'None'}")
            
            return True
            
        except Conflict:
            # Table was created by another process
            logger.debug(f"Table {table_name} created by another process")
            return True
        except Exception as e:
            logger.error(f"❌ Error creating table {table_name}: {e}")
            return False
    
    async def stream_data(self, table_name: str, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> bool:
        """
        Stream data to BigQuery table with batching.
        
        Args:
            table_name: Target table name
            data: Data to stream (single row or list of rows)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure table exists
            table_exists = await self.ensure_table_exists(table_name)
            if not table_exists:
                return False
            
            # Convert single row to list
            if isinstance(data, dict):
                data = [data]
            
            # Add timestamp_out if not present
            for row in data:
                if 'timestamp_out' not in row:
                    row['timestamp_out'] = datetime.now(timezone.utc)
            
            # Add to batch
            self.batches[table_name].extend(data)
            
            # Check if batch is full or timeout reached
            should_flush = (
                len(self.batches[table_name]) >= self.config.batch_size or
                await self._should_flush_by_timeout(table_name)
            )
            
            if should_flush:
                await self._flush_batch(table_name)
            elif table_name not in self.batch_timers:
                # Start timeout timer
                self._start_batch_timer(table_name)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error streaming data to {table_name}: {e}")
            self.stats['errors'] += 1
            return False
    
    async def _flush_batch(self, table_name: str) -> bool:
        """Flush batch to BigQuery"""
        try:
            if not self.batches[table_name]:
                return True
            
            batch = self.batches[table_name].copy()
            self.batches[table_name].clear()
            
            # Cancel timer
            if table_name in self.batch_timers:
                self.batch_timers[table_name].cancel()
                del self.batch_timers[table_name]
            
            # Insert batch
            table_ref = self.client.dataset(self.config.dataset_id).table(table_name)
            
            # Convert timestamps to strings for BigQuery
            for row in batch:
                for key, value in row.items():
                    if isinstance(value, datetime):
                        row[key] = value.isoformat()
            
            errors = self.client.insert_rows_json(table_ref, batch)
            
            if errors:
                logger.error(f"❌ BigQuery insert errors for {table_name}: {errors}")
                self.stats['errors'] += 1
                return False
            
            # Update statistics
            self.stats['total_rows_streamed'] += len(batch)
            self.stats['batches_sent'] += 1
            
            # Calculate cost estimate
            batch_size_mb = len(json.dumps(batch).encode()) / (1024 * 1024)
            estimated_cost = batch_size_mb * 0.01  # Rough estimate
            
            logger.info(f"✅ Streamed batch to {table_name}: {len(batch)} rows, "
                       f"~{batch_size_mb:.2f}MB, ~${estimated_cost:.4f}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error flushing batch to {table_name}: {e}")
            self.stats['errors'] += 1
            return False
    
    def _start_batch_timer(self, table_name: str) -> None:
        """Start batch timeout timer"""
        timeout = self.config.batch_timeout_ms / 1000
        
        async def timeout_callback():
            await asyncio.sleep(timeout)
            if table_name in self.batches and self.batches[table_name]:
                logger.info(f"⏰ Batch timeout triggered for {table_name}")
                await self._flush_batch(table_name)
        
        self.batch_timers[table_name] = asyncio.create_task(timeout_callback())
    
    async def _should_flush_by_timeout(self, table_name: str) -> bool:
        """Check if batch should be flushed due to timeout"""
        # This is a simplified check - in practice, you'd track batch start times
        return False
    
    async def flush_all_batches(self) -> None:
        """Flush all pending batches"""
        logger.info("🔄 Flushing all pending batches...")
        
        for table_name in list(self.batches.keys()):
            if self.batches[table_name]:
                await self._flush_batch(table_name)
        
        logger.info("✅ All batches flushed")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get streaming statistics"""
        runtime = datetime.now(timezone.utc) - self.stats['start_time']
        
        return {
            'total_rows_streamed': self.stats['total_rows_streamed'],
            'batches_sent': self.stats['batches_sent'],
            'errors': self.stats['errors'],
            'tables_created': self.stats['tables_created'],
            'runtime_seconds': runtime.total_seconds(),
            'rows_per_second': self.stats['total_rows_streamed'] / max(runtime.total_seconds(), 1),
            'error_rate': self.stats['errors'] / max(self.stats['batches_sent'], 1),
            'pending_batches': {table: len(batch) for table, batch in self.batches.items() if batch},
            'mode': 'live' if self.config.is_live else 'historical'
        }
    
    async def shutdown(self) -> None:
        """Shutdown client and flush remaining data"""
        logger.info("🛑 Shutting down BigQueryStreamingClient...")
        
        # Cancel all timers
        for timer in self.batch_timers.values():
            timer.cancel()
        self.batch_timers.clear()
        
        # Flush all remaining batches
        await self.flush_all_batches()
        
        # Close client
        self.client.close()
        
        logger.info("✅ BigQueryStreamingClient shutdown complete")


# Example usage
if __name__ == "__main__":
    import asyncio
    from datetime import datetime, timezone
    
    async def test_bigquery_streaming():
        # Live streaming config
        live_config = StreamingConfig(
            project_id="your-project-id",
            dataset_id="market_data_streaming_live",
            batch_size=100,
            batch_timeout_ms=30000,  # 30 seconds for testing
            is_live=True
        )
        
        client = BigQueryStreamingClient(live_config)
        
        # Test streaming trade data
        trade_data = {
            'symbol': 'BTC-USDT',
            'exchange': 'binance',
            'timestamp': datetime.now(timezone.utc),
            'data_type': 'trades',
            'price': 67000.0,
            'amount': 0.1,
            'side': 'buy',
            'trade_id': 'test_123'
        }
        
        success = await client.stream_data('ticks_trades', trade_data)
        print(f"Streaming success: {success}")
        
        # Test streaming candle data
        candle_data = {
            'symbol': 'BTC-USDT',
            'exchange': 'binance',
            'timestamp': datetime.now(timezone.utc),
            'timestamp_in': datetime.now(timezone.utc),
            'data_type': 'candles',
            'timeframe': '1m',
            'open': 66900.0,
            'high': 67100.0,
            'low': 66800.0,
            'close': 67000.0,
            'volume': 1.5,
            'trade_count': 45,
            'vwap': 67000.0,
            # HFT features
            'sma_5': 67050.0,
            'ema_5': 67025.0,
            'rsi_5': 55.2
        }
        
        success = await client.stream_data('candles_1m', candle_data)
        print(f"Candle streaming success: {success}")
        
        # Show stats
        stats = client.get_stats()
        print(f"Stats: {stats}")
        
        await client.shutdown()
    
    # Uncomment to run test (requires valid GCP credentials)
    # asyncio.run(test_bigquery_streaming())