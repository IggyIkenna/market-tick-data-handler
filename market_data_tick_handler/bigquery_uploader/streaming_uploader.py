"""
Streaming BigQuery Uploader

Handles real-time uploads for streaming ticks and candles with:
- 1-minute queued batches to optimize BigQuery costs
- Hourly partitioning on timestamp_out for 5-minute granularity
- 30-day TTL for live data
- Exchange/symbol clustering for optimal query performance
"""

import logging
import pandas as pd
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from collections import defaultdict, deque
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import threading
import time

logger = logging.getLogger(__name__)

class StreamingBigQueryUploader:
    """Handles streaming uploads to BigQuery with 1-minute batching for cost optimization"""
    
    def __init__(self, project_id: str, dataset_id: str, batch_interval_seconds: int = 60):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.batch_interval_seconds = batch_interval_seconds
        self.bq_client = bigquery.Client(project=project_id)
        
        # Batch queues for different data types
        self.candle_batches = defaultdict(deque)  # timeframe -> deque of DataFrames
        self.tick_batches = defaultdict(deque)    # data_type -> deque of DataFrames
        
        # Batch metadata
        self.last_flush_time = defaultdict(lambda: datetime.now(timezone.utc))
        self.batch_stats = defaultdict(lambda: {'rows': 0, 'batches': 0})
        
        # Background flushing
        self._flush_lock = threading.Lock()
        self._flush_task = None
        self._stop_flushing = False
        
        # Start background batch flusher
        self._start_batch_flusher()
        
    def _start_batch_flusher(self):
        """Start background thread to flush batches every minute"""
        def flush_worker():
            while not self._stop_flushing:
                try:
                    time.sleep(1)  # Check every second
                    current_time = datetime.now(timezone.utc)
                    
                    with self._flush_lock:
                        # Check candle batches
                        for timeframe in list(self.candle_batches.keys()):
                            if (current_time - self.last_flush_time[f"candles_{timeframe}"]).total_seconds() >= self.batch_interval_seconds:
                                # Schedule flush in a thread-safe way
                                try:
                                    loop = asyncio.get_running_loop()
                                    loop.create_task(self._flush_candle_batch(timeframe))
                                except RuntimeError:
                                    # No running loop, skip this flush
                                    pass
                        
                        # Check tick batches
                        for data_type in list(self.tick_batches.keys()):
                            if (current_time - self.last_flush_time[f"ticks_{data_type}"]).total_seconds() >= self.batch_interval_seconds:
                                # Schedule flush in a thread-safe way
                                try:
                                    loop = asyncio.get_running_loop()
                                    loop.create_task(self._flush_tick_batch(data_type))
                                except RuntimeError:
                                    # No running loop, skip this flush
                                    pass
                                
                except Exception as e:
                    logger.error(f"Error in batch flusher: {e}")
        
        self._flush_task = threading.Thread(target=flush_worker, daemon=True)
        self._flush_task.start()
        logger.info(f"Started batch flusher with {self.batch_interval_seconds}s interval")
    
    def add_streaming_candles(
        self, 
        candles_df: pd.DataFrame, 
        timeframe: str
    ) -> int:
        """Add streaming candles to batch queue (non-blocking)"""
        
        if candles_df.empty:
            return 0
        
        with self._flush_lock:
            # Add to batch queue
            self.candle_batches[timeframe].append(candles_df.copy())
            self.batch_stats[f"candles_{timeframe}"]['rows'] += len(candles_df)
            
            logger.debug(f"Added {len(candles_df)} {timeframe} candles to batch queue")
            
        return len(candles_df)
    
    def add_streaming_ticks(
        self, 
        ticks_df: pd.DataFrame, 
        data_type: str
    ) -> int:
        """Add streaming tick data to batch queue (non-blocking)"""
        
        if ticks_df.empty:
            return 0
        
        with self._flush_lock:
            # Add to batch queue
            self.tick_batches[data_type].append(ticks_df.copy())
            self.batch_stats[f"ticks_{data_type}"]['rows'] += len(ticks_df)
            
            logger.debug(f"Added {len(ticks_df)} {data_type} ticks to batch queue")
            
        return len(ticks_df)
    
    async def _flush_candle_batch(self, timeframe: str):
        """Flush accumulated candle batch to BigQuery"""
        
        batch_key = f"candles_{timeframe}"
        
        with self._flush_lock:
            if not self.candle_batches[timeframe]:
                return
            
            # Combine all DataFrames in the batch
            batch_dfs = list(self.candle_batches[timeframe])
            self.candle_batches[timeframe].clear()
            
            # Update flush time
            self.last_flush_time[batch_key] = datetime.now(timezone.utc)
        
        if not batch_dfs:
            return
        
        try:
            # Combine all candles
            combined_df = pd.concat(batch_dfs, ignore_index=True)
            
            # Sort by timestamp_out for optimal insertion
            combined_df = combined_df.sort_values('timestamp_out')
            
            table_id = f"{self.project_id}.{self.dataset_id}.streaming_candles_{timeframe}"
            
            # Ensure table exists
            await self._ensure_streaming_table_exists(table_id, combined_df.columns.tolist(), "candles")
            
            # Upload batch
            rows_uploaded = await self._upload_dataframe(combined_df, table_id)
            
            # Update stats
            self.batch_stats[batch_key]['batches'] += 1
            
            logger.info(f"💾 Flushed {timeframe} candle batch: {rows_uploaded} rows, batch #{self.batch_stats[batch_key]['batches']}")
            
        except Exception as e:
            logger.error(f"Failed to flush {timeframe} candle batch: {e}")
    
    async def _flush_tick_batch(self, data_type: str):
        """Flush accumulated tick batch to BigQuery"""
        
        batch_key = f"ticks_{data_type}"
        
        with self._flush_lock:
            if not self.tick_batches[data_type]:
                return
            
            # Combine all DataFrames in the batch
            batch_dfs = list(self.tick_batches[data_type])
            self.tick_batches[data_type].clear()
            
            # Update flush time
            self.last_flush_time[batch_key] = datetime.now(timezone.utc)
        
        if not batch_dfs:
            return
        
        try:
            # Combine all ticks
            combined_df = pd.concat(batch_dfs, ignore_index=True)
            
            # Sort by timestamp_out for optimal insertion
            combined_df = combined_df.sort_values('timestamp_out')
            
            table_id = f"{self.project_id}.{self.dataset_id}.streaming_ticks_{data_type}"
            
            # Ensure table exists
            await self._ensure_streaming_table_exists(table_id, combined_df.columns.tolist(), "ticks")
            
            # Upload batch
            rows_uploaded = await self._upload_dataframe(combined_df, table_id)
            
            # Update stats
            self.batch_stats[batch_key]['batches'] += 1
            
            logger.info(f"💾 Flushed {data_type} tick batch: {rows_uploaded} rows, batch #{self.batch_stats[batch_key]['batches']}")
            
        except Exception as e:
            logger.error(f"Failed to flush {data_type} tick batch: {e}")
    
    async def force_flush_all(self):
        """Force flush all pending batches immediately"""
        
        logger.info("🚀 Force flushing all pending batches...")
        
        # Flush all candle batches
        for timeframe in list(self.candle_batches.keys()):
            if self.candle_batches[timeframe]:
                await self._flush_candle_batch(timeframe)
        
        # Flush all tick batches
        for data_type in list(self.tick_batches.keys()):
            if self.tick_batches[data_type]:
                await self._flush_tick_batch(data_type)
        
        logger.info("✅ All batches flushed")
    
    def get_batch_stats(self) -> Dict[str, Any]:
        """Get current batch statistics"""
        
        stats = {
            'batch_interval_seconds': self.batch_interval_seconds,
            'queued_batches': {},
            'total_stats': dict(self.batch_stats)
        }
        
        with self._flush_lock:
            # Count queued items
            for timeframe, queue in self.candle_batches.items():
                if queue:
                    queued_rows = sum(len(df) for df in queue)
                    stats['queued_batches'][f'candles_{timeframe}'] = {
                        'queued_dataframes': len(queue),
                        'queued_rows': queued_rows
                    }
            
            for data_type, queue in self.tick_batches.items():
                if queue:
                    queued_rows = sum(len(df) for df in queue)
                    stats['queued_batches'][f'ticks_{data_type}'] = {
                        'queued_dataframes': len(queue),
                        'queued_rows': queued_rows
                    }
        
        return stats
    
    def stop_batch_flusher(self):
        """Stop the background batch flusher"""
        self._stop_flushing = True
        if self._flush_task and self._flush_task.is_alive():
            self._flush_task.join(timeout=5)
        logger.info("Stopped batch flusher")
    
    # Legacy methods for backward compatibility
    async def upload_streaming_candles(
        self, 
        candles_df: pd.DataFrame, 
        timeframe: str
    ) -> int:
        """Upload streaming candles (legacy method - adds to batch queue)"""
        
        logger.warning("upload_streaming_candles is deprecated. Use add_streaming_candles for batching.")
        return self.add_streaming_candles(candles_df, timeframe)
    
    async def upload_streaming_ticks(
        self, 
        ticks_df: pd.DataFrame, 
        data_type: str
    ) -> int:
        """Upload streaming tick data (legacy method - adds to batch queue)"""
        
        logger.warning("upload_streaming_ticks is deprecated. Use add_streaming_ticks for batching.")
        return self.add_streaming_ticks(ticks_df, data_type)
    
    async def _ensure_streaming_table_exists(self, table_id: str, columns: List[str], data_type: str):
        """Ensure streaming BigQuery table exists with proper partitioning and clustering"""
        
        try:
            table = self.bq_client.get_table(table_id)
            logger.info(f"✅ Streaming table {table_id} exists")
        except NotFound:
            # Create table with streaming-optimized schema
            schema = self._get_streaming_schema(columns, data_type)
            
            table = bigquery.Table(table_id, schema=schema)
            
            # Streaming tables: hourly partitioning with 30-day TTL
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.HOUR,
                field="timestamp_out",
                expiration_ms=30 * 24 * 60 * 60 * 1000  # 30 days TTL
            )
            
            # Clustering for optimal query performance
            table.clustering_fields = ["exchange", "symbol"]
            
            table = self.bq_client.create_table(table)
            logger.info(f"✅ Created streaming table {table_id}")
            logger.info(f"  Partitioning: hourly (30d TTL) on timestamp_out")
            logger.info(f"  Clustering: exchange, symbol")
    
    def _get_streaming_schema(self, columns: List[str], data_type: str) -> List[bigquery.SchemaField]:
        """Get BigQuery schema optimized for streaming data"""
        
        schema = []
        
        # Common column types for streaming data
        if data_type == "candles":
            column_types = {
                'symbol': 'STRING',
                'exchange': 'STRING',
                'timeframe': 'STRING',
                'timestamp': 'TIMESTAMP',
                'timestamp_out': 'TIMESTAMP',
                'instrument_id': 'STRING',
                'open': 'FLOAT',
                'high': 'FLOAT',
                'low': 'FLOAT',
                'close': 'FLOAT',
                'volume': 'FLOAT',
                'trade_count': 'INTEGER',
                'vwap': 'FLOAT',
                # HFT features
                'buy_volume_sum': 'FLOAT',
                'sell_volume_sum': 'FLOAT',
                'size_avg': 'FLOAT',
                'price_vwap': 'FLOAT',
                'delay_median': 'FLOAT',
                'delay_max': 'FLOAT',
                'delay_min': 'FLOAT',
                'delay_mean': 'FLOAT',
                'liquidation_buy_volume': 'FLOAT',
                'liquidation_sell_volume': 'FLOAT',
                'liquidation_count': 'FLOAT',
                'funding_rate': 'FLOAT',
                'index_price': 'FLOAT',
                'mark_price': 'FLOAT',
                'open_interest': 'FLOAT',
                'predicted_funding_rate': 'FLOAT',
                'oi_change': 'FLOAT',
                'liquidation_with_rising_oi': 'FLOAT',
                'liquidation_with_falling_oi': 'FLOAT',
                'skew_25d_put_call_ratio': 'FLOAT',
                'atm_mark_iv': 'FLOAT'
            }
        else:  # ticks
            column_types = {
                'symbol': 'STRING',
                'exchange': 'STRING',
                'timestamp': 'TIMESTAMP',
                'timestamp_out': 'TIMESTAMP',
                'local_timestamp': 'TIMESTAMP',
                'instrument_id': 'STRING',
                'data_type': 'STRING',
                'price': 'FLOAT',
                'amount': 'FLOAT',
                'side': 'STRING',
                'trade_id': 'STRING',
                # Book snapshot fields
                'bid_price_1': 'FLOAT',
                'bid_volume_1': 'FLOAT',
                'ask_price_1': 'FLOAT',
                'ask_volume_1': 'FLOAT',
                # Liquidation fields
                'liquidation_side': 'STRING',
                'liquidation_amount': 'FLOAT',
                # Derivatives fields
                'funding_rate': 'FLOAT',
                'index_price': 'FLOAT',
                'mark_price': 'FLOAT',
                'open_interest': 'FLOAT',
                # Options fields
                'underlying': 'STRING',
                'strike_price': 'FLOAT',
                'option_type': 'STRING',
                'expiry_date': 'DATE',
                'mark_iv': 'FLOAT'
            }
        
        # Create schema fields for existing columns
        for column in columns:
            column_type = column_types.get(column, 'STRING')  # Default to STRING
            schema.append(bigquery.SchemaField(column, column_type))
        
        return schema
    
    async def _upload_dataframe(self, df: pd.DataFrame, table_id: str) -> int:
        """Upload DataFrame to streaming BigQuery table"""
        
        # Configure job for streaming
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",  # Always append for streaming
            create_disposition="CREATE_NEVER",  # Table should already exist
            ignore_unknown_values=True,  # Ignore extra columns
            max_bad_records=10  # Allow some bad records in streaming
        )
        
        try:
            job = self.bq_client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            
            # Wait for job to complete
            job.result()
            
            logger.info(f"📤 Uploaded {len(df)} rows to streaming table {table_id}")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to upload streaming data to {table_id}: {e}")
            raise
    
    def create_sample_queries(self) -> Dict[str, str]:
        """Generate sample queries for streaming data"""
        
        return {
            "recent_candles": f"""
                SELECT timestamp_out, symbol, exchange, open, high, low, close, volume,
                       buy_volume_sum, sell_volume_sum, price_vwap
                FROM `{self.project_id}.{self.dataset_id}.streaming_candles_1m`
                WHERE timestamp_out >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
                  AND exchange = 'binance'
                  AND symbol = 'BTC-USDT'
                ORDER BY timestamp_out DESC
                LIMIT 60
            """,
            
            "recent_ticks": f"""
                SELECT timestamp_out, symbol, exchange, price, amount, side, data_type
                FROM `{self.project_id}.{self.dataset_id}.streaming_ticks_trades`
                WHERE timestamp_out >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
                  AND exchange = 'binance'
                  AND symbol = 'BTC-USDT'
                ORDER BY timestamp_out DESC
                LIMIT 1000
            """,
            
            "volume_analysis": f"""
                SELECT 
                    TIMESTAMP_TRUNC(timestamp_out, MINUTE) as minute,
                    symbol,
                    exchange,
                    SUM(volume) as total_volume,
                    AVG(price_vwap) as avg_vwap,
                    COUNT(*) as candle_count
                FROM `{self.project_id}.{self.dataset_id}.streaming_candles_1m`
                WHERE timestamp_out >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
                  AND exchange = 'binance'
                GROUP BY minute, symbol, exchange
                ORDER BY minute DESC, total_volume DESC
            """
        }
