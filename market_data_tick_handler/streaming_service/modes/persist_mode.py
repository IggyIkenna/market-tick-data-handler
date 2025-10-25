"""
Persist Mode

Persists real-time data to BigQuery with optimized partitioning and clustering.
Separate from serving to allow independent scaling and processing.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from ..bigquery_client.streaming_client import BigQueryStreamingClient, StreamingConfig

logger = logging.getLogger(__name__)


@dataclass
class PersistConfig:
    """Configuration for persist mode"""
    project_id: str
    dataset_id: str
    batch_size: int = 1000
    batch_timeout_ms: int = 60000  # 1 minute for high-frequency
    max_batch_timeout_ms: int = 300000  # 5 minutes max
    is_live: bool = True
    enable_cost_optimization: bool = True


class PersistMode:
    """
    Persist mode for streaming data to BigQuery.
    
    Features:
    - Separate process from serving for independent scaling
    - Per data type tables with optimized schemas
    - Cost-optimized batching and partitioning
    - Exchange/symbol clustering for query performance
    """
    
    def __init__(self, config: PersistConfig):
        """
        Initialize persist mode.
        
        Args:
            config: Persist mode configuration
        """
        self.config = config
        
        # Initialize BigQuery client with streaming config
        streaming_config = StreamingConfig(
            project_id=config.project_id,
            dataset_id=config.dataset_id,
            batch_size=config.batch_size,
            batch_timeout_ms=config.batch_timeout_ms,
            max_batch_timeout_ms=config.max_batch_timeout_ms,
            is_live=config.is_live
        )
        
        self.bq_client = BigQueryStreamingClient(streaming_config)
        
        # Statistics
        self.stats = {
            'ticks_persisted': 0,
            'candles_persisted': 0,
            'errors': 0,
            'tables_used': set(),
            'start_time': datetime.now(timezone.utc)
        }
        
        logger.info("✅ PersistMode initialized")
        logger.info(f"   Project: {config.project_id}")
        logger.info(f"   Dataset: {config.dataset_id}")
        logger.info(f"   Mode: {'Live' if config.is_live else 'Historical'}")
    
    async def persist_tick_data(self, tick_data: Dict[str, Any]) -> bool:
        """
        Persist tick data to appropriate BigQuery table.
        
        Args:
            tick_data: Processed tick data
            
        Returns:
            True if successful
        """
        try:
            data_type = tick_data.get('data_type', 'unknown')
            table_name = f"ticks_{data_type}"
            
            # Ensure timestamp_out is set
            if 'timestamp_out' not in tick_data:
                tick_data['timestamp_out'] = datetime.now(timezone.utc)
            
            # Stream to BigQuery
            success = await self.bq_client.stream_data(table_name, tick_data)
            
            if success:
                self.stats['ticks_persisted'] += 1
                self.stats['tables_used'].add(table_name)
                logger.debug(f"💾 Persisted tick: {tick_data['symbol']} {data_type}")
            else:
                self.stats['errors'] += 1
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error persisting tick data: {e}")
            self.stats['errors'] += 1
            return False
    
    async def persist_candle_with_features(self, candle_data, hft_features=None) -> bool:
        """
        Persist candle data with HFT features to BigQuery.
        
        Args:
            candle_data: CandleData object
            hft_features: HFTFeatures object (optional)
            
        Returns:
            True if successful
        """
        try:
            table_name = f"candles_{candle_data.timeframe}"
            
            # Prepare candle data for BigQuery
            persist_data = {
                'symbol': candle_data.symbol,
                'exchange': getattr(candle_data, 'exchange', 'unknown'),
                'timestamp': candle_data.timestamp_in,
                'timestamp_in': candle_data.timestamp_in,
                'timestamp_out': candle_data.timestamp_out or datetime.now(timezone.utc),
                'data_type': 'candles',
                'timeframe': candle_data.timeframe,
                
                # OHLCV data
                'open': candle_data.open,
                'high': candle_data.high,
                'low': candle_data.low,
                'close': candle_data.close,
                'volume': candle_data.volume,
                'trade_count': candle_data.trade_count,
                'vwap': candle_data.vwap,
            }
            
            # Add HFT features if available
            if hft_features:
                features_dict = hft_features.to_dict()
                # Remove non-BigQuery fields
                features_dict.pop('symbol', None)
                features_dict.pop('timeframe', None)
                features_dict.pop('timestamp', None)
                
                persist_data.update(features_dict)
            
            # Stream to BigQuery
            success = await self.bq_client.stream_data(table_name, persist_data)
            
            if success:
                self.stats['candles_persisted'] += 1
                self.stats['tables_used'].add(table_name)
                logger.debug(f"💾 Persisted candle: {candle_data.symbol} {candle_data.timeframe} @ ${candle_data.close}")
            else:
                self.stats['errors'] += 1
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error persisting candle with features: {e}")
            self.stats['errors'] += 1
            return False
    
    async def persist_batch_data(self, 
                                table_name: str, 
                                data_batch: List[Dict[str, Any]]) -> bool:
        """
        Persist batch of data to BigQuery.
        
        Args:
            table_name: Target table name
            data_batch: List of data records
            
        Returns:
            True if successful
        """
        try:
            # Ensure all records have timestamp_out
            for record in data_batch:
                if 'timestamp_out' not in record:
                    record['timestamp_out'] = datetime.now(timezone.utc)
            
            success = await self.bq_client.stream_data(table_name, data_batch)
            
            if success:
                self.stats['ticks_persisted'] += len(data_batch)
                self.stats['tables_used'].add(table_name)
                logger.info(f"💾 Persisted batch: {len(data_batch)} records to {table_name}")
            else:
                self.stats['errors'] += 1
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error persisting batch to {table_name}: {e}")
            self.stats['errors'] += 1
            return False
    
    async def ensure_tables_exist(self, table_names: List[str]) -> Dict[str, bool]:
        """
        Ensure all required tables exist.
        
        Args:
            table_names: List of table names to create
            
        Returns:
            Dictionary of table_name -> success status
        """
        results = {}
        
        for table_name in table_names:
            try:
                success = await self.bq_client.ensure_table_exists(table_name)
                results[table_name] = success
                
                if success:
                    logger.info(f"✅ Table ready: {table_name}")
                else:
                    logger.error(f"❌ Failed to create table: {table_name}")
                    
            except Exception as e:
                logger.error(f"❌ Error ensuring table {table_name}: {e}")
                results[table_name] = False
        
        return results
    
    def get_cost_estimate(self) -> Dict[str, Any]:
        """Get cost estimate for current streaming"""
        bq_stats = self.bq_client.get_stats()
        
        # Rough cost estimates (actual costs may vary)
        estimated_cost_per_gb = 0.01  # $0.01 per GB for streaming
        estimated_gb_per_1k_rows = 0.001  # Rough estimate
        
        total_rows = bq_stats['total_rows_streamed']
        estimated_gb = total_rows * estimated_gb_per_1k_rows / 1000
        estimated_cost = estimated_gb * estimated_cost_per_gb
        
        return {
            'total_rows_streamed': total_rows,
            'estimated_gb': estimated_gb,
            'estimated_cost_usd': estimated_cost,
            'cost_per_row': estimated_cost / max(total_rows, 1),
            'bigquery_stats': bq_stats
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get persist mode statistics"""
        runtime = datetime.now(timezone.utc) - self.stats['start_time']
        
        base_stats = {
            'ticks_persisted': self.stats['ticks_persisted'],
            'candles_persisted': self.stats['candles_persisted'],
            'errors': self.stats['errors'],
            'tables_used': list(self.stats['tables_used']),
            'runtime_seconds': runtime.total_seconds(),
            'records_per_second': (self.stats['ticks_persisted'] + self.stats['candles_persisted']) / max(runtime.total_seconds(), 1),
            'error_rate': self.stats['errors'] / max(self.stats['ticks_persisted'] + self.stats['candles_persisted'], 1)
        }
        
        # Add BigQuery client stats
        bq_stats = self.bq_client.get_stats()
        base_stats['bigquery'] = bq_stats
        
        # Add cost estimates if enabled
        if self.config.enable_cost_optimization:
            base_stats['cost_estimate'] = self.get_cost_estimate()
        
        return base_stats
    
    async def flush_all_data(self) -> None:
        """Flush all pending data to BigQuery"""
        logger.info("🔄 Flushing all pending data...")
        await self.bq_client.flush_all_batches()
        logger.info("✅ All data flushed")
    
    async def shutdown(self) -> None:
        """Shutdown persist mode"""
        logger.info("🛑 Shutting down PersistMode...")
        
        # Flush any remaining data
        await self.flush_all_data()
        
        # Shutdown BigQuery client
        await self.bq_client.shutdown()
        
        # Show final stats
        final_stats = self.get_stats()
        logger.info("📊 Final PersistMode Statistics:")
        logger.info(f"   Ticks persisted: {final_stats['ticks_persisted']:,}")
        logger.info(f"   Candles persisted: {final_stats['candles_persisted']:,}")
        logger.info(f"   Tables used: {len(final_stats['tables_used'])}")
        logger.info(f"   Error rate: {final_stats['error_rate']:.2%}")
        
        if 'cost_estimate' in final_stats:
            cost = final_stats['cost_estimate']
            logger.info(f"   Estimated cost: ${cost['estimated_cost_usd']:.4f}")
        
        logger.info("✅ PersistMode shutdown complete")


# Example usage
if __name__ == "__main__":
    import asyncio
    from datetime import datetime, timezone
    
    async def test_persist_mode():
        # Test persist mode
        config = PersistConfig(
            project_id="your-project-id",
            dataset_id="market_data_streaming_test",
            batch_size=10,  # Small batch for testing
            batch_timeout_ms=5000,  # 5 seconds for testing
            is_live=True
        )
        
        persist_mode = PersistMode(config)
        
        # Test persisting tick data
        tick_data = {
            'symbol': 'BTC-USDT',
            'exchange': 'binance',
            'timestamp': datetime.now(timezone.utc),
            'data_type': 'trades',
            'price': 67000.0,
            'amount': 0.1,
            'side': 'buy',
            'trade_id': 'test_123'
        }
        
        success = await persist_mode.persist_tick_data(tick_data)
        print(f"Tick persist success: {success}")
        
        # Test persisting candle data
        class MockCandle:
            def __init__(self):
                self.symbol = "BTC-USDT"
                self.timeframe = "1m"
                self.timestamp_in = datetime.now(timezone.utc)
                self.timestamp_out = datetime.now(timezone.utc)
                self.open = 67000.0
                self.high = 67100.0
                self.low = 66900.0
                self.close = 67050.0
                self.volume = 1.5
                self.trade_count = 45
                self.vwap = 67025.0
        
        class MockFeatures:
            def to_dict(self):
                return {
                    'sma_5': 67025.0,
                    'ema_5': 67030.0,
                    'rsi_5': 55.2,
                    'price_volatility_5': 0.001
                }
        
        candle = MockCandle()
        features = MockFeatures()
        
        success = await persist_mode.persist_candle_with_features(candle, features)
        print(f"Candle persist success: {success}")
        
        # Show stats
        stats = persist_mode.get_stats()
        print(f"Stats: {stats}")
        
        await persist_mode.shutdown()
    
    # Uncomment to run test (requires valid GCP credentials)
    # asyncio.run(test_persist_mode())
