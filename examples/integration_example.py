#!/usr/bin/env python3
"""
Integration Example: How to Use Production Candle Processor

This example shows how to integrate the production candle processor
into your existing market data service.
"""

import pandas as pd
from datetime import date, datetime
from production_candle_processor import (
    ProductionCandleProcessor, 
    CandleConfig, 
    TradeData, 
    generate_daily_candles,
    validate_candle_boundaries
)

def integrate_with_existing_service():
    """Example of integrating with existing service"""
    
    # 1. Configuration
    config = CandleConfig(
        interval_seconds=60,      # 1-minute candles
        latency_ms=200,          # 200ms latency simulation
        boundary_shift_minutes=1, # Shift boundaries by 1 minute
        target_date=date(2023, 5, 23)
    )
    
    # 2. Load your existing trade data
    # This would be your existing data loading logic
    df_trades = load_trades_from_gcs("BINANCE:SPOT_PAIR:BTC-USDT", date(2023, 5, 23))
    
    # 3. Generate candles with correct boundaries
    candles = generate_daily_candles(df_trades, date(2023, 5, 23), config)
    
    # 4. Validate results
    if validate_candle_boundaries(candles):
        print("✅ Candles generated successfully")
    else:
        print("❌ Candle validation failed")
    
    # 5. Save to your existing storage
    save_candles_to_gcs(candles, "BINANCE:SPOT_PAIR:BTC-USDT", date(2023, 5, 23))
    
    return candles

def live_streaming_integration():
    """Example of integrating with live streaming"""
    
    # 1. Create processor for live streaming
    config = CandleConfig(interval_seconds=60, latency_ms=200)
    processor = ProductionCandleProcessor(config)
    
    # 2. Process trades as they arrive (simulated)
    def on_trade_received(trade_data):
        """Callback for when a trade is received"""
        
        # Convert your trade format to TradeData
        trade = TradeData(
            timestamp=trade_data['timestamp'],
            price=trade_data['price'],
            amount=trade_data['amount'],
            side=trade_data.get('side'),
            trade_id=trade_data.get('id')
        )
        
        # Process trade
        processing_time = processor._process_trade(trade)
        
        # Log processing time
        if processing_time > 0.001:  # Log if > 1ms
            print(f"Slow trade processing: {processing_time*1000:.3f}ms")
    
    # 3. Get completed candles periodically
    def get_completed_candles():
        """Get candles that are complete (not current)"""
        return processor.get_completed_candles()
    
    return on_trade_received, get_completed_candles

def batch_processing_integration():
    """Example of integrating with batch processing"""
    
    # 1. Process multiple days
    dates = [date(2023, 5, 23), date(2023, 5, 24), date(2023, 5, 25)]
    
    for target_date in dates:
        print(f"Processing {target_date}...")
        
        # 2. Load data for this date
        df_trades = load_trades_from_gcs("BINANCE:SPOT_PAIR:BTC-USDT", target_date)
        
        # 3. Generate candles
        candles = generate_daily_candles(df_trades, target_date)
        
        # 4. Validate and save
        if validate_candle_boundaries(candles):
            save_candles_to_gcs(candles, "BINANCE:SPOT_PAIR:BTC-USDT", target_date)
            print(f"✅ {target_date}: {len(candles)} candles")
        else:
            print(f"❌ {target_date}: Validation failed")

def update_existing_models():
    """Example of updating your existing models"""
    
    # Your existing TradeData model (update this)
    from dataclasses import dataclass
    
    @dataclass
    class TradeData:
        """Enhanced trade data with realistic timing"""
        timestamp: int              # Exchange timestamp (microseconds)
        local_timestamp: int        # Tardis receive timestamp (microseconds)
        our_receive_timestamp: int  # When we receive it (microseconds)
        id: str
        side: str
        price: float
        amount: float
        
        def get_processing_timestamp(self, mode: str = 'realistic') -> int:
            """Get appropriate timestamp based on processing mode"""
            if mode == 'perfect':
                return self.timestamp  # Exchange time
            elif mode == 'tardis':
                return self.local_timestamp  # Tardis receive time
            elif mode == 'realistic':
                return self.our_receive_timestamp  # Our receive time
            else:
                raise ValueError(f"Unknown mode: {mode}")
    
    # Your existing OHLCV model (update this)
    @dataclass
    class OHLCV:
        """OHLCV candle data"""
        timestamp: int
        open: float
        high: float
        low: float
        close: float
        volume: float
        trade_count: int
        
        def to_dict(self) -> dict:
            """Convert to dictionary for storage"""
            return {
                'timestamp': self.timestamp,
                'open': self.open,
                'high': self.high,
                'low': self.low,
                'close': self.close,
                'volume': self.volume,
                'trade_count': self.trade_count
            }

def migration_checklist():
    """Checklist for migrating to new candle boundaries"""
    
    checklist = [
        "✅ Test new candle processor with historical data",
        "✅ Validate exactly 1,440 candles per day",
        "✅ Update data models to include boundary shift",
        "✅ Update live streaming service to use new processor",
        "✅ Update batch processing to use new processor",
        "✅ Update data storage format",
        "✅ Update API endpoints to return new candle format",
        "✅ Update monitoring to track candle count",
        "✅ Deploy to staging environment",
        "✅ Run parallel processing for validation",
        "✅ Deploy to production with feature flag",
        "✅ Monitor for 24 hours",
        "✅ Remove old boundary logic",
        "✅ Update documentation"
    ]
    
    print("Migration Checklist:")
    for item in checklist:
        print(f"  {item}")

# Placeholder functions (replace with your actual implementations)
def load_trades_from_gcs(instrument: str, target_date: date) -> pd.DataFrame:
    """Load trades from GCS (replace with your implementation)"""
    # Your existing GCS loading logic
    pass

def save_candles_to_gcs(candles, instrument: str, target_date: date):
    """Save candles to GCS (replace with your implementation)"""
    # Your existing GCS saving logic
    pass

if __name__ == "__main__":
    print("🔧 Candle Processor Integration Examples")
    print("=" * 50)
    
    # Show migration checklist
    migration_checklist()
    
    print("\n📚 Available Integration Examples:")
    print("  1. integrate_with_existing_service()")
    print("  2. live_streaming_integration()")
    print("  3. batch_processing_integration()")
    print("  4. update_existing_models()")
    print("  5. migration_checklist()")
