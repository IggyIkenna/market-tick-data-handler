"""
High-Frequency Trading (HFT) Features Calculator

Computes real-time HFT features on 15s and 1m candles:
- Moving averages (SMA, EMA, WMA)
- Price momentum and velocity
- Volume-weighted indicators
- Volatility measures
- Order flow imbalance proxies
- Microstructure features
"""

import asyncio
import logging
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from collections import deque
import math

logger = logging.getLogger(__name__)


@dataclass
class HFTFeatures:
    """Container for computed HFT features"""
    symbol: str
    timeframe: str
    timestamp: datetime
    
    # Price-based features
    price: float
    sma_5: Optional[float] = None
    sma_10: Optional[float] = None
    sma_20: Optional[float] = None
    ema_5: Optional[float] = None
    ema_10: Optional[float] = None
    ema_20: Optional[float] = None
    wma_5: Optional[float] = None
    
    # Momentum features
    price_momentum_3: Optional[float] = None  # 3-period momentum
    price_momentum_5: Optional[float] = None  # 5-period momentum
    price_velocity: Optional[float] = None    # Rate of price change
    price_acceleration: Optional[float] = None # Rate of velocity change
    
    # Volume features
    volume: float = 0.0
    volume_sma_5: Optional[float] = None
    volume_ema_5: Optional[float] = None
    volume_ratio: Optional[float] = None      # Current vs average volume
    vwap: Optional[float] = None
    vwap_deviation: Optional[float] = None    # Price deviation from VWAP
    
    # Volatility features
    price_volatility_5: Optional[float] = None   # 5-period price volatility
    price_volatility_10: Optional[float] = None  # 10-period price volatility
    high_low_ratio: Optional[float] = None       # (High-Low)/Close
    close_to_close_return: Optional[float] = None # Log return
    
    # Microstructure features
    trade_intensity: Optional[float] = None      # Trades per unit time
    avg_trade_size: Optional[float] = None       # Average trade size
    price_impact: Optional[float] = None         # Price impact estimate
    bid_ask_spread_proxy: Optional[float] = None # High-Low as spread proxy
    
    # Technical indicators
    rsi_5: Optional[float] = None               # 5-period RSI
    bollinger_position: Optional[float] = None  # Position within Bollinger bands
    macd_signal: Optional[float] = None         # MACD signal
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'symbol': self.symbol,
            'timeframe': self.timeframe,
            'timestamp': self.timestamp.isoformat(),
            'price': self.price,
            'sma_5': self.sma_5,
            'sma_10': self.sma_10,
            'sma_20': self.sma_20,
            'ema_5': self.ema_5,
            'ema_10': self.ema_10,
            'ema_20': self.ema_20,
            'wma_5': self.wma_5,
            'price_momentum_3': self.price_momentum_3,
            'price_momentum_5': self.price_momentum_5,
            'price_velocity': self.price_velocity,
            'price_acceleration': self.price_acceleration,
            'volume': self.volume,
            'volume_sma_5': self.volume_sma_5,
            'volume_ema_5': self.volume_ema_5,
            'volume_ratio': self.volume_ratio,
            'vwap': self.vwap,
            'vwap_deviation': self.vwap_deviation,
            'price_volatility_5': self.price_volatility_5,
            'price_volatility_10': self.price_volatility_10,
            'high_low_ratio': self.high_low_ratio,
            'close_to_close_return': self.close_to_close_return,
            'trade_intensity': self.trade_intensity,
            'avg_trade_size': self.avg_trade_size,
            'price_impact': self.price_impact,
            'bid_ask_spread_proxy': self.bid_ask_spread_proxy,
            'rsi_5': self.rsi_5,
            'bollinger_position': self.bollinger_position,
            'macd_signal': self.macd_signal
        }


class HFTFeatureCalculator:
    """
    Real-time HFT feature calculator for high-frequency trading strategies.
    
    Computes features on 15s and 1m candles with efficient rolling calculations.
    """
    
    def __init__(self, 
                 symbol: str,
                 timeframes: List[str] = None,
                 max_history: int = 100):
        """
        Initialize HFT feature calculator.
        
        Args:
            symbol: Trading symbol
            timeframes: Timeframes to compute features for
            max_history: Maximum candles to keep in history
        """
        self.symbol = symbol
        self.timeframes = timeframes or ['15s', '1m']
        self.max_history = max_history
        
        # Price and volume history for each timeframe
        self.price_history: Dict[str, deque] = {tf: deque(maxlen=max_history) for tf in self.timeframes}
        self.volume_history: Dict[str, deque] = {tf: deque(maxlen=max_history) for tf in self.timeframes}
        self.high_history: Dict[str, deque] = {tf: deque(maxlen=max_history) for tf in self.timeframes}
        self.low_history: Dict[str, deque] = {tf: deque(maxlen=max_history) for tf in self.timeframes}
        self.vwap_history: Dict[str, deque] = {tf: deque(maxlen=max_history) for tf in self.timeframes}
        self.trade_count_history: Dict[str, deque] = {tf: deque(maxlen=max_history) for tf in self.timeframes}
        
        # EMA state for efficient calculation
        self.ema_state: Dict[str, Dict[int, float]] = {tf: {} for tf in self.timeframes}
        
        # Feature cache
        self.last_features: Dict[str, HFTFeatures] = {}
        
        logger.info(f"✅ HFTFeatureCalculator initialized for {symbol}")
        logger.info(f"   Timeframes: {self.timeframes}")
        logger.info(f"   Max history: {max_history}")
    
    async def compute_features(self, candle_data) -> Optional[HFTFeatures]:
        """
        Compute HFT features for a completed candle (live processing).
        
        Args:
            candle_data: CandleData object
            
        Returns:
            HFTFeatures object or None if insufficient data
        """
        return await self.compute_incremental(candle_data)
    
    async def compute_incremental(self, candle_data) -> Optional[HFTFeatures]:
        """
        Compute HFT features incrementally for live streaming.
        
        Args:
            candle_data: CandleData object
            
        Returns:
            HFTFeatures object or None if insufficient data
        """
        if candle_data.timeframe not in self.timeframes:
            return None
        
        try:
            # Update history
            self._update_history(candle_data)
            
            # Compute features
            features = await self._compute_all_features(candle_data)
            
            # Cache features
            self.last_features[candle_data.timeframe] = features
            
            return features
            
        except Exception as e:
            logger.error(f"❌ Error computing HFT features (incremental): {e}")
            return None
    
    async def compute_batch(self, candles_df, timeframe: str = "1m") -> List[HFTFeatures]:
        """
        Compute HFT features for historical batch processing.
        
        Args:
            candles_df: DataFrame with OHLCV candle data
            timeframe: Timeframe for the candles
            
        Returns:
            List of HFTFeatures objects
        """
        try:
            import pandas as pd
            
            features_list = []
            
            # Sort by timestamp to ensure proper order
            candles_df = candles_df.sort_values('timestamp_in')
            
            # Reset history for batch processing
            tf = timeframe
            if tf not in self.timeframes:
                self.timeframes.append(tf)
                self.price_history[tf] = deque(maxlen=self.max_history)
                self.volume_history[tf] = deque(maxlen=self.max_history)
                self.high_history[tf] = deque(maxlen=self.max_history)
                self.low_history[tf] = deque(maxlen=self.max_history)
                self.vwap_history[tf] = deque(maxlen=self.max_history)
                self.trade_count_history[tf] = deque(maxlen=self.max_history)
                self.ema_state[tf] = {}
            
            # Clear existing history for fresh batch calculation
            self.price_history[tf].clear()
            self.volume_history[tf].clear()
            self.high_history[tf].clear()
            self.low_history[tf].clear()
            self.vwap_history[tf].clear()
            self.trade_count_history[tf].clear()
            self.ema_state[tf].clear()
            
            # Process each candle
            for _, row in candles_df.iterrows():
                # Create mock candle data object
                candle_data = type('CandleData', (), {
                    'symbol': self.symbol,
                    'timeframe': tf,
                    'timestamp_in': pd.to_datetime(row['timestamp_in']),
                    'close': float(row['close']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'volume': float(row['volume']),
                    'trade_count': int(row.get('trade_count', 1)),
                    'vwap': float(row.get('vwap', row['close'])) if pd.notna(row.get('vwap')) else float(row['close'])
                })()
                
                # Compute features incrementally
                features = await self.compute_incremental(candle_data)
                if features:
                    features_list.append(features)
            
            logger.info(f"✅ Computed HFT features for {len(features_list)} candles (batch)")
            return features_list
            
        except Exception as e:
            logger.error(f"❌ Error computing HFT features (batch): {e}")
            return []
    
    def _update_history(self, candle_data) -> None:
        """Update price and volume history"""
        tf = candle_data.timeframe
        
        self.price_history[tf].append(candle_data.close)
        self.volume_history[tf].append(candle_data.volume)
        self.high_history[tf].append(candle_data.high)
        self.low_history[tf].append(candle_data.low)
        self.trade_count_history[tf].append(candle_data.trade_count)
        
        if candle_data.vwap:
            self.vwap_history[tf].append(candle_data.vwap)
    
    async def _compute_all_features(self, candle_data) -> HFTFeatures:
        """Compute all HFT features"""
        tf = candle_data.timeframe
        
        features = HFTFeatures(
            symbol=self.symbol,
            timeframe=tf,
            timestamp=candle_data.timestamp_in,
            price=candle_data.close,
            volume=candle_data.volume,
            vwap=candle_data.vwap
        )
        
        # Price-based features
        await self._compute_moving_averages(features, tf)
        await self._compute_momentum_features(features, tf)
        
        # Volume features
        await self._compute_volume_features(features, tf)
        
        # Volatility features
        await self._compute_volatility_features(features, tf, candle_data)
        
        # Microstructure features
        await self._compute_microstructure_features(features, tf, candle_data)
        
        # Technical indicators
        await self._compute_technical_indicators(features, tf)
        
        return features
    
    async def _compute_moving_averages(self, features: HFTFeatures, tf: str) -> None:
        """Compute moving averages"""
        prices = list(self.price_history[tf])
        
        if len(prices) >= 5:
            features.sma_5 = np.mean(prices[-5:])
            features.ema_5 = self._compute_ema(prices[-1], tf, 5)
            features.wma_5 = self._compute_wma(prices[-5:])
        
        if len(prices) >= 10:
            features.sma_10 = np.mean(prices[-10:])
            features.ema_10 = self._compute_ema(prices[-1], tf, 10)
        
        if len(prices) >= 20:
            features.sma_20 = np.mean(prices[-20:])
            features.ema_20 = self._compute_ema(prices[-1], tf, 20)
    
    def _compute_ema(self, current_price: float, tf: str, period: int) -> float:
        """Compute exponential moving average efficiently"""
        alpha = 2.0 / (period + 1)
        key = f"{tf}_{period}"
        
        if key not in self.ema_state:
            # Initialize with current price
            self.ema_state[tf][period] = current_price
            return current_price
        
        prev_ema = self.ema_state[tf][period]
        new_ema = alpha * current_price + (1 - alpha) * prev_ema
        self.ema_state[tf][period] = new_ema
        
        return new_ema
    
    def _compute_wma(self, prices: List[float]) -> float:
        """Compute weighted moving average"""
        if not prices:
            return 0.0
        
        weights = np.arange(1, len(prices) + 1)
        return np.average(prices, weights=weights)
    
    async def _compute_momentum_features(self, features: HFTFeatures, tf: str) -> None:
        """Compute momentum-based features"""
        prices = list(self.price_history[tf])
        
        if len(prices) >= 4:
            # 3-period momentum
            features.price_momentum_3 = (prices[-1] - prices[-4]) / prices[-4]
        
        if len(prices) >= 6:
            # 5-period momentum
            features.price_momentum_5 = (prices[-1] - prices[-6]) / prices[-6]
        
        if len(prices) >= 3:
            # Price velocity (rate of change)
            features.price_velocity = prices[-1] - prices[-2]
            
            if len(prices) >= 4:
                # Price acceleration
                prev_velocity = prices[-2] - prices[-3]
                features.price_acceleration = features.price_velocity - prev_velocity
    
    async def _compute_volume_features(self, features: HFTFeatures, tf: str) -> None:
        """Compute volume-based features"""
        volumes = list(self.volume_history[tf])
        
        if len(volumes) >= 5:
            features.volume_sma_5 = np.mean(volumes[-5:])
            features.volume_ema_5 = self._compute_volume_ema(volumes[-1], tf, 5)
            
            # Volume ratio (current vs average)
            if features.volume_sma_5 > 0:
                features.volume_ratio = features.volume / features.volume_sma_5
        
        # VWAP deviation
        if features.vwap and features.vwap > 0:
            features.vwap_deviation = (features.price - features.vwap) / features.vwap
    
    def _compute_volume_ema(self, current_volume: float, tf: str, period: int) -> float:
        """Compute volume EMA"""
        alpha = 2.0 / (period + 1)
        key = f"vol_{tf}_{period}"
        
        if key not in self.ema_state:
            self.ema_state[tf][f"vol_{period}"] = current_volume
            return current_volume
        
        prev_ema = self.ema_state[tf][f"vol_{period}"]
        new_ema = alpha * current_volume + (1 - alpha) * prev_ema
        self.ema_state[tf][f"vol_{period}"] = new_ema
        
        return new_ema
    
    async def _compute_volatility_features(self, features: HFTFeatures, tf: str, candle_data) -> None:
        """Compute volatility-based features"""
        prices = list(self.price_history[tf])
        
        # Price volatility (rolling standard deviation)
        if len(prices) >= 5:
            features.price_volatility_5 = np.std(prices[-5:])
        
        if len(prices) >= 10:
            features.price_volatility_10 = np.std(prices[-10:])
        
        # High-Low ratio
        if candle_data.close > 0:
            features.high_low_ratio = (candle_data.high - candle_data.low) / candle_data.close
        
        # Close-to-close return
        if len(prices) >= 2:
            features.close_to_close_return = math.log(prices[-1] / prices[-2])
    
    async def _compute_microstructure_features(self, features: HFTFeatures, tf: str, candle_data) -> None:
        """Compute microstructure features"""
        trade_counts = list(self.trade_count_history[tf])
        
        # Trade intensity (trades per unit time)
        timeframe_seconds = self._get_timeframe_seconds(tf)
        if timeframe_seconds > 0:
            features.trade_intensity = candle_data.trade_count / timeframe_seconds
        
        # Average trade size
        if candle_data.trade_count > 0:
            features.avg_trade_size = candle_data.volume / candle_data.trade_count
        
        # Price impact estimate (simplified)
        if len(trade_counts) >= 2 and trade_counts[-1] > 0:
            volume_change = candle_data.volume - (self.volume_history[tf][-2] if len(self.volume_history[tf]) >= 2 else 0)
            if volume_change > 0:
                price_change = abs(candle_data.close - (self.price_history[tf][-2] if len(self.price_history[tf]) >= 2 else candle_data.close))
                features.price_impact = price_change / volume_change
        
        # Bid-ask spread proxy (High-Low)
        features.bid_ask_spread_proxy = candle_data.high - candle_data.low
    
    def _get_timeframe_seconds(self, timeframe: str) -> int:
        """Convert timeframe to seconds"""
        if timeframe == '15s':
            return 15
        elif timeframe == '1m':
            return 60
        elif timeframe == '5m':
            return 300
        elif timeframe == '15m':
            return 900
        elif timeframe == '4h':
            return 14400
        elif timeframe == '24h':
            return 86400
        return 60  # Default
    
    async def _compute_technical_indicators(self, features: HFTFeatures, tf: str) -> None:
        """Compute technical indicators"""
        prices = list(self.price_history[tf])
        
        # RSI (5-period)
        if len(prices) >= 6:
            features.rsi_5 = self._compute_rsi(prices[-6:])
        
        # Bollinger band position
        if features.sma_20 and features.price_volatility_10:
            upper_band = features.sma_20 + (2 * features.price_volatility_10)
            lower_band = features.sma_20 - (2 * features.price_volatility_10)
            if upper_band != lower_band:
                features.bollinger_position = (features.price - lower_band) / (upper_band - lower_band)
        
        # MACD signal (simplified)
        if features.ema_5 and features.ema_10:
            features.macd_signal = features.ema_5 - features.ema_10
    
    def _compute_rsi(self, prices: List[float], period: int = 5) -> float:
        """Compute Relative Strength Index"""
        if len(prices) < 2:
            return 50.0
        
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        avg_gain = np.mean(gains[-period:]) if gains else 0
        avg_loss = np.mean(losses[-period:]) if losses else 0
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def get_latest_features(self, timeframe: str) -> Optional[HFTFeatures]:
        """Get latest computed features for a timeframe"""
        return self.last_features.get(timeframe)
    
    def get_feature_summary(self) -> Dict[str, Any]:
        """Get summary of computed features across timeframes"""
        summary = {
            'symbol': self.symbol,
            'timeframes': {},
            'total_features_computed': len(self.last_features)
        }
        
        for tf, features in self.last_features.items():
            summary['timeframes'][tf] = {
                'timestamp': features.timestamp.isoformat(),
                'price': features.price,
                'volume': features.volume,
                'key_features': {
                    'sma_5': features.sma_5,
                    'ema_5': features.ema_5,
                    'price_momentum_5': features.price_momentum_5,
                    'volume_ratio': features.volume_ratio,
                    'price_volatility_5': features.price_volatility_5,
                    'rsi_5': features.rsi_5
                }
            }
        
        return summary


# Example usage and testing
if __name__ == "__main__":
    import asyncio
    from datetime import datetime, timezone
    from dataclasses import dataclass
    
    @dataclass
    class MockCandleData:
        symbol: str = "BTC-USDT"
        timeframe: str = "1m"
        timestamp_in: datetime = datetime.now(timezone.utc)
        close: float = 67000.0
        high: float = 67100.0
        low: float = 66900.0
        volume: float = 1.5
        trade_count: int = 50
        vwap: float = 67000.0
    
    async def test_hft_calculator():
        # Create calculator
        calc = HFTFeatureCalculator(
            symbol="BTC-USDT",
            timeframes=['1m']
        )
        
        # Generate test candles with price movement
        base_price = 67000.0
        for i in range(30):
            price = base_price + math.sin(i * 0.2) * 100  # Sine wave price movement
            
            candle = MockCandleData(
                close=price,
                high=price + 20,
                low=price - 20,
                volume=1.0 + (i % 5) * 0.2,
                trade_count=40 + (i % 10)
            )
            
            features = await calc.compute_features(candle)
            
            if features and i >= 20:  # Show features after enough history
                print(f"Candle {i}: Price=${price:.2f}")
                if features.sma_5:
                    print(f"  SMA(5): ${features.sma_5:.2f}")
                if features.ema_5:
                    print(f"  EMA(5): ${features.ema_5:.2f}")
                if features.price_momentum_5:
                    print(f"  Momentum(5): {features.price_momentum_5:.4f}")
                if features.rsi_5:
                    print(f"  RSI(5): {features.rsi_5:.1f}")
                print()
        
        # Show summary
        summary = calc.get_feature_summary()
        print("Feature Summary:")
        print(f"  Features computed: {summary['total_features_computed']}")
        for tf, data in summary['timeframes'].items():
            print(f"  {tf}: {len(data['key_features'])} key features")
    
    asyncio.run(test_hft_calculator())
