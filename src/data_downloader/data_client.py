"""
Data Client

Mock data client for testing purposes.
"""

from typing import List, Optional
from datetime import datetime
import pandas as pd


class DataClient:
    """Mock data client for testing"""
    
    def __init__(self, bucket: str, config=None):
        self.bucket = bucket
        self.config = config
    
    async def get_candles(
        self,
        instrument_id: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[dict]:
        """Mock method to get candles"""
        # Return empty list for testing
        return []
    
    async def get_tick_data(
        self,
        instrument_id: str,
        data_type: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[dict]:
        """Mock method to get tick data"""
        # Return empty list for testing
        return []