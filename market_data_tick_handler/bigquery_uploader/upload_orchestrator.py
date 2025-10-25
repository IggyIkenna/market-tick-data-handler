"""
Upload Orchestrator

Orchestrates batch uploads of candle data to BigQuery for date ranges.
Handles partitioning by date and timeframe, tracks progress and errors.
"""

import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from .candle_uploader import CandleUploader, UploadConfig
from ..data_client.data_client import DataClient

logger = logging.getLogger(__name__)

@dataclass
class OrchestrationConfig:
    """Configuration for upload orchestration"""
    max_concurrent_days: int = 5
    max_concurrent_timeframes: int = 3
    retry_failed_days: bool = True
    max_retries: int = 3

class UploadOrchestrator:
    """Orchestrates batch uploads to BigQuery"""
    
    def __init__(
        self, 
        data_client: DataClient, 
        upload_config: UploadConfig,
        orchestration_config: OrchestrationConfig = None
    ):
        self.data_client = data_client
        self.upload_config = upload_config
        self.orchestration_config = orchestration_config or OrchestrationConfig()
        
        self.uploader = CandleUploader(data_client, upload_config)
    
    async def upload_date_range(
        self, 
        start_date: datetime, 
        end_date: datetime,
        timeframes: Optional[List[str]] = None,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Upload candle data for a date range with orchestration
        
        Args:
            start_date: Start date (UTC)
            end_date: End date (UTC)
            timeframes: List of timeframes to upload
            overwrite: Whether to overwrite existing data
            
        Returns:
            Dictionary with orchestration results
        """
        logger.info(f"🚀 Starting orchestrated upload from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        timeframes = timeframes or self.upload_config.timeframes
        results = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'timeframes': timeframes,
            'total_days': 0,
            'successful_days': 0,
            'failed_days': 0,
            'total_rows_uploaded': 0,
            'daily_results': [],
            'timeframe_summary': {tf: {'total_rows': 0, 'days_processed': 0} for tf in timeframes},
            'errors': []
        }
        
        # Calculate total days
        current_date = start_date
        while current_date <= end_date:
            results['total_days'] += 1
            current_date += timedelta(days=1)
        
        # Process days in batches
        current_date = start_date
        day_batch = []
        
        while current_date <= end_date:
            day_batch.append(current_date)
            
            # Process batch when it reaches max size or we're at the end
            if (len(day_batch) >= self.orchestration_config.max_concurrent_days or 
                current_date == end_date):
                
                batch_results = await self._process_day_batch(
                    day_batch, timeframes, overwrite
                )
                
                # Aggregate results
                results['daily_results'].extend(batch_results['daily_results'])
                results['successful_days'] += batch_results['successful_days']
                results['failed_days'] += batch_results['failed_days']
                results['total_rows_uploaded'] += batch_results['total_rows_uploaded']
                results['errors'].extend(batch_results['errors'])
                
                # Update timeframe summary
                for tf, summary in batch_results['timeframe_summary'].items():
                    results['timeframe_summary'][tf]['total_rows'] += summary['total_rows']
                    results['timeframe_summary'][tf]['days_processed'] += summary['days_processed']
                
                # Clear batch
                day_batch = []
            
            current_date += timedelta(days=1)
        
        # Retry failed days if enabled
        if self.orchestration_config.retry_failed_days:
            failed_days = [r for r in results['daily_results'] if r['status'] == 'failed']
            if failed_days:
                logger.info(f"🔄 Retrying {len(failed_days)} failed days")
                retry_results = await self._retry_failed_days(failed_days, timeframes, overwrite)
                
                # Update results with retry data
                results['successful_days'] += retry_results['successful_days']
                results['failed_days'] -= retry_results['successful_days']
                results['total_rows_uploaded'] += retry_results['total_rows_uploaded']
                results['errors'].extend(retry_results['errors'])
        
        logger.info(f"✅ Orchestration completed: {results['successful_days']}/{results['total_days']} days successful")
        return results
    
    async def _process_day_batch(
        self, 
        days: List[datetime], 
        timeframes: List[str], 
        overwrite: bool
    ) -> Dict[str, Any]:
        """Process a batch of days concurrently"""
        
        logger.info(f"📦 Processing batch of {len(days)} days")
        
        # Create tasks for each day
        tasks = []
        for day in days:
            task = self._process_single_day(day, timeframes, overwrite)
            tasks.append(task)
        
        # Execute tasks concurrently
        day_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        results = {
            'daily_results': [],
            'successful_days': 0,
            'failed_days': 0,
            'total_rows_uploaded': 0,
            'timeframe_summary': {tf: {'total_rows': 0, 'days_processed': 0} for tf in timeframes},
            'errors': []
        }
        
        for i, day_result in enumerate(day_results):
            day = days[i]
            
            if isinstance(day_result, Exception):
                # Handle exception
                error_msg = f"Failed to process {day.strftime('%Y-%m-%d')}: {day_result}"
                logger.error(error_msg)
                
                results['daily_results'].append({
                    'date': day.strftime('%Y-%m-%d'),
                    'status': 'failed',
                    'error': str(day_result),
                    'rows_uploaded': 0
                })
                results['failed_days'] += 1
                results['errors'].append(error_msg)
            else:
                # Process successful result
                results['daily_results'].append(day_result)
                
                if day_result['status'] == 'success':
                    results['successful_days'] += 1
                    results['total_rows_uploaded'] += day_result['rows_uploaded']
                    
                    # Update timeframe summary
                    for tf, tf_result in day_result['timeframe_results'].items():
                        results['timeframe_summary'][tf]['total_rows'] += tf_result['rows_uploaded']
                        results['timeframe_summary'][tf]['days_processed'] += 1
                else:
                    results['failed_days'] += 1
                    results['errors'].extend(day_result.get('errors', []))
        
        return results
    
    async def _process_single_day(
        self, 
        day: datetime, 
        timeframes: List[str], 
        overwrite: bool
    ) -> Dict[str, Any]:
        """Process a single day with all timeframes"""
        
        logger.info(f"📅 Processing {day.strftime('%Y-%m-%d')}")
        
        try:
            # Upload all timeframes for this day
            upload_result = await self.uploader.upload_day(day, timeframes, overwrite)
            
            # Calculate total rows uploaded
            total_rows = sum(
                tf_result.get('rows_uploaded', 0) 
                for tf_result in upload_result['timeframes'].values()
            )
            
            return {
                'date': day.strftime('%Y-%m-%d'),
                'status': 'success' if not upload_result['errors'] else 'partial',
                'rows_uploaded': total_rows,
                'timeframe_results': upload_result['timeframes'],
                'errors': upload_result['errors']
            }
            
        except Exception as e:
            logger.error(f"Failed to process {day.strftime('%Y-%m-%d')}: {e}")
            return {
                'date': day.strftime('%Y-%m-%d'),
                'status': 'failed',
                'rows_uploaded': 0,
                'timeframe_results': {},
                'errors': [str(e)]
            }
    
    async def _retry_failed_days(
        self, 
        failed_days: List[Dict[str, Any]], 
        timeframes: List[str], 
        overwrite: bool
    ) -> Dict[str, Any]:
        """Retry failed days with exponential backoff"""
        
        results = {
            'successful_days': 0,
            'total_rows_uploaded': 0,
            'errors': []
        }
        
        for day_result in failed_days:
            date_str = day_result['date']
            day = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            
            for attempt in range(self.orchestration_config.max_retries):
                try:
                    logger.info(f"🔄 Retrying {date_str} (attempt {attempt + 1})")
                    
                    # Add exponential backoff
                    if attempt > 0:
                        await asyncio.sleep(2 ** attempt)
                    
                    # Retry the day
                    retry_result = await self._process_single_day(day, timeframes, overwrite)
                    
                    if retry_result['status'] == 'success':
                        results['successful_days'] += 1
                        results['total_rows_uploaded'] += retry_result['rows_uploaded']
                        logger.info(f"✅ Retry successful for {date_str}")
                        break
                    else:
                        logger.warning(f"⚠️ Retry {attempt + 1} failed for {date_str}")
                        
                except Exception as e:
                    error_msg = f"Retry {attempt + 1} failed for {date_str}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
                    
                    if attempt == self.orchestration_config.max_retries - 1:
                        logger.error(f"❌ All retries failed for {date_str}")
        
        return results
    
    async def get_upload_status(
        self, 
        start_date: datetime, 
        end_date: datetime,
        timeframes: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Get status of uploaded data in BigQuery"""
        
        timeframes = timeframes or self.upload_config.timeframes
        status = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'timeframes': {},
            'total_days_checked': 0,
            'days_with_data': 0
        }
        
        # Check each timeframe
        for timeframe in timeframes:
            table_id = f"{self.upload_config.project_id}.{self.upload_config.dataset_id}.candles_{timeframe.replace('m', 'min').replace('h', 'hour').replace('s', 'sec')}"
            
            try:
                # Query for data in date range
                query = f"""
                SELECT 
                    DATE(timestamp) as date,
                    COUNT(*) as row_count
                FROM `{table_id}`
                WHERE DATE(timestamp) BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
                GROUP BY DATE(timestamp)
                ORDER BY date
                """
                
                query_job = self.uploader.bq_client.query(query)
                results = query_job.result()
                
                timeframe_data = {}
                for row in results:
                    date_str = row.date.strftime('%Y-%m-%d')
                    timeframe_data[date_str] = row.row_count
                    status['days_with_data'] += 1
                
                status['timeframes'][timeframe] = {
                    'days_with_data': len(timeframe_data),
                    'total_rows': sum(timeframe_data.values()),
                    'daily_breakdown': timeframe_data
                }
                
            except Exception as e:
                logger.warning(f"Failed to check status for {timeframe}: {e}")
                status['timeframes'][timeframe] = {
                    'error': str(e)
                }
        
        # Calculate total days checked
        current_date = start_date
        while current_date <= end_date:
            status['total_days_checked'] += 1
            current_date += timedelta(days=1)
        
        return status
