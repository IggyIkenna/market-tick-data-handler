"""
Performance Monitoring Utilities for Market Data Tick Handler

Provides comprehensive performance monitoring, metrics collection, and
observability features for the market data processing pipeline.
"""

import time
import asyncio
import functools
import logging
import psutil
import threading
from typing import Dict, Any, Optional, List, Callable, Union
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from collections import defaultdict, deque
import json
import sys


@dataclass
class PerformanceMetrics:
    """Performance metrics data structure"""
    operation: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    success: bool = True
    error: Optional[str] = None
    memory_usage_mb: Optional[float] = None
    cpu_percent: Optional[float] = None
    additional_metrics: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if self.end_time and not self.duration:
            self.duration = (self.end_time - self.start_time).total_seconds()


@dataclass
class SystemMetrics:
    """System resource metrics"""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_available_mb: float
    disk_usage_percent: float
    network_io: Dict[str, int]
    process_count: int


class PerformanceMonitor:
    """Centralized performance monitoring"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.metrics_history: deque = deque(maxlen=1000)  # Keep last 1000 metrics
        self.system_metrics_history: deque = deque(maxlen=100)  # Keep last 100 system metrics
        self.operation_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            'count': 0,
            'total_duration': 0.0,
            'min_duration': float('inf'),
            'max_duration': 0.0,
            'success_count': 0,
            'error_count': 0,
            'last_execution': None
        })
        self._monitoring_active = False
        self._monitoring_thread: Optional[threading.Thread] = None
    
    def start_monitoring(self, interval: int = 30):
        """Start background system monitoring"""
        if self._monitoring_active:
            return
        
        self._monitoring_active = True
        self._monitoring_thread = threading.Thread(
            target=self._monitor_system_resources,
            args=(interval,),
            daemon=True
        )
        self._monitoring_thread.start()
        self.logger.info(f"Started system monitoring with {interval}s interval")
    
    def stop_monitoring(self):
        """Stop background system monitoring"""
        self._monitoring_active = False
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=5)
        self.logger.info("Stopped system monitoring")
    
    def _monitor_system_resources(self, interval: int):
        """Monitor system resources in background thread"""
        while self._monitoring_active:
            try:
                metrics = self._collect_system_metrics()
                self.system_metrics_history.append(metrics)
                
                # Log high resource usage
                if metrics.cpu_percent > 80:
                    self.logger.warning(f"High CPU usage: {metrics.cpu_percent:.1f}%")
                if metrics.memory_percent > 85:
                    self.logger.warning(f"High memory usage: {metrics.memory_percent:.1f}%")
                
                time.sleep(interval)
            except Exception as e:
                self.logger.error(f"Error in system monitoring: {e}")
                time.sleep(interval)
    
    def _collect_system_metrics(self) -> SystemMetrics:
        """Collect current system metrics"""
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        network_io = psutil.net_io_counters()._asdict()
        
        return SystemMetrics(
            timestamp=datetime.now(timezone.utc),
            cpu_percent=cpu_percent,
            memory_percent=memory.percent,
            memory_available_mb=memory.available / (1024 * 1024),
            disk_usage_percent=disk.percent,
            network_io=network_io,
            process_count=len(psutil.pids())
        )
    
    def record_operation(
        self,
        operation: str,
        duration: float,
        success: bool = True,
        error: Optional[str] = None,
        **additional_metrics
    ) -> PerformanceMetrics:
        """Record performance metrics for an operation"""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(seconds=duration)
        
        # Get current system metrics
        system_metrics = self._collect_system_metrics()
        
        metrics = PerformanceMetrics(
            operation=operation,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            success=success,
            error=error,
            memory_usage_mb=system_metrics.memory_percent * system_metrics.memory_available_mb / 100,
            cpu_percent=system_metrics.cpu_percent,
            additional_metrics=additional_metrics
        )
        
        # Store metrics
        self.metrics_history.append(metrics)
        
        # Update operation statistics
        stats = self.operation_stats[operation]
        stats['count'] += 1
        stats['total_duration'] += duration
        stats['min_duration'] = min(stats['min_duration'], duration)
        stats['max_duration'] = max(stats['max_duration'], duration)
        stats['last_execution'] = end_time
        
        if success:
            stats['success_count'] += 1
        else:
            stats['error_count'] += 1
        
        # Log performance metrics
        self._log_performance_metrics(metrics)
        
        return metrics
    
    def _log_performance_metrics(self, metrics: PerformanceMetrics):
        """Log performance metrics with structured information"""
        log_data = {
            'operation': metrics.operation,
            'duration_seconds': metrics.duration,
            'success': metrics.success,
            'memory_usage_mb': metrics.memory_usage_mb,
            'cpu_percent': metrics.cpu_percent,
            'additional_metrics': metrics.additional_metrics
        }
        
        if metrics.error:
            log_data['error'] = metrics.error
        
        if metrics.success:
            self.logger.info(
                f"Operation '{metrics.operation}' completed in {metrics.duration:.3f}s",
                extra={'performance_metrics': log_data}
            )
        else:
            self.logger.error(
                f"Operation '{metrics.operation}' failed after {metrics.duration:.3f}s: {metrics.error}",
                extra={'performance_metrics': log_data}
            )
    
    def get_operation_stats(self, operation: Optional[str] = None) -> Dict[str, Any]:
        """Get performance statistics for operations"""
        if operation:
            return self.operation_stats.get(operation, {}).copy()
        
        return {op: stats.copy() for op, stats in self.operation_stats.items()}
    
    def get_recent_metrics(self, count: int = 100) -> List[PerformanceMetrics]:
        """Get recent performance metrics"""
        return list(self.metrics_history)[-count:]
    
    def get_system_metrics_summary(self) -> Dict[str, Any]:
        """Get system metrics summary"""
        if not self.system_metrics_history:
            return {}
        
        recent_metrics = list(self.system_metrics_history)[-10:]  # Last 10 measurements
        
        return {
            'avg_cpu_percent': sum(m.cpu_percent for m in recent_metrics) / len(recent_metrics),
            'avg_memory_percent': sum(m.memory_percent for m in recent_metrics) / len(recent_metrics),
            'avg_memory_available_mb': sum(m.memory_available_mb for m in recent_metrics) / len(recent_metrics),
            'avg_disk_usage_percent': sum(m.disk_usage_percent for m in recent_metrics) / len(recent_metrics),
            'current_process_count': recent_metrics[-1].process_count if recent_metrics else 0,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    def export_metrics(self, filepath: str):
        """Export metrics to JSON file"""
        export_data = {
            'operation_stats': self.operation_stats,
            'recent_metrics': [
                {
                    'operation': m.operation,
                    'start_time': m.start_time.isoformat(),
                    'end_time': m.end_time.isoformat() if m.end_time else None,
                    'duration': m.duration,
                    'success': m.success,
                    'error': m.error,
                    'memory_usage_mb': m.memory_usage_mb,
                    'cpu_percent': m.cpu_percent,
                    'additional_metrics': m.additional_metrics
                }
                for m in self.metrics_history
            ],
            'system_metrics_summary': self.get_system_metrics_summary(),
            'export_timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        self.logger.info(f"Exported performance metrics to {filepath}")


# Global performance monitor instance
_performance_monitor = PerformanceMonitor()


def performance_monitor(operation: str = None, **additional_metrics):
    """Decorator for automatic performance monitoring"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            op_name = operation or f"{func.__module__}.{func.__name__}"
            start_time = time.time()
            success = True
            error = None
            
            try:
                result = await func(*args, **kwargs)
                return result
            except Exception as e:
                success = False
                error = str(e)
                raise
            finally:
                duration = time.time() - start_time
                _performance_monitor.record_operation(
                    op_name, duration, success, error, **additional_metrics
                )
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            op_name = operation or f"{func.__module__}.{func.__name__}"
            start_time = time.time()
            success = True
            error = None
            
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                success = False
                error = str(e)
                raise
            finally:
                duration = time.time() - start_time
                _performance_monitor.record_operation(
                    op_name, duration, success, error, **additional_metrics
                )
        
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def record_performance(operation: str, **additional_metrics):
    """Context manager for recording performance metrics"""
    class PerformanceContext:
        def __init__(self, op_name: str, **metrics):
            self.operation = op_name
            self.start_time = time.time()
            self.additional_metrics = metrics
            self.success = True
            self.error = None
        
        def __enter__(self):
            return self
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            duration = time.time() - self.start_time
            
            if exc_type is not None:
                self.success = False
                self.error = str(exc_val)
            
            _performance_monitor.record_operation(
                self.operation, duration, self.success, self.error, **self.additional_metrics
            )
    
    return PerformanceContext(operation, **additional_metrics)


def get_performance_monitor() -> PerformanceMonitor:
    """Get the global performance monitor instance"""
    return _performance_monitor


def start_performance_monitoring(interval: int = 30):
    """Start global performance monitoring"""
    _performance_monitor.start_monitoring(interval)


def stop_performance_monitoring():
    """Stop global performance monitoring"""
    _performance_monitor.stop_monitoring()


def get_performance_summary() -> Dict[str, Any]:
    """Get performance summary"""
    return {
        'operation_stats': _performance_monitor.get_operation_stats(),
        'system_metrics': _performance_monitor.get_system_metrics_summary(),
        'recent_metrics_count': len(_performance_monitor.metrics_history)
    }


# Convenience functions for common performance monitoring scenarios
def monitor_api_call(operation: str, method: str, url: str, **additional_metrics):
    """Monitor API call performance"""
    return record_performance(
        f"api_call_{operation}",
        api_method=method,
        api_url=url,
        **additional_metrics
    )


def monitor_data_processing(operation: str, item_count: int, **additional_metrics):
    """Monitor data processing performance"""
    return record_performance(
        f"data_processing_{operation}",
        item_count=item_count,
        **additional_metrics
    )


def monitor_file_operation(operation: str, file_path: str, file_size_mb: float, **additional_metrics):
    """Monitor file operation performance"""
    return record_performance(
        f"file_operation_{operation}",
        file_path=file_path,
        file_size_mb=file_size_mb,
        **additional_metrics
    )
