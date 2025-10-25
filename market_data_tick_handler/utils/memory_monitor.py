"""
Cross-Platform Memory Monitor

Provides memory monitoring capabilities that work on both Mac (Darwin) and Linux.
Uses psutil for cross-platform compatibility.
"""

import platform
import logging
from typing import Dict, Any, Optional
import psutil

logger = logging.getLogger(__name__)

class MemoryMonitor:
    """Cross-platform memory monitoring with OS detection"""
    
    def __init__(self, threshold_percent: float = 80.0):
        """
        Initialize memory monitor
        
        Args:
            threshold_percent: Memory usage threshold percentage (default: 70%)
        """
        self.threshold_percent = threshold_percent
        self.os_type = platform.system()
        self._validate_platform()
        
        # Get system memory info
        self.total_memory = psutil.virtual_memory().total
        self.total_memory_gb = self.total_memory / (1024**3)
        
        logger.info(f"MemoryMonitor initialized for {self.os_type}")
        logger.info(f"Total system memory: {self.total_memory_gb:.2f} GB")
        logger.info(f"Memory threshold: {self.threshold_percent}%")
    
    def _validate_platform(self):
        """Validate that the platform is supported"""
        supported_platforms = ['Darwin', 'Linux']
        if self.os_type not in supported_platforms:
            raise RuntimeError(f"Unsupported platform: {self.os_type}. Supported platforms: {supported_platforms}")
    
    def get_memory_usage_percent(self) -> float:
        """
        Get current memory usage percentage
        
        Returns:
            float: Memory usage percentage (0-100)
        """
        try:
            memory_info = psutil.virtual_memory()
            return memory_info.percent
        except Exception as e:
            logger.error(f"Failed to get memory usage: {e}")
            return 0.0
    
    def get_memory_usage_bytes(self) -> int:
        """
        Get current memory usage in bytes
        
        Returns:
            int: Memory usage in bytes
        """
        try:
            memory_info = psutil.virtual_memory()
            return memory_info.used
        except Exception as e:
            logger.error(f"Failed to get memory usage bytes: {e}")
            return 0
    
    def get_available_memory_bytes(self) -> int:
        """
        Get available memory in bytes
        
        Returns:
            int: Available memory in bytes
        """
        try:
            memory_info = psutil.virtual_memory()
            return memory_info.available
        except Exception as e:
            logger.error(f"Failed to get available memory: {e}")
            return 0
    
    def is_memory_threshold_exceeded(self, threshold_percent: Optional[float] = None) -> bool:
        """
        Check if memory usage exceeds threshold
        
        Args:
            threshold_percent: Override default threshold (optional)
            
        Returns:
            bool: True if memory usage exceeds threshold
        """
        threshold = threshold_percent or self.threshold_percent
        current_usage = self.get_memory_usage_percent()
        
        exceeded = current_usage >= threshold
        if exceeded:
            logger.warning(f"Memory threshold exceeded: {current_usage:.1f}% >= {threshold}%")
        
        return exceeded
    
    def get_memory_info(self) -> Dict[str, Any]:
        """
        Get comprehensive memory information
        
        Returns:
            Dict containing memory statistics
        """
        try:
            memory_info = psutil.virtual_memory()
            usage_percent = memory_info.percent
            threshold_exceeded = usage_percent >= self.threshold_percent
            
            return {
                'total_bytes': memory_info.total,
                'total_gb': memory_info.total / (1024**3),
                'available_bytes': memory_info.available,
                'available_gb': memory_info.available / (1024**3),
                'used_bytes': memory_info.used,
                'used_gb': memory_info.used / (1024**3),
                'usage_percent': usage_percent,
                'threshold_percent': self.threshold_percent,
                'threshold_exceeded': threshold_exceeded,
                'os_type': self.os_type
            }
        except Exception as e:
            logger.error(f"Failed to get memory info: {e}")
            return {
                'error': str(e),
                'os_type': self.os_type
            }
    
    def log_memory_status(self, context: str = ""):
        """
        Log current memory status
        
        Args:
            context: Additional context for the log message
        """
        info = self.get_memory_info()
        if 'error' not in info:
            context_str = f" [{context}]" if context else ""
            logger.info(f"Memory Status{context_str}: {info['usage_percent']:.1f}% used "
                       f"({info['used_gb']:.2f}GB / {info['total_gb']:.2f}GB)")
            
            if info['threshold_exceeded']:
                logger.warning(f"⚠️ Memory threshold exceeded{context_str}: "
                             f"{info['usage_percent']:.1f}% >= {info['threshold_percent']}%")
        else:
            logger.error(f"Failed to get memory status{context_str}: {info['error']}")

# Global instance for easy access
_memory_monitor: Optional[MemoryMonitor] = None

def get_memory_monitor(threshold_percent: float = 70.0) -> MemoryMonitor:
    """
    Get global memory monitor instance
    
    Args:
        threshold_percent: Memory threshold percentage
        
    Returns:
        MemoryMonitor instance
    """
    global _memory_monitor
    if _memory_monitor is None:
        _memory_monitor = MemoryMonitor(threshold_percent)
    return _memory_monitor

def check_memory_threshold(threshold_percent: float = 70.0) -> bool:
    """
    Quick check if memory threshold is exceeded
    
    Args:
        threshold_percent: Memory threshold percentage
        
    Returns:
        bool: True if threshold exceeded
    """
    monitor = get_memory_monitor(threshold_percent)
    return monitor.is_memory_threshold_exceeded()

def log_memory_status(context: str = ""):
    """
    Quick memory status logging
    
    Args:
        context: Additional context for log message
    """
    monitor = get_memory_monitor()
    monitor.log_memory_status(context)
