#!/usr/bin/env python3
"""
Entry point for running market_data_tick_handler as a module.

This allows the package to be executed with:
    python -m market_data_tick_handler --mode instruments --start-date 2023-05-23 --end-date 2023-05-25
"""

from .main import main
import asyncio

if __name__ == "__main__":
    asyncio.run(main())
