"""Logging utilities"""

import logging
import sys
from datetime import datetime


def setup_logging(
    name: str = "PySpark",
    level: str = "INFO",
    format_string: str = None
) -> logging.Logger:
    """
    Setup logging configuration.
    
    Args:
        name: Logger name
        level: Logging level
        format_string: Custom format string
        
    Returns:
        Configured logger
    """
    if format_string is None:
        format_string = (
            "%(asctime)s - %(name)s - %(levelname)s - "
            "%(filename)s:%(lineno)d - %(message)s"
        )
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_string,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f"logs/{name}_{datetime.now().strftime('%Y%m%d')}.log")
        ]
    )
    
    logger = logging.getLogger(name)
    return logger
