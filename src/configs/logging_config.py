"""
Logging configuration for PySpark applications
"""

import logging
import sys
from typing import Optional


def setup_logging(level: str = "INFO", 
                 format_string: Optional[str] = None) -> logging.Logger:
    """
    Set up logging configuration
    
    Args:
        level: Logging level (DEBUG, INFO, WARN, ERROR)
        format_string: Custom format string
        
    Returns:
        Logger: Configured logger
    """
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_string,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("app.log", mode="a")
        ]
    )
    
    # Reduce Spark logging verbosity
    logging.getLogger("py4j").setLevel(logging.WARN)
    logging.getLogger("pyspark").setLevel(logging.WARN)
    
    return logging.getLogger(__name__)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name"""
    return logging.getLogger(name)
