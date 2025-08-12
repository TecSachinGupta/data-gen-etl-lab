"""Configuration utilities"""

import os
import yaml
from typing import Dict, Any
from pathlib import Path


def load_yaml_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Configuration dictionary
    """
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def get_config(config_name: str = None) -> Dict[str, Any]:
    """
    Get configuration based on environment.
    
    Args:
        config_name: Specific config file name
        
    Returns:
        Configuration dictionary
    """
    if config_name is None:
        env = os.getenv("ENV", "development")
        config_name = f"{env}.yaml"
    
    config_path = Path("src/configs/environments") / config_name
    
    if config_path.exists():
        return load_yaml_config(str(config_path))
    else:
        # Return default config
        return {
            "spark": {
                "app_name": "DefaultApp",
                "master": "local[*]"
            },
            "data": {
                "input_path": "assets/data/sample/",
                "output_path": "tmp/output/"
            }
        }
