"""
Configuration management module
"""
import os
import yaml
from typing import Dict, Any, Optional


class ConfigManager:
    """Manages configuration loading and validation"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.config = None
        
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            
            # Validate required settings
            self._validate_config()
            
            # Create directories if they don't exist
            self._create_directories()
            
            return self.config
            
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in configuration file: {e}")
    
    def _validate_config(self) -> None:
        """Validate required configuration settings"""
        required_sections = ['database', 'import', 'directories', 'logging']
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Validate database settings
        db_config = self.config['database']
        
        # Check for either connection string OR individual parameters
        if 'connection_string' in db_config:
            # Using connection string - only validate table_name
            if 'table_name' not in db_config:
                raise ValueError("Missing required database configuration: table_name")
        else:
            # Using individual parameters - validate all fields
            required_db_fields = ['host', 'port', 'database', 'user', 'password', 'table_name']
            for field in required_db_fields:
                if field not in db_config:
                    raise ValueError(f"Missing required database configuration: {field}")
        
        # Validate schema exists (needed for both connection methods)
        if 'schema' not in db_config:
            # Default to public if not specified
            self.config['database']['schema'] = 'public'
    
    def _create_directories(self) -> None:
        """Create necessary directories if they don't exist"""
        directories = [
            self.config['directories']['temp_directory'],
            self.config['directories']['log_directory']
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key"""
        return self.config.get(key, default)