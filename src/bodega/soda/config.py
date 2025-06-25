"""
Configuration management for doc_store library.

This module handles environment variables, AWS credentials, and default settings
for the document storage system.
"""

import os
from typing import Optional, Dict, Any
from pathlib import Path

from pydantic import Field, validator
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import boto3
from loguru import logger


class DocStoreConfig(BaseSettings):
    """Configuration settings for doc_store library."""
    
    # AWS Configuration
    aws_region: str = Field(default="us-east-1", env="AWS_REGION")
    aws_access_key_id: Optional[str] = Field(default=None, env="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: Optional[str] = Field(default=None, env="AWS_SECRET_ACCESS_KEY")
    aws_session_token: Optional[str] = Field(default=None, env="AWS_SESSION_TOKEN")
    
    # S3 Configuration
    doc_bucket: str = Field(env="DOC_BUCKET")
    doc_bucket_dev: Optional[str] = Field(default=None, env="DOC_BUCKET_DEV")
    
    # Cache Configuration
    cache_ttl_seconds: int = Field(default=3600, env="CACHE_TTL_SECONDS")
    enable_redis_cache: bool = Field(default=False, env="ENABLE_REDIS_CACHE")
    redis_url: Optional[str] = Field(default=None, env="REDIS_URL")
    
    # Processing Configuration
    processing_timeout_mins: int = Field(default=10, env="PROCESSING_TIMEOUT_MINS")
    health_check_interval: int = Field(default=300, env="HEALTH_CHECK_INTERVAL")
    
    # Logging Configuration
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_format: str = Field(
        default="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
        env="LOG_FORMAT"
    )
    
    # Environment
    environment: str = Field(default="dev", env="ENVIRONMENT")
    
    class Config:
        """Pydantic configuration for DocStoreConfig."""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "allow"  # Allow extra fields for flexibility
    
    @validator("doc_bucket")
    def validate_bucket_name(cls, v):
        """Validate S3 bucket name format."""
        if not v:
            raise ValueError("DOC_BUCKET environment variable is required")
        
        # Basic S3 bucket name validation
        if len(v) < 3 or len(v) > 63:
            raise ValueError("Bucket name must be between 3 and 63 characters")
        
        if not v.replace("-", "").replace(".", "").isalnum():
            raise ValueError("Bucket name must contain only alphanumeric characters, hyphens, and periods")
        
        return v.lower()
    
    @validator("log_level")
    def validate_log_level(cls, v):
        """Validate log level."""
        valid_levels = ["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of: {valid_levels}")
        return v.upper()
    
    @validator("environment")
    def validate_environment(cls, v):
        """Validate environment."""
        valid_envs = ["dev", "test", "staging", "prod"]
        if v.lower() not in valid_envs:
            raise ValueError(f"Environment must be one of: {valid_envs}")
        return v.lower()
    
    def get_bucket_name(self) -> str:
        """Get the appropriate bucket name based on environment."""
        if self.environment == "dev" and self.doc_bucket_dev:
            return self.doc_bucket_dev
        return self.doc_bucket
    
    def get_aws_credentials(self) -> Dict[str, Optional[str]]:
        """Get AWS credentials as a dictionary."""
        return {
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "aws_session_token": self.aws_session_token,
            "region_name": self.aws_region,
        }
    
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == "prod"
    
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment == "dev"


def load_config(config_file: Optional[str] = None) -> DocStoreConfig:
    """
    Load configuration from environment variables and optional config file.
    
    Args:
        config_file: Optional path to .env file
        
    Returns:
        DocStoreConfig instance
        
    Raises:
        ConfigurationError: If required configuration is missing or invalid
    """
    from .exceptions import ConfigurationError
    
    # Load environment file if specified or if default exists
    if config_file:
        if not Path(config_file).exists():
            raise ConfigurationError(f"Config file not found: {config_file}")
        load_dotenv(config_file)
    elif Path(".env").exists():
        load_dotenv(".env")
    
    try:
        config = DocStoreConfig()
        logger.info(f"Configuration loaded successfully for environment: {config.environment}")
        return config
    except Exception as e:
        raise ConfigurationError(f"Failed to load configuration: {str(e)}")


def validate_aws_credentials(config: DocStoreConfig) -> bool:
    """
    Validate AWS credentials by attempting to create an S3 client.
    
    Args:
        config: DocStoreConfig instance
        
    Returns:
        bool: True if credentials are valid
        
    Raises:
        ConfigurationError: If credentials are invalid
    """
    from .exceptions import ConfigurationError
    
    try:
        credentials = config.get_aws_credentials()
        # Filter out None values
        credentials = {k: v for k, v in credentials.items() if v is not None}
        
        # Try to create S3 client
        s3_client = boto3.client('s3', **credentials)
        
        # Test credentials by listing buckets
        s3_client.list_buckets()
        
        logger.info("AWS credentials validated successfully")
        return True
        
    except Exception as e:
        raise ConfigurationError(f"Invalid AWS credentials: {str(e)}")


def setup_logging(config: DocStoreConfig) -> None:
    """
    Setup logging configuration based on config settings.
    
    Args:
        config: DocStoreConfig instance
    """
    # Remove default handler
    logger.remove()
    
    # Add new handler with custom format
    logger.add(
        sink=lambda message: print(message, end=""),
        format=config.log_format,
        level=config.log_level,
        colorize=True,
    )
    
    # Add file logging in production
    if config.is_production():
        logger.add(
            sink="logs/doc_store.log",
            format=config.log_format,
            level=config.log_level,
            rotation="10 MB",
            retention="30 days",
            compression="gz",
        )


# Global configuration instance
_config: Optional[DocStoreConfig] = None


def get_config() -> DocStoreConfig:
    """
    Get the global configuration instance.
    
    Returns:
        DocStoreConfig: Global configuration instance
    """
    global _config
    if _config is None:
        _config = load_config()
        setup_logging(_config)
    return _config


def set_config(config: DocStoreConfig) -> None:
    """
    Set the global configuration instance.
    
    Args:
        config: DocStoreConfig instance to set as global
    """
    global _config
    _config = config
    setup_logging(config) 