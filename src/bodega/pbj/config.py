#!/usr/bin/env python3
"""
⚙️ Configuration Management for PB&J Pipeline
============================================

FLEXIBLE CONFIGURATION SYSTEM THAT SUPPORTS:
1. Environment variables (highest priority)
2. Configuration files (config.yaml, config.json)
3. .env files (fallback)
4. Default values (lowest priority)

SUPPORTS MULTIPLE PROJECTS WITH DIFFERENT SETTINGS
"""

import os
import json
import yaml
from pathlib import Path
from typing import Optional, Dict, Any, Union
from dataclasses import dataclass, field

@dataclass
class PipelineConfig:
    """Configuration for the PB&J Pipeline"""
    
    # API KEYS
    llamaparse_api_key: Optional[str] = None
    openai_api_key: Optional[str] = None
    
    # OUTPUT SETTINGS
    output_base_dir: str = "processed_documents"
    create_timestamped_folders: bool = True
    preserve_original_structure: bool = True
    
    # LLAMAPARSE SETTINGS
    use_premium_mode: bool = False
    page_separator: str = "\n---\n"
    max_timeout: int = 180
    
    # OPENAI SETTINGS
    openai_model: str = "gpt-4"
    max_tokens: int = 8000
    
    # PROCESSING SETTINGS
    enable_verbose_logging: bool = True
    save_intermediate_files: bool = True
    
    def __post_init__(self):
        """Load configuration from various sources after initialization"""
        self._load_configuration()
    
    def _load_configuration(self):
        """Load configuration from multiple sources in priority order"""
        
        # 1. ENVIRONMENT VARIABLES (HIGHEST PRIORITY)
        self._load_from_environment()
        
        # 2. CONFIGURATION FILES
        self._load_from_config_files()
        
        # 3. .ENV FILE (FALLBACK)
        self._load_from_env_file()
        
        # 4. VALIDATE REQUIRED SETTINGS
        self._validate_configuration()
    
    def _load_from_environment(self):
        """Load configuration from environment variables"""
        # API KEYS
        if not self.llamaparse_api_key:
            self.llamaparse_api_key = os.getenv("LLAMAPARSE_API_KEY") or os.getenv("LLAMA_CLOUD_API_KEY")
        
        if not self.openai_api_key:
            self.openai_api_key = os.getenv("OPENAI_API_KEY")
        
        # OUTPUT SETTINGS
        env_output_dir = os.getenv("PBJ_OUTPUT_DIR")
        if env_output_dir:
            self.output_base_dir = env_output_dir
        
        # PROCESSING SETTINGS
        if os.getenv("PBJ_PREMIUM_MODE", "").lower() in ("true", "1", "yes"):
            self.use_premium_mode = True
        
        if os.getenv("PBJ_VERBOSE", "").lower() in ("true", "1", "yes"):
            self.enable_verbose_logging = True
    
    def _load_from_config_files(self):
        """Load configuration from config.yaml or config.json files"""
        config_files = [
            "config.yaml",
            "config.yml", 
            "config.json",
            "pbj_config.yaml",
            "pbj_config.json"
        ]
        
        for config_file in config_files:
            config_path = Path(config_file)
            if config_path.exists():
                try:
                    if config_path.suffix.lower() in ('.yaml', '.yml'):
                        with open(config_path, 'r', encoding='utf-8') as f:
                            config_data = yaml.safe_load(f)
                    elif config_path.suffix.lower() == '.json':
                        with open(config_path, 'r', encoding='utf-8') as f:
                            config_data = json.load(f)
                    else:
                        continue
                    
                    # APPLY CONFIGURATION DATA
                    self._apply_config_data(config_data)
                    print(f"📋 LOADED CONFIGURATION FROM: {config_path}")
                    break
                    
                except Exception as e:
                    print(f"⚠️  WARNING: Could not load config from {config_path}: {e}")
    
    def _load_from_env_file(self):
        """Load configuration from .env file as fallback"""
        env_file = Path(".env")
        if env_file.exists():
            try:
                from dotenv import load_dotenv
                load_dotenv(env_file)
                
                # RELOAD ENVIRONMENT VARIABLES AFTER .ENV
                self._load_from_environment()
                print("📋 LOADED CONFIGURATION FROM: .env")
                
            except Exception as e:
                print(f"⚠️  WARNING: Could not load .env file: {e}")
    
    def _apply_config_data(self, config_data: Dict[str, Any]):
        """Apply configuration data to this object"""
        if not isinstance(config_data, dict):
            return
        
        # API KEYS
        if not self.llamaparse_api_key and config_data.get("llamaparse_api_key"):
            self.llamaparse_api_key = config_data["llamaparse_api_key"]
        
        if not self.openai_api_key and config_data.get("openai_api_key"):
            self.openai_api_key = config_data["openai_api_key"]
        
        # OUTPUT SETTINGS
        if config_data.get("output_base_dir"):
            self.output_base_dir = config_data["output_base_dir"]
        
        if "create_timestamped_folders" in config_data:
            self.create_timestamped_folders = config_data["create_timestamped_folders"]
        
        if "preserve_original_structure" in config_data:
            self.preserve_original_structure = config_data["preserve_original_structure"]
        
        # LLAMAPARSE SETTINGS
        if "use_premium_mode" in config_data:
            self.use_premium_mode = config_data["use_premium_mode"]
        
        if "page_separator" in config_data:
            self.page_separator = config_data["page_separator"]
        
        if "max_timeout" in config_data:
            self.max_timeout = config_data["max_timeout"]
        
        # OPENAI SETTINGS
        if "openai_model" in config_data:
            self.openai_model = config_data["openai_model"]
        
        if "max_tokens" in config_data:
            self.max_tokens = config_data["max_tokens"]
        
        # PROCESSING SETTINGS
        if "enable_verbose_logging" in config_data:
            self.enable_verbose_logging = config_data["enable_verbose_logging"]
        
        if "save_intermediate_files" in config_data:
            self.save_intermediate_files = config_data["save_intermediate_files"]
    
    def _validate_configuration(self):
        """Validate that required configuration is present"""
        if not self.llamaparse_api_key:
            raise ValueError(
                "LlamaParse API key is required. Set one of:\n"
                "  - LLAMAPARSE_API_KEY environment variable\n"
                "  - llamaparse_api_key in config.yaml/config.json\n"
                "  - LLAMAPARSE_API_KEY in .env file\n"
                "  - Pass api_key parameter to constructor"
            )
    
    def get_output_path(self, pdf_filename: str, custom_output_dir: Optional[str] = None) -> Path:
        """
        Get the output path for a PDF file
        
        Args:
            pdf_filename: Name of the PDF file being processed
            custom_output_dir: Optional custom output directory override
            
        Returns:
            Path: Full path where the document should be processed
        """
        # DETERMINE BASE OUTPUT DIRECTORY
        if custom_output_dir:
            base_dir = Path(custom_output_dir)
        else:
            base_dir = Path(self.output_base_dir)
        
        # CREATE TIMESTAMPED FOLDER IF ENABLED
        if self.create_timestamped_folders:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            document_name = Path(pdf_filename).stem
            output_path = base_dir / f"{document_name}_{timestamp}"
        else:
            document_name = Path(pdf_filename).stem
            output_path = base_dir / document_name
        
        return output_path
    
    def save_config(self, config_path: Union[str, Path] = "pbj_config.yaml"):
        """Save current configuration to a file"""
        config_path = Path(config_path)
        
        config_data = {
            "llamaparse_api_key": self.llamaparse_api_key,
            "openai_api_key": self.openai_api_key,
            "output_base_dir": self.output_base_dir,
            "create_timestamped_folders": self.create_timestamped_folders,
            "use_premium_mode": self.use_premium_mode,
            "openai_model": self.openai_model,
            "enable_verbose_logging": self.enable_verbose_logging,
            "save_intermediate_files": self.save_intermediate_files,
            "page_separator": self.page_separator,
            "max_timeout": self.max_timeout,
            "max_tokens": self.max_tokens
        }
        
        # REMOVE NONE VALUES
        config_data = {k: v for k, v in config_data.items() if v is not None}
        
        try:
            if config_path.suffix.lower() in ('.yaml', '.yml'):
                with open(config_path, 'w', encoding='utf-8') as f:
                    yaml.dump(config_data, f, default_flow_style=False, indent=2)
            elif config_path.suffix.lower() == '.json':
                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(config_data, f, indent=2, ensure_ascii=False)
            else:
                raise ValueError(f"Unsupported config file format: {config_path.suffix}")
            
            print(f"📋 CONFIGURATION SAVED TO: {config_path}")
            
        except Exception as e:
            print(f"❌ ERROR SAVING CONFIGURATION: {e}")
            raise

def create_config(
    llamaparse_api_key: Optional[str] = None,
    openai_api_key: Optional[str] = None,
    output_base_dir: Optional[str] = None,
    use_premium_mode: bool = False,
    **kwargs
) -> PipelineConfig:
    """
    Create a PipelineConfig with the given parameters
    
    Args:
        llamaparse_api_key: LlamaParse API key
        openai_api_key: OpenAI API key  
        output_base_dir: Base directory for output files
        use_premium_mode: Whether to use LlamaParse Premium mode
        **kwargs: Additional configuration parameters
        
    Returns:
        PipelineConfig: Configured pipeline settings
    """
    config = PipelineConfig()
    
    # OVERRIDE WITH PROVIDED PARAMETERS
    if llamaparse_api_key:
        config.llamaparse_api_key = llamaparse_api_key
    if openai_api_key:
        config.openai_api_key = openai_api_key
    if output_base_dir:
        config.output_base_dir = output_base_dir
    if use_premium_mode:
        config.use_premium_mode = use_premium_mode
    
    # APPLY ADDITIONAL KWARGS
    for key, value in kwargs.items():
        if hasattr(config, key):
            setattr(config, key, value)
    
    return config 