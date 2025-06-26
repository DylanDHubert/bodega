#!/usr/bin/env python3
"""
🥪 Sandwich Inspector Configuration
==================================

UI-specific configuration settings for the Sandwich Inspector app.
The main pipeline configuration (API keys, processing settings) is handled by the PB&J config system.
"""

import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

@dataclass
class InspectorConfig:
    """UI-specific configuration settings for the Sandwich Inspector"""
    
    # Inspector-specific UI settings
    max_file_size_mb: int = 100
    supported_formats: list = None
    auto_save_interval: int = 30  # seconds
    
    # UI theme settings
    theme_color: str = "#FFD700"
    items_per_page: int = 10
    
    def __post_init__(self):
        """Initialize default values"""
        if self.supported_formats is None:
            self.supported_formats = ['pdf']

def get_config() -> InspectorConfig:
    """Get the UI-specific application configuration"""
    return InspectorConfig()

# Sandwich-themed messages
SANDWICH_MESSAGES = {
    'processing': [
        "🔥 Grilling your document...",
        "🥪 Assembling the perfect sandwich...",
        "🧈 Spreading the AI butter...",
        "🍇 Adding the data jelly...",
        "👨‍🍳 The chef is working hard..."
    ],
    'success': [
        "🎉 Your sandwich is ready to serve!",
        "✨ Bon appétit! Your document is processed.",
        "🥪 Fresh from the kitchen!",
        "👏 Another masterpiece created!",
    ],
    'errors': [
        "😱 Oops! The kitchen had a mishap.",
        "🔥 Something burned in the oven!",
        "😅 The chef needs a break...",
        "🚨 Kitchen emergency!"
    ]
}

def get_random_message(category: str) -> str:
    """Get a random sandwich-themed message"""
    import random
    messages = SANDWICH_MESSAGES.get(category, ["Working..."])
    return random.choice(messages) 