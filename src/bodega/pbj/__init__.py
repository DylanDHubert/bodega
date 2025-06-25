"""
🥜🧈🍇🥪 PB&J Pipeline - Parse, Better, JSON
============================================

A delicious document processing pipeline that transforms PDFs into structured JSON.

## The PB&J Stack:
- 🥜 **Peanut** (Parse): PDF → Markdown using LlamaParse
- 🧈 **Butter** (Better): Markdown → Enhanced Markdown using OpenAI  
- 🍇 **Jelly** (JSON): Markdown → Structured JSON using OpenAI
- 🥪 **Sandwich** (Complete): Full pipeline orchestration

## Standardized Interface:
All classes support consistent method naming:
- `process()` - Main processing method
- `process_file()` - Process single file (Butter, Jelly)
- `process_folder()` - Process folder of files (Butter, Jelly)
- `process_async()` - Async processing

## Quick Start:
```python
from pbj import Sandwich

# Complete pipeline (two ways)
sandwich = Sandwich(use_premium=True)
result = sandwich.process("document.pdf")
# or
result = sandwich.make("document.pdf")  # user-friendly alias

# Individual stages
from pbj import Peanut, Butter, Jelly

peanut = Peanut()
parsed = peanut.process("document.pdf")

butter = Butter()
enhanced = butter.process(parsed[0].content)

jelly = Jelly()
json_data = jelly.process(enhanced.enhanced_content)
```

## Fun Aliases:
```python
# For those who like the themed names
from pbj import Parse, Better, JSON, Pipeline

parser = Parse()
enhancer = Better() 
extractor = JSON()
pipeline = Pipeline()

# Use with standardized interface
parsed = parser.process("document.pdf")
enhanced = enhancer.process(parsed[0].content)
json_data = extractor.process(enhanced.enhanced_content)
complete = pipeline.process("document.pdf")  # or pipeline.make()
```
"""

from .peanut import Peanut
from .butter import Butter
from .jelly import Jelly
from .sandwich import Sandwich

__version__ = "1.0.0"
__author__ = "Dylan Hubert"
__email__ = "TBA"

# Fun aliases for the themed experience
Parse = Peanut
Better = Butter
JSON = Jelly
Pipeline = Sandwich

__all__ = [
    # Main classes
    'Peanut', 'Butter', 'Jelly', 'Sandwich',
    # Fun aliases
    'Parse', 'Better', 'JSON', 'Pipeline'
] 