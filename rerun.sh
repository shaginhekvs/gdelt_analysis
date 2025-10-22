#!/bin/bash

# Activate the existing environment
source .venv/bin/activate

# Set DATA_DIR for testing
export DATA_DIR="/usr/home/opc/gdelt_data"

# Create directories
mkdir -p "$DATA_DIR"
mkdir -p "$DATA_DIR/cache"
mkdir -p "$DATA_DIR/cache/full_text"

# Check if synthetic-data-kit is installed (optional, since it's already in the env)
if ! python -c "import synthetic_data_kit" 2>/dev/null; then
    echo "Warning: synthetic_data_kit not found. Install if needed."
fi

# Set OpenRouter API key if available (user needs to set this)
# export openrouterKey="your_api_key_here"

echo "Environment activated. DATA_DIR set to $DATA_DIR"
echo "Directories created."
echo "To run the test: python gdelt_test.py"
echo "To run the main script: python gdelt.py"
