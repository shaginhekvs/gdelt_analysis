#!/bin/bash

# Set DATA_DIR
export DATA_DIR="/usr/home/opc/gdelt_data"

# Create directory
mkdir -p "$DATA_DIR"

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Add uv to PATH
export PATH="$HOME/.cargo/bin:$PATH"

# Create virtual environment
uv venv --python 3.12 --seed

# Activate virtual environment
source .venv/bin/activate

# Install requirements
pip install -r requirement.txt

# Install synthetic-data-kit
pip install synthetic-data-kit

echo "Setup complete. DATA_DIR is set to $DATA_DIR"
echo "To run the Flask app: python app.py"
echo "To run the GDELT script: python gdelt.py"
