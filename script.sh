#!/bin/bash

# Step 1: Create a virtual environment
echo "Creating virtual environment..."
python -m venv venv
echo "Virtual environment created."

# Step 2: Activate the virtual environment and install requirements
echo "Activating virtual environment and installing requirements..."
source venv/bin/activate
pip install -r requirements.txt
echo "Requirements installed."

# Step 3: Run the main Python script
echo "Running main.py..."
python main.py
