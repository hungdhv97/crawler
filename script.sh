#!/bin/bash

# Step 1: Install Python 3.10
echo "Installing Python 3.10..."
sudo apt-get install python3.10
echo "Python 3.10 installed."

# Step 2: Create a virtual environment
echo "Creating virtual environment..."
python3 -m venv venv
echo "Virtual environment created."

# Step 3: Activate the virtual environment and install requirements
echo "Activating virtual environment and installing requirements..."
source venv/bin/activate
pip install -r requirements.txt
echo "Requirements installed."

# Step 4: Run the main Python script
echo "Running main.py..."
python3 main.py
