#!/bin/bash
set -euo pipefail

create_sqlmesh_venv() {
  local venv_dir=".venv"
  local requirements_file="requirements.txt"
  local custom_mats_dir="../custom_materializations"
  
  echo "Creating Python virtual environment in $venv_dir..."
  if ! python -m venv "$venv_dir"; then
    echo "ERROR: Failed to create virtual environment" >&2
    return 1
  fi
  
  echo "Activating virtual environment..."
  source "$venv_dir/bin/activate" || {
    echo "ERROR: Failed to activate virtual environment" >&2
    return 1
  }
  
  if [ -f "$requirements_file" ]; then
    echo "Installing requirements from $requirements_file..."
    if ! pip install -r "$requirements_file"; then
      echo "ERROR: Failed to install requirements" >&2
      deactivate
      return 1
    fi
  else
    echo "WARNING: $requirements_file not found" >&2
  fi
  
  if [ -d "$custom_mats_dir" ]; then
    echo "Installing custom materializations from $custom_mats_dir..."
    if ! pip install -e "$custom_mats_dir"; then
      echo "ERROR: Failed to install custom materializations" >&2
      deactivate
      return 1
    fi
  else
    echo "ERROR: Custom materializations directory not found at $custom_mats_dir" >&2
    deactivate
    return 1
  fi
  
  echo "Setup completed successfully. Virtual environment is active."
  return 0
}

# Check if the script is being run in a terminal
if [ -t 1 ]; then
  create_sqlmesh_venv
else
  echo "This script should be run in a terminal." >&2
  exit 1
fi