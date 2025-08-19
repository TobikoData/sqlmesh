#!/bin/bash

# Install node dependencies
sudo -l "$(whoami)" -c "pnpm install"

# Install python dependencies
sudo -l "$(whoami)" -c "pip install -e '.[dev]'"
