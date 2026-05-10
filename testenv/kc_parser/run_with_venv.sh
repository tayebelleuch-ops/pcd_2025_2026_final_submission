#!/bin/bash
# Helper script to run python scripts using the testenv venv

# Get the directory of this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
VENV_PYTHON="$DIR/../venv/bin/python3"

if [ ! -f "$VENV_PYTHON" ]; then
    echo "Error: Virtual environment not found at $VENV_PYTHON"
    echo "Please create it first."
    exit 1
fi

# Execute the passed command with the venv python
"$VENV_PYTHON" "$@"
