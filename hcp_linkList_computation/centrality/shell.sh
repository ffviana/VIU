#!/bin/bash

# Install dependencies in userspace
pip install --user -r requirements.txt

# Launch application with arguments passed to the script
python -u $@ 1>&1 2>&2;

