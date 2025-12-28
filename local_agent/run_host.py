#!/usr/bin/env python3
"""
Wrapper script to run host.py with Python.
Required because native messaging on Windows needs an executable.
"""
import os
import sys
import subprocess

# Get the directory containing this script
script_dir = os.path.dirname(os.path.abspath(__file__))
host_script = os.path.join(script_dir, "host.py")

# Run host.py with Python
subprocess.run([sys.executable, host_script], stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr)
