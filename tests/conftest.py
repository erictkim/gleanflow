import os
import sys

# make the repo root importable (gleanflow + examples) when running pytest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
