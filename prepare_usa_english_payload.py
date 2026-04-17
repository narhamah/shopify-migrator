#!/usr/bin/env python3
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from tara_migrate.tools.prepare_usa_english_payload import main

if __name__ == "__main__":
    main()
