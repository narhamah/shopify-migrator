#!/usr/bin/env python3
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from tara_migrate.fixers.fix_images import main

if __name__ == "__main__":
    main()
