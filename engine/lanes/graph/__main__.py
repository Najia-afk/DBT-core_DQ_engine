"""python -m engine.lanes.graph <init|ingest|reconcile|export|reset> (see cli.py)."""

import sys

from .cli import main

sys.exit(main())
