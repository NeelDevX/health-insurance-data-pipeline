# ~/superset_config.py

import os
from superset.config import *  # Load base settings

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "super-secure-neel-key-123!@#")
