"""
Shared environment loader utility for ETL scripts.
Supports loading .env.local or .env.cloud based on configuration.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv


def load_environment(force_local=False):
    """Load environment variables for ETL scripts.

    Priority:
    1. If force_local=True: Only load .env.local
    2. Otherwise: Try .env.cloud first, fallback to .env.local

    Args:
        force_local: If True, only load .env.local (skip .env.cloud)
    """
    # Get repo root (parent of utils directory)
    utils_dir = Path(__file__).resolve().parent
    repo_root = utils_dir.parent

    cloud_env_path = repo_root / '.env.cloud'
    local_env_path = repo_root / '.env.local'

    if force_local:
        if local_env_path.exists():
            load_dotenv(local_env_path)
            print(f"[INFO] Loaded environment from: {local_env_path} (local mode)")
            return
        else:
            print(f"[ERROR] .env.local not found: {local_env_path}")
            sys.exit(1)

    # Default: prefer cloud, fallback to local
    if cloud_env_path.exists():
        load_dotenv(cloud_env_path)
        print(f"[INFO] Loaded environment from: {cloud_env_path}")
        return

    if local_env_path.exists():
        load_dotenv(local_env_path)
        print(f"[INFO] Loaded environment from: {local_env_path} (fallback)")
        return

    # If neither exists, fail fast with clear message
    print("[ERROR] No environment file found.")
    print(f"  Checked: {cloud_env_path}")
    print(f"  Checked: {local_env_path}")
    sys.exit(1)

