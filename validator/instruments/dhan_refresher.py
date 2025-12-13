import os
import requests
import pandas as pd
from datetime import datetime

DHAN_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

# This is where your CSV will be stored
LOCAL_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "dhan_instruments.csv"
)

META_PATH = LOCAL_PATH.replace(".csv", "_meta.json")


def refresh_dhan_instruments() -> str:
    """
    Downloads the latest Dhan instrument master and saves it locally.
    Returns the local CSV path.
    """
    try:
        resp = requests.get(DHAN_URL, timeout=20)
        resp.raise_for_status()

        # Save CSV
        with open(LOCAL_PATH, "wb") as f:
            f.write(resp.content)

        # Save metadata
        meta = {
            "last_updated": datetime.now().isoformat(),
            "source": DHAN_URL,
        }

        import json
        with open(META_PATH, "w") as f:
            json.dump(meta, f, indent=4)

        return LOCAL_PATH

    except Exception as e:
        raise RuntimeError(f"Failed refreshing Dhan instruments: {e}")
