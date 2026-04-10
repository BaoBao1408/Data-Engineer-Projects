import json
import re

def safe_json(x):
    if isinstance(x, str):
        x = x.strip()
        if x.startswith("{") or x.startswith("["):
            try:
                return json.loads(x)
            except:
                return []
    return x


def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        return [x]
    return []


def ensure_dict(x):
    if isinstance(x, dict):
        return x
    return {}


def parse_float(x):
    if x in [None, "", "null", "None"]:
        return None
    try:
        x = re.sub(r"[^\d.]", "", str(x))
        return float(x) if x else None
    except:
        return None


def safe_str(x):
    if x in [None, "null", "None"]:
        return None
    return str(x)