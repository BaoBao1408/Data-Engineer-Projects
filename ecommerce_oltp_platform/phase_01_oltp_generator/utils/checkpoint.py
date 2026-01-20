import json
import os

# === Xác định root project ===
BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

# === checkpoints folder ===
CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoints")
CHECKPOINT_FILE = os.path.join(
    CHECKPOINT_DIR,
    "processed_state.json"
)


def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return {
            "orders_generated": 0,
            "last_order_index": 0,
            "order_items_generated": 0,
            "order_items_order_idx": 0,
        }

    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_checkpoint(state: dict):
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
