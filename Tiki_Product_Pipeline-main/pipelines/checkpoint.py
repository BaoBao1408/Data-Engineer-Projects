import os
import json

def checkpoint_path():
    os.makedirs("checkpoints", exist_ok=True)
    return "checkpoints/progress.json"

def load_checkpoint():
    if not os.path.exists(checkpoint_path()):
        return {"last_batch": 0}

    with open(checkpoint_path(), "r") as f:
        return json.load(f)

def save_checkpoint(batch_index):
    with open(checkpoint_path(), "w") as f:
        json.dump({"last_batch": batch_index}, f)
