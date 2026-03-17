import os
import shutil

def reset_pipeline():
    for folder in ["output", "errors", "checkpoints"]:
        if os.path.exists(folder):
            shutil.rmtree(folder)
            print(f"[RESET] Removed {folder}")
