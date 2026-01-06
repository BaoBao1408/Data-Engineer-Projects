import json
import os

def write_batch(data, batch_index, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    path = f"{output_dir}/products_{batch_index}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
