import time
import os
from datetime import datetime

START_TIME = time.time()
TOTAL_COLLECTED = 0

def record_batch(count):
    global TOTAL_COLLECTED
    TOTAL_COLLECTED += count

def summary():
    duration = time.time() - START_TIME
    return TOTAL_COLLECTED, duration

def send_alert(message):
    os.makedirs("gdrive_alert", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    alert_file = f"gdrive_alert/ALERT_{timestamp}.txt"
    with open(alert_file, "w", encoding="utf-8") as f:
        f.write(message)
