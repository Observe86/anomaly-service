import json
import random

def detect_anomaly(raw_data: str):
    # Parse event
    data = json.loads(raw_data)

    # TODO: use real ML model here
    score = random.uniform(-1, 1)
    is_anomaly = score < -0.5

    return is_anomaly, score