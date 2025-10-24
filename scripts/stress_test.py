import requests
import random
import uuid
import time
import argparse
from datetime import datetime

def gen_event(topic_prefix="test", reuse_ids=None):
    if reuse_ids and random.random() < 0.2 and reuse_ids:
        # sometimes reuse an id
        event_id = random.choice(reuse_ids)
    else:
        event_id = str(uuid.uuid4())
        if reuse_ids is not None:
            reuse_ids.append(event_id)
    return {
        "topic": f"{topic_prefix}",
        "event_id": event_id,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "stress_test",
        "payload": {"n": random.randint(1, 1000000)}
    }

def run(url, count=5000, dup_rate=0.2, batch_size=100):
    reuse_ids = []
    start = time.time()
    sent = 0
    while sent < count:
        batch = []
        for _ in range(min(batch_size, count - sent)):
            # with dup_rate chance reuse id
            if random.random() < dup_rate and reuse_ids:
                eid = random.choice(reuse_ids)
                evt = {
                    "topic": "stress",
                    "event_id": eid,
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": "stress_test",
                    "payload": {"info": "dup"}
                }
            else:
                eid = str(uuid.uuid4())
                reuse_ids.append(eid)
                evt = {
                    "topic": "stress",
                    "event_id": eid,
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": "stress_test",
                    "payload": {"info": "unique"}
                }
            batch.append(evt)
            sent += 1
        resp = requests.post(url.rstrip("/") + "/publish", json=batch, timeout=60)
        if resp.status_code != 200:
            print("Publish failed:", resp.status_code, resp.text)
            break
    duration = time.time() - start
    print(f"Sent {sent} events in {duration:.2f} s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--count", type=int, default=5000)
    parser.add_argument("--dup-rate", type=float, default=0.2)
    args = parser.parse_args()
    run(args.url, args.count, args.dup_rate)
