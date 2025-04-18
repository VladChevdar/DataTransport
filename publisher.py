# Vlad Chevdar | CS410 Data Engineering
from google.cloud import pubsub_v1
import json, time

# Google Cloud project and topic details
project_id = "mov-data-eng"
topic_id = "my-topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Load records from JSON file
with open("bcsample.json") as f:
    records = json.load(f)

# Publish each record as a message
start = time.time()
futures = [
    publisher.publish(topic_path, json.dumps(r).encode("utf-8"))
    for r in records
]

# Ensure all messages are published
for f in futures:
    f.result()

elapsed = time.time() - start
print(f"Published {len(records)} messages in {elapsed:.2f}s")

