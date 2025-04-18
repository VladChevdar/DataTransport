# Vlad Chevdar | CS410 Data Engineering
from google.cloud import pubsub_v1
import json, time

project_id = "mov-data-eng"
topic_id = "my-topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

with open("bcsample.json") as f:
    records = json.load(f)

start = time.time()
futures = [
    publisher.publish(topic_path, json.dumps(r).encode("utf-8"))
    for r in records
]

# Wait for all publish operations to complete
for f in futures:
    f.result()

elapsed = time.time() - start
print(f"Published {len(records)} messages in {elapsed:.2f}s")

