# Vlad Chevdar | CS410 Data Engineering
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import time
import threading

# Google Cloud project and subscription details
project_id = "mov-data-eng"
subscription_id = "my-sub"
expected_message_count = 5385

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

counter = 0
start = time.time()
done = threading.Event()

# Callback to process messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global counter
    counter += 1
    message.ack()
    
    if counter >= expected_message_count:
        done.set()

# Start listening for messages
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...\n")

# Wait for completion or timeout
done.wait(timeout=30)

end = time.time()
elapsed = end - start

# Clean up the subscriber
streaming_pull_future.cancel()
subscriber.close()

# Print processing stats
print(f"Total messages received: {counter}")
print(f"Time elapsed: {elapsed:.2f} seconds.")

