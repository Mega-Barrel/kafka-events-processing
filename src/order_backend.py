import json
import time

from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 15

# produce events
producer = KafkaProducer(boostrap_servers="localhost:29092")

print("Generating orders after 10 seconds.")
print("Generate one unique order every 10 seconds.")

for i in range(1, ORDER_LIMIT):
    data = {
        "order_id": i,
        "user_id": f"user_{i}",
        "total_cost": i * 5,
        "items": "burger, sandwich"
    }

    producer.send(
        ORDER_KAFKA_TOPIC,
        json.dumps(data).encode("utf-8")
    )
    
    print(f"Done sending...{i}")
    time.sleep(10)