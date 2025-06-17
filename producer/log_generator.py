import random
import time
import os
from datetime import datetime
from kafka import KafkaProducer

kafka_server = "broker:29092"
kafka_topic = os.getenv('KAFKA_TOPIC')

producer = KafkaProducer(bootstrap_servers=kafka_server)

agents = ['Mozilla/5.0', 'Chrome/91.0', 'Safari/537.36', 'Edge/91.0']
ips = [f"192.168.0.{i}" for i in range(1, 10)]

while True:
    log = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')},{random.choice(ips)},{random.choice(agents)}"
    producer.send(kafka_topic, log.encode('utf-8'))
    print(f"Sent: {log}")
    time.sleep(5)
