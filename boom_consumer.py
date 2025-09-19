import time

from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': f'umn_boom_kafka_consumer_group_{int(time.time())}',
    'auto.offset.reset': 'earliest',
})
topic = 'LSST_alerts_results'
consumer.subscribe([topic])

def consume():
    print("Listening for messages...")
    cpt=0
    try:
        while True:
            msg = consumer.poll(10.0)
            if msg is None:
                print(f"No {'more ' if cpt > 0 else ''}messages available, exiting")

                break
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            cpt+=1

    except KeyboardInterrupt:
        pass
    finally:
        print(f"Processed {cpt} messages")
        consumer.close()

if __name__ == "__main__":
    consume()