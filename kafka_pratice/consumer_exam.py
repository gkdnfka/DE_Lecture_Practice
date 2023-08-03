from confluent_kafka import Consumer

if __name__ == "__main__":
    consumer = Consumer({
        "bootstrap.servers": ("Bootstrap Severs IP or Domain Name"),
        "group.id": "test_group1",
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe(topics=["test"])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer Error: {msg.error()}")
            continue

        print(f"받은 메세지: {msg.value().decode('utf-8')}")

    consumer.close()