from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"메세지 전송 실패: {err}")
    else:
        print(f"{msg.topic()}[{msg.partition()}]으로 데이터 전송 성공")


if __name__ == '__main__':
    producer = Producer({
        "bootstrap.servers": ("Bootstrap Severs IP or Domain Name"),
        "batch.size": 20000,
        "acks": "all",
        "retries": 1,
        "linger.ms": 1
    })

    for i in range(10):
        list_data = f"전달 메세지: {i}"
        producer.produce(topic="test2", value=list_data, callback=delivery_report)

    producer.flush()