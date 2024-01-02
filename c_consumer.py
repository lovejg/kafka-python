from confluent_kafka import Consumer, KafkaException, KafkaError

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['test'])

try:
    while True:
        msg = c.poll(timeout=1.0)
        if msg is None: continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        print('Received message: {}'.format(msg.value().decode('utf-8')))

except KeyboardInterrupt:
    pass
finally:
    c.close()
