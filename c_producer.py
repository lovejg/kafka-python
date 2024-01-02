from confluent_kafka import Producer
import json
import time # 시간 측정을 위함

def delivery_report(err, msg): # 예외처리 관련
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()}')

# Kafka 설정
config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka 브로커 주소
    'client.id': 'test-producer'
}

# Producer 객체 생성
p = Producer(config)

start = time.time() # 시간 측정하기(start는 측정하기 시작한 시간)

# 10,000개의 메시지 보내기
for i in range(100000):
    message = {'key': i, 'value': f'Test message {i}'}
    p.produce('test', key=str(message['key']), value=json.dumps(message), callback=delivery_report)
    p.poll(0)

# 모든 메시지가 전송될 때까지 대기
p.flush()

print("Message sending completed")

print("elapsed time:", time.time() - start) # 걸린 시간(성능 체크용)