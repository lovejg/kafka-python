from kafka import KafkaProducer
from json import dumps
import time # 시간 측정을 위함

producer = KafkaProducer(acks=0, #메시지 받은 사람이 메시지를 잘 받았는지 체크하는 옵션 (0은 그냥 보내기만 하고 확인x)
                         compression_type='gzip', # 메시지를 gzip 형태로 압축하여 전송
                         bootstrap_servers=['localhost:9092'], # write 할 kafka broker host
                         value_serializer=lambda x: dumps(x).encode('utf-8')) # 직렬화: 데이터 전송을 위해 dumps 함수 이용해서 byte단위로 바꿔줌 
                                                               # dumps: json 값을 encoding을 통해 메모리에 올림
                                                               # x가 있으면 x를 dumps로 바꾸고 encode한다.

start = time.time() # 시간 측정하기(start는 측정하기 시작한 시간)

for i in range(100000): # 총 100000개의 data(message) 보내기
    data = {'str' : 'result'+str(i)} # message 형식
    producer.send('test', value=data) # test라는 topic에 보내기(test topic은 자동생성)
    
    
producer.flush() # 모든 메시지가 전송될 때까지 대기
    
print("elapsed time:", time.time() - start) # 걸린 시간(성능 체크용)