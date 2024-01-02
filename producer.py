from kafka import KafkaProducer
from json import dumps
from watchdog.observers import Observer # 실시간 변화 감지
from watchdog.events import FileSystemEventHandler # 실시간 변화 감지
import time

class FileWatcher(FileSystemEventHandler):
    def __init__(self, filename, producer, topic): # 초기화 함수
        self.filename = filename
        self.producer = producer
        self.topic = topic
        self.last_sent_line_num = 0

    def on_modified(self, event): # 변화 감지 함수(event 매개변수를 이용)
        if event.src_path == self.filename: # 파일 경로가 옳다면
            with open(self.filename, 'r') as file: # file open
                lines = file.readlines() # file read
                
                # start를 통해 반환하기 시작할 위치를 정하고, enumerate를 통해 인덱스(i)와 값(line)을 모두 반환
                for i, line in enumerate(lines[self.last_sent_line_num:], start=self.last_sent_line_num):
                    if line.strip(): # 빈 줄 건너뛰기
                        self.producer.send(self.topic, value={'str': line.strip()}) # kafka에 send
                        print(f"Sent to Kafka: {line.strip()}") # send 잘 됐는지 확인하는 출력문
                    self.last_sent_line_num = i + 1 # 즉 last_sent_line_num은 data.txt에서의 인덱스 역할을 수행

producer = KafkaProducer(acks=0, # 메시지 받은 사람이 메시지를 잘 받았는지 체크하는 옵션 (0은 그냥 보내기만 하고 확인x)co
                         compression_type='gzip', # 메시지를 gzip 형태로 압축하여 전송
                         bootstrap_servers=['localhost:9092'], # write 할 kafka broker host
                         value_serializer=lambda x: dumps(x).encode('utf-8')) # 직렬화: 데이터 전송을 위해 dumps 함수 이용해서 byte단위로 바꿔줌 
                                                            # dumps: json 값을 encoding을 통해 메모리에 올림
                                                            # x가 있으면 x를 dumps로 바꾸고 encode한다.

# 파일 감시자 설정 및 시작
path_to_watch = './data.txt' # 경로 설정
observer = Observer()
event_handler = FileWatcher(path_to_watch, producer, 'test') # file, producer, topic name
observer.schedule(event_handler, path='.', recursive=False)
observer.start()

try:
    while True:
        time.sleep(1) # 무한 루프로 계속 돌게 함
except KeyboardInterrupt: # ex) ctrl c
    observer.stop()

observer.join() # observer 스레드가 종료될때까지 대기
producer.flush()