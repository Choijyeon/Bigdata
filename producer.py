"""
    - kafka.KafkaProducer: Producer 클래스
    - json.dumps(): Python 객체를 JSON 포맷으로 인코딩(= 직렬화)
    - time(): 현재 시간을 출력
"""

from kafka import KafkaProducer
from json import dumps
import time

"""
    KafkaProducer 객체 생성
    - acks=0: Producer의 메시지가 Kafka Cluster 에 잘 도착했는지를 확인하지 않음
    - compression_type: Producer 메시지들을 배치 방식으로 보낼 때의 압축 포맷
    - bootstrap_servers: Kafka Broker 들의 주소 리스트
    - value_serializer: Python Object -> JSON 인코딩 방식

"""
producer = KafkaProducer(
    acks=0,
    compression_type='gzip',
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

"""
    Producer 메시지 10000 개 전송
    - data: Kafka Cluster 에 보낼 메시지
    - KafkaProducer.send(): 보낼 메시지를 해당 Topic과 함께 지정
    - KafkaProducer.flush(): 버퍼에 쌓여있는 메시지를 실제로 전송
"""

start = time.time()

for i in range(10000):
    data = {'str' : 'result'+str(i)}
    producer.send('test', value=data)
    producer.flush()

print("elapsed time:", time.time() - start)
