"""
    - kafka.KafkaConsumer: Consumer 클래스
    - json.loads(): JSON 데이터 Python 객체로 디코딩(= 역직렬화)
"""

from kafka import KafkaConsumer
from json import loads


"""
    KafkaConsumer 객체 생성
    - 'test': Consumer 객체가 처리할 Topic
    - bootstrap_servers: Kafka Broker 들의 주소 리스트
    - enable_auto_commit=True: 메시지 Offset 을 일정 주기마다 자동으로 갱신
    - group_id: Consumer Group Id
    - value_deserializer: JSON -> Python Object 디코딩 방식
    - consumer_timeout_ms: Broker와 Consumer의 세션 타임아웃 기간
"""

consumer = KafkaConsumer(
    'test',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     consumer_timeout_ms=1000
)

"""
    Broker 로부터 받은 메시지 출력
"""

print('[begin] get message list')

for message in consumer:
    print("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s" % (
        message.topic, message.partition, message.offset, message.key, message.value
    ))

print('[end] get message list')
