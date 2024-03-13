from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition

from project.common.entities import Singleton

class KafkaConsumer(metaClass=Singleton):
    def __init__(self, bootstrap_servers: str, group_id: str, topic: str, auto_commit: bool = False):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': auto_commit
        })
        self.topic = topic
        self.consumer.subscribe([self.topic])

    # def consume(self):
    #     try:
    #         while True:
    #             msg = self.consumer.poll(1.0)
    #             if msg is None:
    #                 continue
    #             if msg.error():
    #                 if msg.error().code() == KafkaError._PARTITION_EOF:
    #                     continue
    #                 else:
    #                     print(msg.error())
    #                     break
    #             print('Received message: {}'.format(msg.value().decode('utf-8')))
    #     except KeyboardInterrupt:
    #         pass
    #     finally:
    #         self.consumer.close()
        
    def commit_offsets(self, offsets: dict):
        offsets = [TopicPartition(self.topic, partition, offset) for partition, offset in offsets.items()]
        self.consumer.commit(offsets, asynchronous = True)
