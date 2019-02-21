import time
from kafka import KafkaProducer
from kafka import KafkaClient
kafa_host = "localhost:9092"

topic='spark_streaming_test'

class KafkaSender():

    def __init__(self):
        self.client=KafkaClient(hosts)
        self.producer=KafkaProducer(bootstrap_servers=hosts)
        self.client.ensure_topic_exists(topic)
    def send_messages(self,msg):
        self.producer.send(topic,msg)
        self.producer.flush()

def get_instance():
    return KafkaSender()

def main():
    producer=get_instance()
    while True:
        data_file = open("/root/test_data/part-00000",'r')
        for l in data_file:
            producer.send_messages(l)
            print l



if __name__ == "__main__":
    main()