from kafka import KafkaAdminClient,  KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
import time
import weather, report_pb2
from datetime import datetime

broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

producer = KafkaProducer(
    bootstrap_servers=[broker],
    retries=10,  # Retry up to 10 times
    acks='all'  # Wait for all in-sync replicas to receive the data
)

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(3) # Deletion sometimes takes a while to reflect

# TODO: Create topic 'temperatures' with 4 partitions and replication factor = 1
new_topic = NewTopic(name="temperatures", num_partitions=4, replication_factor=1)
admin_client.create_topics([new_topic])
print("Topic 'temperatures' created successfully")

for date, degrees in weather.get_next_weather(delay_sec=0.1):
    # Create a protobuf message
    report_message = report_pb2.Report()
    report_message.date = date
    report_message.degrees = degrees

    date_obj = datetime.strptime(date, '%Y-%m-%d').strftime('%B').encode('utf-8')

    producer.send("temperatures", key=date_obj, value=report_message.SerializeToString())
    #print(f"Sent weather report for {date}")


print("Topics:", admin_client.list_topics())