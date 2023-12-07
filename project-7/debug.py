from kafka import KafkaConsumer
import json
import report_pb2

broker = 'localhost:9092'

consumer = KafkaConsumer("temperatures",
    group_id="debug",
    bootstrap_servers=[broker],
    enable_auto_commit=False)

for message in consumer:
    # Deserialize protobuf message
    report_message = report_pb2.Report()
    report_message.ParseFromString(message.value)

    # Print message details as a dictionary
    message_dict = {
        'partition': message.partition,
        'key': message.key.decode('utf-8'),
        'date': report_message.date,
        'degrees': report_message.degrees
    }

    print(message_dict)