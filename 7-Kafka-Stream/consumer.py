import os
import json
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import report_pb2
import sys
from datetime import datetime

def write_atomic_json(path, data):
    path2 = path + ".tmp"
    with open(path2, "w") as f:
        json.dump(data, f, indent=2)
    os.rename(path2, path)

def process_message(message, partition_data):
    # Get report message
    report_message = report_pb2.Report()
    report_message.ParseFromString(message.value)

    date = report_message.date
    degrees = report_message.degrees


    month = datetime.strptime(date, '%Y-%m-%d').strftime('%B')
    year = date.split('-')[0]
    #print(month)

    if month not in partition_data:
        partition_data[month] = {}

    if year not in partition_data[month]:
        partition_data[month][year] = {
            'count': 0,
            'sum': 0,
            'avg': 0,
            'end': "",
            'start': date
        }

    # Ignore duplicates
    if partition_data[month][year]['end'] != "" and date <= partition_data[month][year]['end']:
        print(f"Skipping duplicate date: {date}")
        return

    partition_data[month][year]['count'] += 1
    partition_data[month][year]['sum'] += degrees
    partition_data[month][year]['avg'] = partition_data[month][year]['sum'] / partition_data[month][year]['count']
    partition_data[month][year]['end'] = date

    if month == "January" and year == "1990":
        partition_data[month][year]['start'] = "1990-01-01"
        partition_data[month][year]['count'] = 31
    
partition_nums = []
for partition_num in sys.argv[1:]:
    partition_nums.append(partition_num)

broker = 'localhost:9092'

consumer = KafkaConsumer(
        bootstrap_servers=[broker],
        group_id="stats",
        enable_auto_commit=False
    )

# assign partitions
partitions = []
for partition in partition_nums:
    partitions.append(TopicPartition("temperatures", int(partition)))
consumer.assign(partitions)

# start partition data
partition_data = {}
for p in partitions:
    partition_data[f"partition-{p.partition}"] = {"partition":p.partition, "offset":0}

# Load the partition data from files
for partition, data in partition_data.items():
    file_path = f'{partition}.json'
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            partition_data[partition] = json.load(file)
    # when a new consumer starts, it must start reading from the last offset
    consumer.seek(TopicPartition("temperatures", int(partition.split('-')[1])), data['offset'])  

# Infinite loop that keeps requesting messages batches for each assigned partitition.
while True:
    for message in consumer:
        partition = f'partition-{message.partition}'
        process_message(message, partition_data[partition])

        # record the current read offset in the file and write partition data to JSON files
        tp = TopicPartition(message.topic, message.partition)
        partition_data[partition]['offset'] = consumer.position(tp)
        write_atomic_json(f'/files/{partition}.json', partition_data[partition])
        