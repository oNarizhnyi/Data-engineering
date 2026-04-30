from kafka import KafkaConsumer
from datetime import datetime
import json
import csv
import os


def my_deserializer(raw_bytes):

    return json.loads(raw_bytes.decode('utf-8'))



consumer = KafkaConsumer(
    'tweets',
    bootstrap_servers=['kafka:29092'],
    auto_offset_reset='earliest',
    value_deserializer=my_deserializer,
    api_version=(3, 4, 0)
)


for message in consumer:

    data = message.value
    
    author_id = data.get("author_id")
    raw_date_str = data.get("created_at")
    text = data.get("text")
    
    dt = datetime.strptime(raw_date_str, "%a %b %d %H:%M:%S.%f %Y")
    
    filename = dt.strftime("tweets_%d_%m_%Y_%H_%M.csv")

    file_exists = os.path.isfile(filename)
   
    with open(filename, mode='a', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)

        if not file_exists:
            writer.writerow(['author_id', 'created_at', 'text'])
            
        writer.writerow([author_id, raw_date_str, text])