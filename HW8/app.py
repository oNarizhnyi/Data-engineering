import csv
from datetime import datetime
import time
from kafka import KafkaProducer
import json


with open("twcs.csv", mode="r", encoding="utf-8") as file:
    reader = csv.reader(file)

    next(reader)

    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        api_version=(3, 4, 0)
    )

    for row in reader:
        
        row[3] = datetime.now().strftime('%a %b %d %H:%M:%S.%f %Y')
        twit = {
            "tweet_id": row[0],
            "author_id": row[1],
            "inbound": row[2],
            "created_at": row[3],
            "text": row[4],
            "response_tweet_id": row[5],
            "in_response_to_tweet_id": row[6]

        }
        producer.send('tweets', json.dumps(twit).encode('utf-8'))
        print(f"Twit with id {row[0]} has been sent")

        time.sleep(0.07)
    producer.flush()