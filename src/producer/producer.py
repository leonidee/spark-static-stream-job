import json
import sys
import uuid
from datetime import datetime
import s3fs
import dotenv

import pandas as pd
from kafka import KafkaProducer


def create_producer():
    return KafkaProducer(
        bootstrap_servers="158.160.78.165:9092",
    )


def send_messages(df, topic_name):
    print(f"Sending messages to kafka")
    producer = create_producer()

    j = 0
    for row in df.iterrows():
        client_id = str(uuid.uuid1())
        polyline = json.loads(row[1]["polyline"])

        send_polyline(client_id, polyline, producer, topic_name)

        if j % 20 == 0:
            send_polyline(client_id, polyline, producer, topic_name)
        j += 1
        print(f"send {len(polyline)} points for user {client_id}")
        producer.flush()
    producer.close()

    print(f"Message has been sent")


def send_polyline(client_id, polyline, producer, topic_name):
    i = 0
    for coordinate in polyline:
        lat = coordinate[0]
        lon = coordinate[1]
        timestamp = datetime.timestamp(datetime.now())
        message = {
            "client_id": client_id,
            "timestamp": timestamp,
            "lat": lat,
            "lon": lon,
        }
        message_json = json.dumps(message).encode("utf-8")
        producer.send(topic=topic_name, value=message_json)
        i += 1
        if i % 10 == 0:
            producer.send(topic=topic_name, value=message_json)


def read_data(file_name):
    import os

    dotenv.find_dotenv()
    dotenv.load_dotenv()

    s3 = s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url=os.getenv("AWS_ENDPOINT_URL")
    )
    with s3.open(file_name, "r") as f:
        df = pd.read_csv(f)

    return df

def main():
    topic_name = "base"
    file_name = "data-ice-lake-05/master/data/source/spark-statis-stream/user-data/routes.csv"

    df = read_data(file_name)
    send_messages(df, topic_name)

if __name__ == "__main__":
    main()