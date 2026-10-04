import os
import glob
import csv
import json
from google.cloud import pubsub_v1

# Use the service-account JSON already inside mnist/data
files = glob.glob("../mnist/data/*.json")

if not files:
    raise FileNotFoundError("Service account JSON file not found.")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

project_id = "sofe4630-ms2-bayan"
topic_id = "labelsData"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


def to_float_or_none(value):
    value = value.strip()

    if value == "":
        return None

    return float(value)


with open("Labels.csv", mode="r", encoding="utf-8") as file:

    reader = csv.DictReader(file)

    for row in reader:

        row["time"] = to_float_or_none(row["time"])
        row["temperature"] = to_float_or_none(row["temperature"])
        row["humidity"] = to_float_or_none(row["humidity"])
        row["pressure"] = to_float_or_none(row["pressure"])

        message = json.dumps(row).encode("utf-8")

        future = publisher.publish(topic_path, message)
        future.result()

        print(f"Published: {row}")

print("All records were published successfully.")