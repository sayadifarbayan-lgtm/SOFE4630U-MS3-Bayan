import argparse
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions


def parse_json(message):
    return json.loads(message.decode("utf-8"))


def filter_none(record):
    return (
        record.get("temperature") is not None
        and record.get("humidity") is not None
        and record.get("pressure") is not None
    )


def convert_units(record):
    record["temperature"] = float(record["temperature"]) * 1.8 + 32
    record["pressure"] = float(record["pressure"]) / 6.895
    return record


def to_json(record):
    return json.dumps(record).encode("utf-8")


def run():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_topic",
        required=True
    )

    parser.add_argument(
        "--output_topic",
        required=True
    )

    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:

        (
            p
            | "ReadFromPubSub"
            >> beam.io.ReadFromPubSub(
                topic=known_args.input_topic
            )

            | "ToDict"
            >> beam.Map(parse_json)

            | "FilterNone"
            >> beam.Filter(filter_none)

            | "ConvertUnits"
            >> beam.Map(convert_units)

            | "ToJson"
            >> beam.Map(to_json)

            | "WriteToPubSub"
            >> beam.io.WriteToPubSub(
                topic=known_args.output_topic
            )
        )


if __name__ == "__main__":
    run()