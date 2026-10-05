# Smart Meter Dataflow Design

This folder contains the design part of Milestone 3.

The smart meter data is published to the `labelsData` Pub/Sub topic. The Dataflow job reads the messages, removes records with missing temperature, humidity, or pressure values, converts the temperature from Celsius to Fahrenheit and the pressure from kPa to psi, and publishes the processed records to the `labelsDataProcessed` topic.

## Files

- `Labels.csv` - Smart meter dataset
- `producer_ms3.py` - Publishes the records to Pub/Sub
- `smartMeterDataflow.py` - Dataflow pipeline used to filter and process the records

## Conversions

Temperature:

`F = C × 1.8 + 32`

Pressure:

`psi = kPa / 6.895`
