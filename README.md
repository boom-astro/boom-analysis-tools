# boom-analysis-tools
This repository contains a script to consume BOOM Kafka alerts and some filters used on the BOOM brokers.
It also includes a set of tools to manage and analyze BOOM outputs and take appropriate actions.

## Boom Kafka Alerts Consumer

This script consumes messages from the BOOM Kafka broker and deserializes them from Avro.

1. **Requirements**

    Install the required Python packages using:
    ```
    pip install -r requirements.txt
    ```

2. **Usage**

    Run the consumer script:
    ```
    python boom_consumer.py
    ```
    The script will connect to the Kafka broker at `localhost:9092` and consumes messages from the topic specified by the `TOPIC` environment variable, saving the first message to `first_alert.json` for inspection.