# boom-analysis-tools
Scripts to evaluate BOOM output

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
    The script will connect to the Kafka broker at `localhost:9092` and consumes messages from the `LSST_alerts_results` topic, saving the first message to `first_alert.json` for inspection.