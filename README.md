# boom-analysis-tools
This repository contains:
- a Kafka consumer for BOOM alerts
- MongoDB query filters (JSON files) used by the BOOM broker
- tools to manage, analyze, and take appropriate actions on BOOM outputs.

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

## Tools
The repository includes various tools for managing and analyzing BOOM outputs.
These tools can be found in the `tools/` directory.

Requirements for all tools:

Install the required Python packages using:
```
pip install -r tools/requirements.txt
```

The available tools are:

### Plot crossmatch ZTF LSST
This tool generates plots of light curves from crossmatched ZTF and LSST data.
Call in a Python script as follows:
```
plot_crossmatch_ztf_lsst(ztf_alert, filter) 
```
*Where `ztf_alert` is a ZTF alert dictionary and `filter` is the Boom filter information containing LSST ID.*


### Process filtered alerts
This tool consume specified filtered alerts from the BOOM Kafka broker and save them to a specified skyportal group.
Set the `SKYPORTAL_URL`, `SKYPORTAL_API_KEY`, `TOPIC`, `ANNOTATION_ORIGIN` and `FILTER_TO_GROUP_MAP` environment variables.
Then run:
```
python -m tools.process_filtered_alerts
```

### Reprocess alerts
This tool reprocesses alerts in Boom by pushing them back to a Redis queue. You can choose to reprocess only the filtering step, or the enrichment and filtering steps.
Run the script with the desired options:
```
python -m tools.reprocess_alerts --survey-name <ZTF|LSST|DECAM> --reprocess-type <enrichment+filter|filter> --batch-size <number>
```

### Superphot
This tool generates superphot light curves for specified objects in the BOOM database.
For this tool, you need to install the required GitHub packages:
```
pip install git+https://github.com/VTDA-Group/superphot-plus.git
pip install git+https://github.com/kdesoto-astro/snapi.git@39ac8aa0c
```
*Use the 39ac8aa0c specific commit of snapi to avoid the following error:
`operands could not be broadcast together with shapes (100,64) (1000,64)`*

Run the script with the desired options:
```
python -m tools.superphot --object-id <object_id>
```