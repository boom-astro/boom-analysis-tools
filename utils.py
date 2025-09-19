import io
import fastavro

def read_avro(msg):
    """
    Reads an Avro record from a Kafka message.

    Args:
        msg: The message object containing the Avro data.

    Returns:
        The first record found in the Avro message, or None if no records are found.
    """

    bytes_io = io.BytesIO(msg.value())  # Get the message value as bytes
    bytes_io.seek(0)
    for record in fastavro.reader(bytes_io):
        return record  # Return the first record found
    return None  # Return None if no records are found or if an error occurs

