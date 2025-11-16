import io
import fastavro

from datetime import datetime
from pymongo import MongoClient

RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
ENDC = "\033[0m"

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

def log(message):
    print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - {message}")

def fetch_mongo(url, db_name, collection_name):
    """
    Fetch a MongoDB collection.
    Args:
        url (str): MongoDB connection URL.
        db_name (str): Name of the database.
        collection_name (str): Name of the collection.
    Returns:
        collection: The MongoDB collection object.
    """
    db = MongoClient(url)[db_name]
    if collection_name not in db.list_collection_names():
        log(f"Collection '{collection_name}' does not exist in database '{db_name}'.")
        return None

    return db[collection_name]