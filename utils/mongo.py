from pymongo import MongoClient
from logger import log

def fetch_mongo(collection_name, url="mongodb://localhost:27017", db_name="boom"):
    """
    Fetch a MongoDB collection.

    Args:
        collection_name (str): Name of the collection.
        url (str): MongoDB connection URL.
        db_name (str): Name of the database.

    Returns:
        collection: The MongoDB collection object.
    """
    db = MongoClient(url)[db_name]
    if collection_name not in db.list_collection_names():
        log(f"Collection '{collection_name}' does not exist in database '{db_name}'.")
        return None

    return db[collection_name]