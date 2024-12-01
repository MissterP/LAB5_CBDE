import os
import json 
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, CollectionInvalid, WriteError

# Define the directory where the JSON schemas are stored
SCHEMA_DIR = os.path.join(os.path.dirname(__file__), 'schemas')

# Function to load a JSON schema from a file
def load_schema(filename):
    filepath = os.path.join(SCHEMA_DIR, filename)
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        print(f"Schema '{filename}' loaded successfully.")
        return schema
    except FileNotFoundError:
        print(f"Schema file '{filename}' not found in '{SCHEMA_DIR}'.")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from file '{filename}': {e}")
        return None

# Creates a connection to MongoDB
def connect_mongo(uri='mongodb://localhost:27017/'):
    try:
        client = MongoClient(uri)
        client.admin.command('ping') # Verify connection
        print("Connected to MongoDB successfully.")
        return client
    except ConnectionFailure as e:
        print(f"Failed to connect MongoDB: {e}.")
        return None

# Creates a collection in a database 
def create_collection(client, db_name, collection_name, schema=None):
    db = client[db_name]
    if collection_name in db.list_collection_names():
        db.drop_collection(collection_name)
        print(f"Collection '{collection_name}' existed and was dropped.")
    try:
        if schema:
            validator = {
                "$jsonSchema": schema
            }
            collection = db.create_collection(collection_name, validator=validator)
            print(f"Collection '{collection_name}' created with schema validation.")
        else:
            collection = db[collection_name]
            print(f"Collection '{collection_name}' created without schema validation.")
        return collection
    except CollectionInvalid as e:
        print(f"Failed to create collection '{collection_name}': {e}")
        return None
    
def create_indexes(db):
    try: 
        db.orders.create_index(
            [
                ("lineItems.shipDate", ASCENDING),
                ("lineItems.returnFlag", ASCENDING),
                ("lineItems.lineStatus", ASCENDING)
            ],
            name="idx_lineItems_shipDate_returnFlag_lineStatus"
        )
        print("Índice compuesto 'idx_lineItems_shipDate_returnFlag_lineStatus' creado.")
    except Exception as e:
        print(f"Error al crear índices: {e}")
    

def main():
    client = connect_mongo()
    if client:
        db_name = 'test'
        
        collections = {
            'orders': 'orders.json',
            'partsupp': 'partsupp.json'
        }

        for collection_name, schema_file in collections.items():
            schema = load_schema(schema_file)
            if schema:
                collection = create_collection(client, db_name, collection_name, schema)
            else:
                collection = create_collection(client, db_name, collection_name)

        create_indexes(client[db_name])


if __name__ == "__main__":
    main()
