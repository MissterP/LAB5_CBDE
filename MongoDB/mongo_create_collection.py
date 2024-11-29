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
    # Indexes for the 'orders' collection
    try:
        # Compound index on 'lineItems.returnFlag' and 'lineItems.lineStatus' for Query 1
        db.orders.create_index(
            [("lineItems.returnFlag", ASCENDING), ("lineItems.lineStatus", ASCENDING)],
            name="idx_returnFlag_lineStatus"
        )
        print("Compound index created on 'orders.lineItems.returnFlag' and 'orders.lineItems.lineStatus'. For Query 1")

        # Index on 'lineItems.shipDate' for Queries 1 and 3
        db.orders.create_index(
            [("lineItems.shipDate", ASCENDING)],
            name="idx_lineItems_shipDate"
        )
        print("Index created on 'orders.lineItems.shipDate'. For Queries 1 and 3")

        # Compound index on 'customerInfo.mktSegment', 'orderDate', and 'shipPriority' for Query 3
        db.orders.create_index(
            [("customerInfo.mktSegment", ASCENDING), ("orderDate", ASCENDING), ("shipPriority", ASCENDING)],
            name="idx_mktSegment_orderDate_shipPriority"
        )
        print("Compound index created on 'orders.customerInfo.mktSegment', 'orders.orderDate', and 'orders.shipPriority'. For Query 3")

        # Index on 'orderDate' for Query 4
        db.orders.create_index(
            [("orderDate", ASCENDING)],
            name="idx_orderDate"
        )
        print("Index created on 'orders.orderDate'. For Query 4")

        # Index on 'customerInfo.nation.region' for Query 4
        db.orders.create_index(
            [("customerInfo.nation.region", ASCENDING)],
            name="idx_nation_region"
        )
        print("Index created on 'orders.customerInfo.nation.region'. For Query 4")

    except Exception as e:
        print(f"Error creating indexes in 'orders': {e}")

    # Indexes for the 'partsupp' collection
    try:
        # Compound index on 'partInfo.size' and 'partInfo.type' for Query 2
        db.partsupp.create_index(
            [("partInfo.size", ASCENDING), ("partInfo.type", ASCENDING), ("suppInfo.nation.region", ASCENDING)],
            name="idx_size_type"
        )
        print("Compound index created on 'partsupp.partInfo.size', 'partsupp.partInfo.type' and 'suppInfo.nation.region'. For Query 2")

        # Index on 'suppInfo.nation.region' for Query 2 and 4
        db.partsupp.create_index(
            [("suppInfo.nation.region", ASCENDING)],
            name="idx_suppInfo_nation_region"
        )
        print("Index created on 'partsupp.suppInfo.nation.region'. For Query 4")

        # Compound index on multiple fields for a specific query (Query 2)
        db.partsupp.create_index(
            [
                ("suppInfo.acctBal", DESCENDING),
                ("suppInfo.nation.name", ASCENDING),
                ("suppInfo.name", ASCENDING),
                ("partInfo.mfgr", ASCENDING),
                ("partInfo.partKey", ASCENDING)
            ],
            name="idx_acctBal_nationName_name_mfgr_brand_type_partKey"
        )
        print("Compound index created on 'partsupp.suppInfo.acctBal', 'partsupp.suppInfo.nation.name', 'partsupp.suppInfo.name', 'partsupp.partInfo.mfgr', 'partsupp.partInfo.brand', 'partsupp.partInfo.type', and 'partsupp.partInfo.partKey'. For Query 2")

    except Exception as e:
        print(f"Error creating indexes in 'partsupp': {e}")

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
