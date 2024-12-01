import json
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, WriteError, PyMongoError
import datetime
import re
import pprint

def connect_mongo(uri='mongodb://localhost:27017/'):
    try:
        client = MongoClient(uri)
        client.admin.command('ping')  
        print("Connected to MongoDB successfully.")
        return client
    except ConnectionFailure as e:
        print(f"Error connecting to MongoDB: {e}")
        return None


def insert_documents(collection, documents):
    try:
        if documents:
            collection.delete_many({})
            result = collection.insert_many(documents)
            print(f"{len(result.inserted_ids)} documents inserted into the '{collection.name}' collection.")
    except WriteError as e:
        print(f"WriteError inserting documents into '{collection.name}': {e}")
    except Exception as e:
        print(f"Unexpected error inserting documents into '{collection.name}': {e}")


def get_sample_data():
    orders_documents = [
        {
            "orderKey": 1,
            "customerKey": 101,
            "partKey": 1001,
            "suppKey": 5001,
            "orderDate": datetime.datetime(2024, 11, 15),
            "shipPriority": 1,
            "customerInfo": {
                "customerKey": 101,
                "nation": {
                    "name": "USA",
                    "region": "North America"
                },
                "mktSegment": "AUTOMOBILE"
            },
            "lineItems": [
                {
                    "partKey": 1001,
                    "suppKey": 5001,
                    "lineNumber": 1,
                    "quantity": 10,
                    "extendedPrice": 250.0,
                    "discount": 0.05,
                    "tax": 0.08,
                    "returnFlag": "A",
                    "lineStatus": "O",
                    "shipDate": datetime.datetime(2024, 12, 1)
                },
                {
                    "partKey": 1002,
                    "suppKey": 5002,
                    "lineNumber": 2,
                    "quantity": 5,
                    "extendedPrice": 150.0,
                    "discount": 0.10,
                    "tax": 0.07,
                    "returnFlag": "B",
                    "lineStatus": "F",
                    "shipDate": datetime.datetime(2024, 11, 25)
                }
            ]
        },
        {
            "orderKey": 2,
            "customerKey": 102,
            "partKey": 1003,
            "suppKey": 5003,
            "orderDate": datetime.datetime(2024, 10, 20),
            "shipPriority": 2,
            "customerInfo": {
                "customerKey": 102,
                "nation": {
                    "name": "Canada",
                    "region": "North America"
                },
                "mktSegment": "FURNITURE"
            },
            "lineItems": [
                {
                    "partKey": 1003,
                    "suppKey": 5003,
                    "lineNumber": 1,
                    "quantity": 20,
                    "extendedPrice": 500.0,
                    "discount": 0.07,
                    "tax": 0.09,
                    "returnFlag": "A",
                    "lineStatus": "O",
                    "shipDate": datetime.datetime(2024, 12, 5)
                }
            ]
        },
        {
            "orderKey": 3,
            "customerKey": 103,
            "partKey": 1004,
            "suppKey": 5004,
            "orderDate": datetime.datetime(2024, 9, 10),
            "shipPriority": 3,
            "customerInfo": {
                "customerKey": 103,
                "nation": {
                    "name": "Mexico",
                    "region": "Central America"
                },
                "mktSegment": "ELECTRONICS"
            },
            "lineItems": [
                {
                    "partKey": 1004,
                    "suppKey": 5004,
                    "lineNumber": 1,
                    "quantity": 15,
                    "extendedPrice": 300.0,
                    "discount": 0.08,
                    "tax": 0.06,
                    "returnFlag": "B",
                    "lineStatus": "F",
                    "shipDate": datetime.datetime(2024, 11, 30)
                },
                {
                    "partKey": 1005,
                    "suppKey": 5005,
                    "lineNumber": 2,
                    "quantity": 8,
                    "extendedPrice": 200.0,
                    "discount": 0.12,
                    "tax": 0.07,
                    "returnFlag": "A",
                    "lineStatus": "O",
                    "shipDate": datetime.datetime(2024, 12, 10)
                }
            ]
        }
    ]

    partsupp_documents = [
        {
            "partKey": 1001,
            "suppKey": 5001,
            "supplyCost": 50.0,
            "partInfo": {
                "partKey": 1001,
                "name": "Engine",
                "mfgr": "ManufacturerA",
                "brand": "BrandX",
                "type": "Type1",
                "size": 10
            },
            "suppInfo": {
                "suppKey": 5001,
                "name": "SupplierA",
                "address": "1234 Industrial Ave",
                "nation": {
                    "name": "USA",
                    "region": "North America"
                },
                "phone": "555-1234",
                "acctBal": 10000.0,
                "comment": "Reliable supplier."
            }
        },
        {
            "partKey": 1002,
            "suppKey": 5002,
            "supplyCost": 30.0,
            "partInfo": {
                "partKey": 1002,
                "name": "Wheel",
                "mfgr": "ManufacturerB",
                "brand": "BrandY",
                "type": "Type2",
                "size": 5
            },
            "suppInfo": {
                "suppKey": 5002,
                "name": "SupplierB",
                "address": "5678 Commerce St",
                "nation": {
                    "name": "Canada",
                    "region": "North America"
                },
                "phone": "555-5678",
                "acctBal": 8000.0,
                "comment": "Fast delivery."
            }
        },
        {
            "partKey": 1003,
            "suppKey": 5003,
            "supplyCost": 40.0,
            "partInfo": {
                "partKey": 1003,
                "name": "Seat",
                "mfgr": "ManufacturerC",
                "brand": "BrandZ",
                "type": "Type3",
                "size": 7
            },
            "suppInfo": {
                "suppKey": 5003,
                "name": "SupplierC",
                "address": "9101 Market Blvd",
                "nation": {
                    "name": "Mexico",
                    "region": "Central America"
                },
                "phone": "555-9101",
                "acctBal": 9500.0,
                "comment": "High-quality materials."
            }
        },
        {
            "partKey": 1004,
            "suppKey": 5004,
            "supplyCost": 25.0,
            "partInfo": {
                "partKey": 1004,
                "name": "Battery",
                "mfgr": "ManufacturerD",
                "brand": "BrandW",
                "type": "Type4",
                "size": 3
            },
            "suppInfo": {
                "suppKey": 5004,
                "name": "SupplierD",
                "address": "1213 Tech Park",
                "nation": {
                    "name": "USA",
                    "region": "North America"
                },
                "phone": "555-1213",
                "acctBal": 7000.0,
                "comment": "Innovative products."
            }
        },
        {
            "partKey": 1005,
            "suppKey": 5005,
            "supplyCost": 35.0,
            "partInfo": {
                "partKey": 1005,
                "name": "Screen",
                "mfgr": "ManufacturerE",
                "brand": "BrandV",
                "type": "Type5",
                "size": 4
            },
            "suppInfo": {
                "suppKey": 5005,
                "name": "SupplierE",
                "address": "1415 Innovation Dr",
                "nation": {
                    "name": "Canada",
                    "region": "North America"
                },
                "phone": "555-1415",
                "acctBal": 8500.0,
                "comment": "Cutting-edge technology."
            }
        }
    ]

    return orders_documents, partsupp_documents


def run_query1(orders_collection, date_param):
    query1 = [
        { "$unwind": "$lineItems" },
        { "$match": { "lineItems.shipDate": { "$lte": date_param } } },
        { "$group": {
            "_id": {
                "returnFlag": "$lineItems.returnFlag",
                "lineStatus": "$lineItems.lineStatus"
            },
            "sum_qty": { "$sum": "$lineItems.quantity" },
            "sum_base_price": { "$sum": "$lineItems.extendedPrice" },
            "sum_disc_price": { 
                "$sum": { 
                    "$multiply": [
                        "$lineItems.extendedPrice",
                        { "$subtract": [1, "$lineItems.discount"] }
                    ] 
                } 
            },
            "sum_charge": { 
                "$sum": { 
                    "$multiply": [
                        "$lineItems.extendedPrice",
                        { "$subtract": [1, "$lineItems.discount"] },
                        { "$add": [1, "$lineItems.tax"] }
                    ] 
                } 
            },
            "avg_qty": { "$avg": "$lineItems.quantity" },
            "avg_price": { "$avg": "$lineItems.extendedPrice" },
            "avg_disc": { "$avg": "$lineItems.discount" },
            "count_order": { "$sum": 1 }
        }},
        { "$sort": { "_id.returnFlag": 1, "_id.lineStatus": 1 } }
    ]

    print("\n--- Executing Aggregation Query with .explain() ---\n")
    try:
        explain_doc = orders_collection.database.command(
            "aggregate", 
            "orders", 
            pipeline=query1, 
            hint="idx_lineItems_shipDate_returnFlag_lineStatus",
            explain=True)
        pprint.pprint(explain_doc)
    except Exception as e:
        print(f"Error executing .explain(): {e}")

    print("\n--- Executing Aggregation Query and Displaying Results ---\n")
    try:
        aggregation_result = orders_collection.aggregate(query1)
        for doc in aggregation_result:
            pprint.pprint(doc)
    except Exception as e:
        print(f"Error executing aggregation: {e}")

def run_query2(partsupp_collection, size, type, region):

    query2 = [
        {
            "$match": {"partInfo.size": size, 
                    "partInfo.type": { "$regex": f"{re.escape(type)}$", "$options": "i" }, 
                    "suppInfo.nation.region": region
            }
        },

        {
            "$project": {
                "s_acctbal": "$suppInfo.acctBal",
                "s_name": "$suppInfo.name",
                "n_name": "$suppInfo.nation.name",
                "p_partkey": "$partInfo.partKey",
                "p_mfgr": "$partInfo.mfgr",
                "s_address": "$suppInfo.address",
                "s_phone": "$suppInfo.phone",
                "s_comment": "$suppInfo.comment"
            }
        },

        {
            
        }

        {"$sort": { "s_acctbal": -1, "n_name": 1, "s_name": 1, "p_partkey": 1 }}

        


    ]


    print("\n--- Executing Aggregation Query with .explain() ---\n")
    try:
        explain_doc = partsupp_collection.database.command(
            "aggregate", 
            "orders", 
            pipeline=query2, 
            hint="idx_lineItems_shipDate_returnFlag_lineStatus",
            explain=True)
        pprint.pprint(explain_doc)
    except Exception as e:
        print(f"Error executing .explain(): {e}")

    print("\n--- Executing Aggregation Query and Displaying Results ---\n")
    try:
        aggregation_result = partsupp_collection.aggregate(query2)
        for doc in aggregation_result:
            pprint.pprint(doc)
    except Exception as e:
        print(f"Error executing aggregation: {e}")


def main():

    client = connect_mongo()
    if not client:
        return

    db_name = 'test'
    db = client[db_name]

    try:
        orders_collection = db['orders']
        partsupp_collection = db['partsupp']
        print("Found 'orders' and 'partsupp' collections.")
    except Exception as e:
        print(f"Error accessing collections: {e}")
        return

    orders_documents, partsupp_documents = get_sample_data()

    insert_documents(orders_collection, orders_documents)
    insert_documents(partsupp_collection, partsupp_documents)

    date_param = datetime.datetime(2024, 12, 1)

    run_query1(orders_collection, date_param)



    run_query2(partsupp_collection, )

if __name__ == "__main__":
    main()
