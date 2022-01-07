import json
import logging
import os
import time
import pymongo

client = pymongo.MongoClient("mongodb+srv://Davide:davide@cluster0.ovl1s.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

# client = pymongo.MongoClient("localhost", 27017)
db = client.projectDB


def load_datasets():
    logging.info("Storing datasets")
    start_time = time.time()
    path = "./datasets/"
    with open("{}customers.json".format(path)) as file:
        if db.customers.count() == 0:
            db.customers.insert_many(json.load(file))
            db.customers.create_index("CUSTOMER_ID", unique=True)
            logging.info("Customers dataset loaded")

    with open("{}terminals.json".format(path)) as file:
        if db.terminals.count() == 0:
            db.terminals.insert_many(json.load(file))
            db.terminals.create_index("TERMINAL_ID", unique=True)
            logging.info("Terminals dataset loaded")

    transactions_files = [f for f in os.listdir(path) if f.startswith("transactions")]

    latest_file = max(transactions_files)

    with open(os.path.join(path, latest_file)) as file:
        db.transactions.insert_many(json.load(file))
        db.transactions.create_index("TERMINAL_ID")
        logging.info("{} dataset loaded".format(latest_file))

    logging.info("Datasets stored on MongoDB in {} seconds".format(time.time() - start_time))
