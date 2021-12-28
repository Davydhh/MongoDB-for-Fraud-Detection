import pymongo
import json
import os
import logging


# client = pymongo.MongoClient("mongodb+srv://Davide:davide@cluster0.ovl1s.mongodb.net/projectDB?retryWrites"
#                              "=true&w=majority")

client = pymongo.MongoClient("localhost", 27017)
db = client.projectDB


def load_datasets():
    path = "./datasets/"
    with open("{}customers.json".format(path)) as file:
        if db.customers.count() == 0:
            db.customers.insert_many(json.load(file))

    with open("{}terminals.json".format(path)) as file:
        if db.terminals.count() == 0:
            db.terminals.insert_many(json.load(file))

    for filename in os.listdir(path):
        if filename.startswith("transactions"):
            with open(os.path.join(path, filename)) as file:
                db.transactions.insert_many(json.load(file))

    logging.info("Datasets stored on MongoDB")

