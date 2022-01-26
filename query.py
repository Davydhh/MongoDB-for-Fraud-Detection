import logging
import time
import datetime
import random
from ingestion import db


def run_queries():
    date = get_current_date()
    run_query_a(date)
    run_query_b()
    run_query_c()
    run_query_d()
    run_query_e()

def get_current_date():
    pipeline = [
        {
            '$project': {
                'TX_DATETIME': {
                    '$toDate': '$TX_DATETIME'
                }
            }
        }, {
            '$group': {
                '_id': None, 
                'current_day': {
                    '$max': '$TX_DATETIME'
                }
            }
        }
    ]

    result = db.transactions.aggregate(pipeline).next()['current_day']
    
    return(result)


def run_query_a(date):
    logging.info("Running query a")

    pipeline = [
        {
            '$project': {
                'TX_DATETIME': {
                    '$toDate': '$TX_DATETIME'
                },
                'CUSTOMER_ID': 1,
                'TX_AMOUNT': 1
            }
        }, {
            '$project': {
                'month': {
                    '$month': '$TX_DATETIME'
                },
                'day': {
                    '$dayOfMonth': '$TX_DATETIME'
                },
                'CUSTOMER_ID': 1,
                'TX_AMOUNT': 1
            }
        }, {
            '$match': {
                'month': date.month
            }
        }, {
            '$group': {
                '_id': {
                    'customer': '$CUSTOMER_ID',
                    'day': '$day'
                },
                'amount': {
                    '$sum': '$TX_AMOUNT'
                }
            }
        }
    ]

    db.transactions.aggregate(pipeline)

    logging.info("Query a executed")

    logging.info("Analyzing performance...")

    performance_result = db.command('explain', {
        'aggregate': "transactions",
        'pipeline': pipeline,
        'cursor': {}
    }, verbosity='executionStats')

    print("Performance about query a: {} seconds\n".format(performance_result.get("stages")[0].get("$cursor").get(
        "executionStats").get("executionTimeMillis") / 1000))


def run_query_b():
    logging.info("Running query b")
    start_time = time.time()

    now = datetime.datetime.utcnow()

    last_30d = (now - datetime.timedelta(days=30)).timestamp() * 1000

    pipeline = [
        {
            '$lookup': {
                'from': 'transactions',
                'localField': 'TERMINAL_ID',
                'foreignField': 'TERMINAL_ID',
                'as': 'transactions'
            }
        }, {
            '$project': {
                'TERMINAL_ID': 1,
                'transactions': 1,
                'transactions_avg': '$transactions'
            }
        }, {
            '$match': {
                '$and': [
                    {
                        'transactions_avg': {
                            '$not': {
                                '$size': 0
                            }
                        }
                    }, {
                        'transactions_avg.TX_DATETIME': {
                            '$gte': last_30d
                        }
                    }
                ]
            }
        }, {
            '$project': {
                'transactions': 1,
                'TERMINAL_ID': 1,
                'transactions_avg': {
                    '$filter': {
                        'input': '$transactions_avg',
                        'as': 't',
                        'cond': {
                            '$gt': [
                                '$$t.TX_DATETIME', last_30d
                            ]
                        }
                    }
                }
            }
        }, {
            '$project': {
                'transactions': 1,
                'TERMINAL_ID': 1,
                'transactions_avg': {
                    '$avg': '$transactions_avg.TX_AMOUNT'
                }
            }
        }, {
            '$project': {
                'transactions': 1,
                'TERMINAL_ID': 1,
                'transactions_avg': {
                    '$divide': [
                        '$transactions_avg', 2
                    ]
                }
            }
        }, {
            '$match': {
                '$expr': {
                    '$gt': [
                        '$transactions.TX_AMOUNT', '$transactions_avg'
                    ]
                }
            }
        }, {
            '$project': {
                'transactions': {
                    '$filter': {
                        'input': '$transactions',
                        'as': 't',
                        'cond': {
                            '$gt': [
                                '$$t.TX_AMOUNT', '$transactions_avg'
                            ]
                        }
                    }
                },
                'TERMINAL_ID': 1
            }
        }
    ]

    db.transactions.aggregate(pipeline)

    logging.info("Query b executed")

    print("Performance about query b: {} seconds\n".format(time.time() - start_time))


def run_query_b_splitted():
    logging.info("Running query b splitted")

    start_time = time.time()

    now = datetime.datetime.utcnow()

    last_30d = (now - datetime.timedelta(days=30)).timestamp() * 1000

    pipeline = [
        {
            '$match': {
                'TX_DATETIME': {
                    '$gt': last_30d
                }
            }
        }, {
            '$group': {
                '_id': '$TERMINAL_ID',
                'avg': {
                    '$avg': '$TX_AMOUNT'
                }
            }
        }, {
            '$project': {
                'avg': {
                    '$divide': [
                        '$avg', 2
                    ]
                }
            }
        }
    ]

    result = db.transactions.aggregate(pipeline)

    thresholds = {}

    for doc in result:
        thresholds[doc["_id"]] = doc["avg"]

    pipeline = [
        {
            '$lookup': {
                'from': 'transactions',
                'localField': 'TERMINAL_ID',
                'foreignField': 'TERMINAL_ID',
                'as': 'transactions'
            }
        }
    ]

    result = db.terminals.aggregate(pipeline)

    fraudolent_transactions = {}

    for doc in result:
        fraudolent_transactions["TERMINAL_ID"] = doc["TERMINAL_ID"]
        fraudolent_transactions["transactions"] = [t for t in doc["transactions"] if
                                                   t["TX_DATETIME"] > thresholds[doc["TERMINAL_ID"]]]

    logging.info("Query b splitted executed")

    print("Performance about query b splitted: {} seconds\n".format(time.time() - start_time))


def run_query_c(target_customer=0):
    logging.info("Running query c")
    start_time = time.time()

    pipeline = [
        {
            "$group": {
                "_id": "$TERMINAL_ID",
                "cust_used_once": {
                    "$addToSet": "$CUSTOMER_ID"
                }
            }
        }
    ]

    terminals_all = db.transactions.aggregate(pipeline)
    terminals_target = db.transactions.aggregate(pipeline + [{"$match": {"cust_used_once":target_customer}}])

    co_customers = set()
    for t_1 in terminals_target:
        for t_2 in terminals_all:
            t_1_and_t_2 = (set(t_1["cust_used_once"]) & set(t_2["cust_used_once"])) - {target_customer}
            t_2_not_t_1 = set(t_2["cust_used_once"]) - set(t_1["cust_used_once"])

            if len(t_1_and_t_2) > 0:
                co_customers.update(t_2_not_t_1)

                if len(t_1_and_t_2) > 1:
                    co_customers.update(t_1_and_t_2)

    logging.info("Query c executed")

    print("Performance about query c: {} seconds\n".format(time.time() - start_time))

    return(co_customers)


def run_query_d():
    random.seed(0)
    logging.info("Running query d.i.1")
    start_time = time.time()

    # Point i.1
    pipeline = [
        {
            "$addFields": {
                "hour": {
                    "$hour": {
                        "$toDate": "$TX_DATETIME"
                    }
                }
            }
        },
        {
            "$addFields": {
                "period": {
                    "$switch": {
                        "branches": [
                            {"case": {"$and": [{"$gte": ["$hour", 6]}, {"$lte": ["$hour", 12]}]}, "then": "morning"},
                            {"case": {"$and": [{"$gt": ["$hour", 12]}, {"$lte": ["$hour", 18]}]}, "then": "afternoon"},
                            {"case": {"$and": [{"$gt": ["$hour", 18]}, {"$lte": ["$hour", 22]}]}, "then": "evening"},
                        ],
                        "default": "night"
                    }
                }
            }
        }
    ]

    db.transactions.update_many({"period": {"$exists": False}}, pipeline)

    logging.info("Query d.i.1 executed")

    print("Performance about query d.i.1: {} seconds\n".format(time.time() - start_time))

    # Point i.2
    logging.info("Running query d.i.2")
    start_time = time.time()

    kinds = ["high-tech", "food", "clothing", "consumable", "other"]
    db.transactions.update_many({"product_kind": {"$exists": False}}, \
        {"$set": {"product_kind": random.choice(kinds)}})

    logging.info("Query d.i.2 executed")

    print("Performance about query d.i.2: {} seconds\n".format(time.time() - start_time))

    # point ii
    logging.info("Running query d.ii")
    start_time = time.time()

    pipeline = [
        {
            "$group": {
                "_id": {
                    "ter": "$TERMINAL_ID", "cust": "$CUSTOMER_ID", "prod": "$product_kind"
                },
                "count": {
                    "$sum": 1
                }
            }
        },
        {
            "$match": {
                "count": {
                    "$gt": 3
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "ter": "$_id.ter", "prod": "$_id.prod"
                },
                "buying_friends": {
                    "$addToSet": "$_id.cust"
                }
            }
        }]

    result = db.transactions.aggregate(pipeline, allowDiskUse=True)

    # explicit representation of the relationship
    for r in result:
        db.buying_groups.update_many({'terminal': r['_id'].get('ter'), \
                                    'product_kind': r['_id'].get('prod')}, \
            {"$addToSet": {"buying_friends": {"$each": r['buying_friends']}}}, upsert=True)

    logging.info("Query d.ii executed")

    print("Performance about query d.ii: {} seconds\n".format(time.time() - start_time))


def run_query_e():
    logging.info("Running query e")

    pipeline = [
        {
            "$group": {
                "_id": "$period",
                "n_transactions": {"$sum": 1},
                "avg_fraudulent": {"$avg": "$TX_FRAUD"}
            }
        }]

    db.transactions.aggregate(pipeline)

    logging.info("Query e exected")

    logging.info("Analyzing performance...")

    performance_result = db.command('explain', {
        'aggregate': "transactions",
        'pipeline': pipeline,
        'cursor': {}
    }, verbosity='executionStats')

    print("Performance about query e: {} seconds\n".format(performance_result.get("stages")[0].get("$cursor").get(
        "executionStats").get("executionTimeMillis") / 1000))
