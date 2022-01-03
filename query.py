import logging
import time
import datetime
import random
from ingestion import db


def run_queries():
    run_query_a()
    run_query_b()
    run_query_c()
    run_query_d()
    run_query_e()


def run_query_a():
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
                'month': datetime.datetime.now().month
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
        }, {
            "$limit": 10
        }
    ]

    result = db.transactions.aggregate(pipeline)

    print("Query a result: {}".format(list(result)))

    logging.info("Analyzing performance...")

    performance_result = db.command('explain', {
        'aggregate': "transactions",
        'pipeline': pipeline,
        'cursor': {}
    }, verbosity='executionStats')

    print("Performance about query a: {} milliseconds\n".format(performance_result.get("stages")[0].get("$cursor").get(
        "executionStats").get("executionTimeMillis")))


def run_query_b():
    logging.info("Running query b")

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
        },
        {
            "$limit": 1
        }
    ]

    result = db.transactions.aggregate(pipeline)

    print("Query b result: {}".format(list(result)))

    logging.info("Analyzing performance...")

    performance_result = db.command('explain', {
        'aggregate': "transactions",
        'pipeline': pipeline,
        'cursor': {}
    }, verbosity='executionStats')

    print("Performance about query b: {} milliseconds\n".format(performance_result.get("stages")[0].get("$cursor").get(
        "executionStats").get("executionTimeMillis")))


def run_query_c():
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

    result = db.transactions.aggregate(pipeline)
    co_customers = []
    for t_1 in result:
        for t_2 in result:
            if t_1["_id"] > t_2["_id"]:
                t_1_and_t_2 = set(t_1["cust_used_once"]) & set(t_2["cust_used_once"])
                t_1_not_t_2 = set(t_1["cust_used_once"]) - set(t_2["cust_used_once"])
                t_2_not_t_1 = set(t_2["cust_used_once"]) - set(t_1["cust_used_once"])
                print('after initialization')

                if len(t_1_and_t_2) > 1:
                    for c_1 in t_1_not_t_2:
                        for c_2 in t_2_not_t_1:
                            co_customers.append({c_1, c_2})
                    
                    if len(t_1_and_t_2) > 2:
                        for c_1 in t_1_and_t_2:
                            for c_2 in t_1_and_t_2:
                                if c_1 != c_2:
                                    co_customers.append({c_1, c_2}) # a customer cannot be co-customer to himself

    print("Performance about query c: {} milliseconds\n".format((time.time() - start_time) * 1000))

    return(co_customers)


def run_query_d():
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

    db.transactions.update_many({}, pipeline)

    print("Performance about query d.i.1: {} milliseconds\n".format((time.time() - start_time) * 1000))

    # Point i.2
    logging.info("Running query d.i.2")
    start_time = time.time()

    kinds = ["high-tech", "food", "clothing", "consumable", "other"]
    for doc in db.transactions.find({"product_kind": {"$exists": False}}):
        db.transactions.update({"_id": doc.get("_id")}, {"$set": {"product_kind": random.choice(kinds)}})


    print("Performance about query d.i.2: {} milliseconds\n".format((time.time() - start_time) * 1000))

    # point ii
    logging.info("Running query d.i.2")
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


    result = db.transactions.aggregate(pipeline, allowDiskUse = True)

    db.customer.update_many({}, {'$set': {'buying_friends': []}})

    # explicit representation of the relationship
    for r in result:
        for c in r['buying_friends']:
            db.customer.update_one({'CUSTOMER_ID': c},\
                {'$push': {'buying_friends': {\
                    'terminal': r['_id'].get('ter'),\
                    'product_kind':r['_id'].get('prod'),\
                    'customers':list(set(r['buying_friends']) - {c})}}})

    print("Performance about query d.ii: {} milliseconds\n".format((time.time() - start_time) * 1000))


def run_query_e():
    logging.info("Running query e")
    start_time = time.time()

    pipeline = [
    {
        "$group": {
            "_id":"$period", 
            "n_transactions":{"$sum": 1}, 
            "avg_fraudulent":{"$avg": "$TX_FRAUD"}
        }
    }]

    result = db.transactions.aggregate(pipeline)

    print("Performance about query d.ii: {} milliseconds\n".format((time.time() - start_time) * 1000))