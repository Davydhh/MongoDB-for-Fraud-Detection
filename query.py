import logging
import time
import datetime
import random
from ingestion import db


def run_queries():
    run_query_a()
    run_query_b()
    run_query_d()


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
    pass


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