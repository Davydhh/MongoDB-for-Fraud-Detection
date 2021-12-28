import logging
import random
from ingestion import db
import datetime


def run_queries():
    run_query_a()
    run_query_b()

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

    print("Query result: {}".format(list(result)))

    logging.info("Analyzing performance...")

    performance_result = db.command('explain', {
        'aggregate': "transactions",
        'pipeline': pipeline,
        'cursor': {}
    }, verbosity='executionStats')

    print("Performance about query: {} milliseconds\n".format(performance_result.get("stages")[0].get("$cursor").get(
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

    print("Query result: {}".format(list(result)))

    logging.info("Analyzing performance...")

    performance_result = db.command('explain', {
        'aggregate': "transactions",
        'pipeline': pipeline,
        'cursor': {}
    }, verbosity='executionStats')

    print("Performance about query: {} milliseconds\n".format(performance_result.get("stages")[0].get("$cursor").get(
        "executionStats").get("executionTimeMillis")))


def run_query_c():
    logging.info("Running query c")
    pass


def run_query_d(product_kind= ["high-tech", "food", "clothing", "consumable", "other"],\
    day_period= [{"name":"night", "start":"00:00:00"}, {"name":"morning", "start":"06:00:00"}, \
        {"name":"afternoon", "start":"12:00:00"}, {"name":"evening", "start":"18:00:00"}]):
    logging.info("Running query d")
    #step one: extend the model
    def select_product(product_kind=product_kind):
        return random.choice(product_kind)

    def select_period(time, period=day_period):
        #suppose period ordered by 'start'
        period_label = "null"
        for p in period:
            if datetime.strptime(time, "%H:%M:%S") >= datetime.strptime(p.get('start'), "%H:%M:%S"):
                period_label = p.get('name')
            else:
                break
        return period_label
    #step two: buying_friends
    return select_period()