import logging
from ingestion import db
from datetime import datetime


def run_queries():
    run_query_a()


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
                'month': datetime.now().month
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


