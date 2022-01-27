db.transactions.aggregate([
    {
        $lookup: {
               from: "transactions",
               localField: "TERMINAL_ID",
               foreignField: "TERMINAL_ID",
               as: "transactions"
             }
    },
    {
        $project: {
            "TERMINAL_ID": 1,
            "transactions": 1,
            "transactions_avg": {
                $map: {
                    input: "$transactions",
                    as: "transaction",
                    in: {"TRANSACTION_ID": "$$transaction.TRANSACTION_ID", 
                        "month": {$month: "$$transaction.TX_DATETIME"},
                        "year": {$year: "$$transaction.TX_DATETIME"},
                        "TX_AMOUNT": "$$transaction.TX_AMOUNT"
                    }
                }
            }
        }
    },
     {
        $match: {
            "transactions_avg": {
                $not: {
                    $size: 0
                }
            }, 
            "transactions_avg.month": 4,
            "transactions_avg.year": 2018
        }
    },
     {
        $project: {
            "transactions": 1,
            "TERMINAL_ID": 1,
            "transactions_avg": {
                $filter: {
                    input: "$transactions_avg",
                    as: "t",
                    cond: {$and: [{$eq: ["$$t.month", 4]}, {$eq: ["$$t.year", 2018]}]}
                }
            },
        }
    },
      {
        $project: {
            "transactions": 1,
            "TERMINAL_ID": 1,
            "transactions_avg": {
                $avg: "$transactions_avg.TX_AMOUNT"
            }
        }
    },
    {
        $project: {
            "transactions": 1,
            "TERMINAL_ID": 1,
            "transactions_avg": {
                $divide: ["$transactions_avg", 2]
            }
        }
    },
    {
        $match: {
            $expr: {
                $gt: ["$transactions.TX_AMOUNT", "$transactions_avg"]
            }
        }    
    },
    {
        $project: {
            "TERMINAL_ID": 1,
            "transactions": {
                $filter: {
                    input: "$transactions",
                    as: "t",
                    cond: {$gt: ["$$t.TX_AMOUNT", "$transactions_avg"]}
                }
            },
            "transactions_avg": 1
        }
    }
])
