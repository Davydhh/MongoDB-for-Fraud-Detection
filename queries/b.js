db.terminals.aggregate([
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
            "transactions_avg": "$transactions"
        }
    },
    {
        $match: {
            $and: [
                {
                    "transactions_avg": {
                        $not: {
                            $size: 0
                        }
                    }   
                },
                {
                    "transactions_avg.TX_DATETIME": {
                            $gte: 1600721716067
                    }
                }
            ]
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
                    cond: {$gt: ["$$t.TX_DATETIME", 1600721716067]}
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
            "transactions": 1,
            "TERMINAL_ID": 1,
            "transactions": {
                $filter: {
                    input: "$transactions",
                    as: "t",
                    cond: {$gt: ["$$t.TX_AMOUNT", "$transactions_avg"]}
                }
            },
        }
    }
])
