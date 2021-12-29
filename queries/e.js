db.small_transaction.aggregate([
    {
        $group: {
            _id:"$period", 
            n_transactions:{$sum: 1}, 
            avg_fraudulent:{$avg: "$TX_FRAUD"}
        }
    }
])