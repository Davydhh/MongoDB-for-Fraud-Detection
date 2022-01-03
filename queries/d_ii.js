db.small_transaction.aggregate([
    {
        $group: {
            _id: {
                "ter": "$TERMINAL_ID", "cust": "$CUSTOMER_ID", "prod": "$product_kind"
            },
            "count": {
                $sum: 1
            }
        }
    },
    {
        $match: {
            "count": {
                $gt: 3
            }
        }
    },
    {
        $group: {
            _id: {
                "ter": "$_id.ter", "prod": "$_id.prod"
            },
            "buying_friends": {
                $addToSet: "$_id.cust"
            }
        }
    }],
    {
        allowDiskUse: true
    })