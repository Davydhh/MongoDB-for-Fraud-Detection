db.transactions.aggregate([
    {
        "$group": {
            "_id": "$TERMINAL_ID",
            "cust_used_once": {
                "$addToSet": "$CUSTOMER_ID"
            }
        }
    }
])