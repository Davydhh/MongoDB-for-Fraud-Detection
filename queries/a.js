db.transactions.aggregate([
   {
      $project: {
          "TX_DATETIME": {
              "$toDate": "$TX_DATETIME"
          },
          "CUSTOMER_ID": 1,
          "TX_AMOUNT": 1
      }  
    },
    {
        $project: {
            "month": {
                $month: "$TX_DATETIME"
            },
            "day": {
                $dayOfMonth: "$TX_DATETIME"  
            },
            "CUSTOMER_ID": 1,
            "TX_AMOUNT": 1
        }
    },
    {
        $match: {
            "month": 4
        }
    },
    {
        $group: {
            _id: {"customer": "$CUSTOMER_ID", "day": "$day"},
            "amount": { $sum: "$TX_AMOUNT"}
        }
    }
])