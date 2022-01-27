db.transactions.aggregate([
   {
      $project: {
          "TERMINAL_ID": 1,
          "CUSTOMER_ID": 1,
          "TX_AMOUNT": 1,
          "year": {
              $year: "$TX_DATETIME"
          },
           "month": {
                $month: "$TX_DATETIME"
            },
            "day": {
                $dayOfMonth: "$TX_DATETIME"  
            }
      }  
    },
    {
        $match: {
            "year": 2018,
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