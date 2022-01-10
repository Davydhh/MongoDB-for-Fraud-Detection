db.transactions.aggregate([
    {
      $match: {
          "TX_DATETIME": {
              $gt: 1577848122000
          }
      }  
    },
    {
        $group: { 
            _id: "$TERMINAL_ID",
            "avg": {
                $avg: "$TX_AMOUNT"
            }
        }
    },
    {
        $project: {
            "avg": {
                $divide: ["$avg", 2]
            }
        }
    }
])