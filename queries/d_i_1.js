db.transactions.updateMany({}, [
    {
      $addFields: {
          "hour": {
              $hour: {
                "$toDate": "$TX_DATETIME"
              }
          }
        }  
    },
    {
        $addFields: {
            "period": {
                $switch: {
                  branches: [
                     { case: {$and: [{$gte: ["$hour", 6]}, {$lte: ["$hour", 12]}]}, then: "morning" },
                     { case: {$and: [{$gt: ["$hour", 12]}, {$lte: ["$hour", 18]}]}, then: "afternoon" },
                     { case: {$and: [{$gt: ["$hour", 18]}, {$lte: ["$hour", 22]}]}, then: "evening" },
                  ],
                  default: "night"
                }
            }
        }
    }
])