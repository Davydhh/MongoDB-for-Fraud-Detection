db.terminals.aggregate([
    {
        $lookup: {
               from: "transactions",
               localField: "TERMINAL_ID",
               foreignField: "TERMINAL_ID",
               as: "transactions"
             }
    }
])