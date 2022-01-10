// max cardinality of 'available_terminal' relationship Customer side
db.customer.aggregate([
    {
        $group: {
            _id: null,
            max_avail_terminal: {
                $max: "$nb_terminals"
            }
        }
    }
])
// max: 106 ; avg: 75.20

// average cardinality of 'available_terminal' relationship Customer side
db.customer.aggregate([
    {
        $group: {
            _id: null,
            avg_avail_terminal: {
                $avg: "$nb_terminals"
            }
        }
    }
])

// max number of transactions per customer
db.transactions.aggregate([
    {
        $group: {
            _id: "$CUSTOMER_ID",
            n_transactions: {
                $sum: 1
            }
        }
    },
    {
        $group: {
            _id: null,
            max_n_trans: {
                $max: "$n_transactions"
            }
        }
    }
])
// 2018: max: 179, avg: 77
// 2018+2019: max: 520, avg: 231  
// 2018+2019+2020: max: 1200, avg: 538

// average number of transactions per customer
db.transactions.aggregate([
    {
        $group: {
            _id: "$CUSTOMER_ID",
            n_transactions: {
                $sum: 1
            }
        }
    },
    {
        $group: {
            _id: null,
            avg_n_trans: {
                $avg: "$n_transactions"
            }
        }
    }
])

// max number of transactions per terminal
db.transactions.aggregate([
    {
        $group: {
            _id: "$TERMINAL_ID",
            n_transactions: {
                $sum: 1
            }
        }
    },
    {
        $group: {
            _id: null,
            max_n_trans_terminal: {
                $max: "$n_transactions"
            }
        }
    }
])
// 2018: max: 95, avg: 38
// 2018+2019: max: 263, avg: 115
// 2018+2019+2020: max: 586, avg: 268

// average number of transactions per terminal
db.transactions.aggregate([
    {
        $group: {
            _id: "$TERMINAL_ID",
            n_transactions: {
                $sum: 1
            }
        }
    },
    {
        $group: {
            _id: null,
            avg_n_trans_terminal: {
                $avg: "$n_transactions"
            }
        }
    }
])

// max cardinality of 'available_terminal' relationship Terminal side
db.customer.aggregate([
    {
        $unwind: "$available_terminals"
    }, {
        $group: {
            _id: "$available_terminals",
            n_customers: {
                $sum: 1
            }
        }
    }, {
        $group: {
            _id: null,
            max: {
                $max:
                    "$n_customers"
            }
        }
    }
])
// max: 63, 37

// average cardinality of 'available_terminal' relationship Terminal side
db.customer.aggregate([
    {
        $unwind: "$available_terminals"
    },
    {
        $group: {
            _id: "$available_terminals",
            n_customers: {
                $sum: 1
            }
        }
    },
    {
        $group: {
            _id: null,
            avg: {
                $avg: "$n_customers"
            }
        }
    }])