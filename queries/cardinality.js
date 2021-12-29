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
db.transaction.aggregate([
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

// average number of transactions per customer
db.transaction.aggregate([
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
db.transaction.aggregate([
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

// average number of transactions per terminal
db.transaction.aggregate([
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