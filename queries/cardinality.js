// max cardinality of 'available_terminal' relationship Customers side
db.customers.aggregate([
    {
        $group: {
            _id: null,
            max_avail_terminal: {
                $max: "$nb_terminals"
            }
        }
    }
])

// average cardinality of 'available_terminal' relationship Customers side
db.customers.aggregate([
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
db.customers.aggregate([
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
db.customers.aggregate([
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

// max cardinality of 'buying_friend' relationship
db.transactions.aggregate([
    {
        "$group": {
            "_id": {
                "ter": "$TERMINAL_ID", "cust": "$CUSTOMER_ID", "prod": "$product_kind"
            },
            "count": {
                "$sum": 1
            }
        }
    },
    {
        "$match": {
            "count": {
                "$gt": 3
            }
        }
    },
    {
        "$group": {
            "_id": {
                "ter": "$_id.ter", "prod": "$_id.prod"
            },
            "buying_friends": {
                "$addToSet": "$_id.cust"
            }
        }
    }, { 
        "$project": { 
            "buying_friends": 1, 
            "n": { 
                "$subtract": [
                    { 
                    "$size": "$buying_friends" 
                    }, 
                    1
                ] 
            } 
        } 
    }, { 
        "$unwind": "$buying_friends" 
    }, { 
        "$group": { 
            "_id": "$buying_friends", 
            "n_friends": { 
                "$sum": "$n" 
            } 
        } 
    }, { 
        "$group": { 
            "_id": null, 
            "max": { 
                "$max": "$n_friends" 
            } 
        } 
    }
])

// average cardinality of 'buying_friend' relationship
db.transactions.aggregate([
    {
        "$group": {
            "_id": {
                "ter": "$TERMINAL_ID", "cust": "$CUSTOMER_ID", "prod": "$product_kind"
            },
            "count": {
                "$sum": 1
            }
        }
    },
    {
        "$match": {
            "count": {
                "$gt": 3
            }
        }
    },
    {
        "$group": {
            "_id": {
                "ter": "$_id.ter", "prod": "$_id.prod"
            },
            "buying_friends": {
                "$addToSet": "$_id.cust"
            }
        }
    }, { 
        "$project": { 
            "buying_friends": 1, 
            "n": { 
                "$subtract": [
                    { 
                    "$size": "$buying_friends" 
                    }, 
                    1
                ] 
            } 
        } 
    }, { 
        "$unwind": "$buying_friends" 
    }, { 
        "$group": { 
            "_id": "$buying_friends", 
            "n_friends": { 
                "$sum": "$n" 
            } 
        } 
    }, { 
        "$group": { 
            "_id": null, 
            "avg": { 
                "$avg": "$n_friends" 
            } 
        } 
    }
])

// max cardinality of 'buying_friend' relationship by 'product_kind' and 'terminal_id'
db.transactions.aggregate([
    {
        "$group": {
            "_id": {
                "ter": "$TERMINAL_ID", "cust": "$CUSTOMER_ID", "prod": "$product_kind"
            },
            "count": {
                "$sum": 1
            }
        }
    },
    {
        "$match": {
            "count": {
                "$gt": 3
            }
        }
    },
    {
        "$group": {
            "_id": {
                "ter": "$_id.ter", "prod": "$_id.prod"
            },
            "buying_friends": {
                "$addToSet": "$_id.cust"
            }
        }
    },
    {
        "$group": {
            "_id": null,
            "max": {
                "$max": {
                    "$size": "$buying_friends"
                }
            }
        }
    }
])

// average cardinality of 'buying_friend' relationship by 'product_kind' and 'terminal_id'
db.transactions.aggregate([
    {
        "$group": {
            "_id": {
                "ter": "$TERMINAL_ID", "cust": "$CUSTOMER_ID", "prod": "$product_kind"
            },
            "count": {
                "$sum": 1
            }
        }
    },
    {
        "$match": {
            "count": {
                "$gt": 3
            }
        }
    },
    {
        "$group": {
            "_id": {
                "ter": "$_id.ter", "prod": "$_id.prod"
            },
            "buying_friends": {
                "$addToSet": "$_id.cust"
            }
        }
    },
    {
        "$group": {
            "_id": null,
            "avg": {
                "$avg": {
                    "$size": "$buying_friends"
                }
            }
        }
    }
])

// max cardinality of 'part_of' relationship customer side
db.transactions.aggregate([
    {
        "$group": {
            "_id": {
                "ter": "$TERMINAL_ID", "cust": "$CUSTOMER_ID", "prod": "$product_kind"
            },
            "count": {
                "$sum": 1
            }
        }
    },
    {
        "$match": {
            "count": {
                "$gt": 3
            }
        }
    },
    {
        "$group": {
            "_id": "$_id.cust",
            "buying_groups": {
                "$sum": 1
            }
        }
    },
    {
        "$group": {
            "_id": null,
            "max": {
                "$max": "$buying_groups"
            }
        }
    }
])

// average cardinality of 'part_of' relationship customer side
db.transactions.aggregate([
    {
        "$group": {
            "_id": {
                "ter": "$TERMINAL_ID", "cust": "$CUSTOMER_ID", "prod": "$product_kind"
            },
            "count": {
                "$sum": 1
            }
        }
    },
    {
        "$match": {
            "count": {
                "$gt": 3
            }
        }
    },
    {
        "$group": {
            "_id": "$_id.cust",
            "buying_groups": {
                "$sum": 1
            }
        }
    },
    {
        "$group": {
            "_id": null,
            "avg": {
                "$avg": "$buying_groups"
            }
        }
    }
])