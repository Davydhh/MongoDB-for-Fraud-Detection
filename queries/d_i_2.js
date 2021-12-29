db.transactions.find({"product_kind": {$exists : false }}).forEach(function(doc) {
    const kinds = ["high-tech", "food", "clothing", "consumable", "other"];
    const random = Math.floor(Math.random() * kinds.length)

    db.transactions.update({_id: doc._id}, {$set: {"product_kind": kinds[random]}})
})