// View that allows selecting user by its UID
function(doc) {
    if(doc.record__ == "user")
        emit(doc._id, 0);
}