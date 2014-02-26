// View that allows counting of users' files size
function(doc) {
    // count only regular files
    if(doc.record__ == "file" && doc.type == 0)
        emit(doc.uid, doc.size);
}