// View that allows counting of users' files size
function(doc) {
    // count only regular files
    if(doc.record__ == "file_meta")
        emit(doc.uid, doc.size);
}