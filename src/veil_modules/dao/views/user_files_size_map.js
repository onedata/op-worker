// View that allows counting sum of sizes of user's files
function(doc) {
    // count only regular files
    if(doc.record__ == "file_meta")
        emit(doc.uid, doc.size);
}