function(doc) {
    if(doc.record__ == "share_desc")
        emit(doc.file, null);
}