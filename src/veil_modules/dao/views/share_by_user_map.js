function(doc) {
    if(doc.record__ == "share_desc")
        emit(doc.user, null);
}