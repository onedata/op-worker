function(doc) {
    if(doc.record__ == "storage_info")
        emit(0, doc.id);
}