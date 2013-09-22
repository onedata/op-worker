function(doc) {
    if(doc.record__ == "storage_info")
        emit(doc.id, 0);
}