// View that allows to list all saved storage documents
function(doc) {
    if(doc.record__ == "storage_info")
        emit(0, doc.id);
}