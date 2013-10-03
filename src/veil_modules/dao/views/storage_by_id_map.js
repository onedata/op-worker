// View that allows fetching storage_info document by storage's ID
function(doc) {
    if(doc.record__ == "storage_info")
        emit(doc.id, 0);
}