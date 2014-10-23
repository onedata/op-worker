// View that allows fetching all file_locations that belong to a particular file
function(doc) {
    if(doc.record__ == "file_location")
        emit(doc.file_id, null);
}