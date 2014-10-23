// View that allows fetching all file_blocks that belong to a particular file_location
function(doc) {
    if(doc.record__ == "file_block")
        emit(doc.file_location_id, null);
}