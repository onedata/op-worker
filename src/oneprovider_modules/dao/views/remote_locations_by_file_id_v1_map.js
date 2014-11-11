// View that allows selecting remote_location by file_id
function(doc)
{
    if(doc.record__ == "remote_location")
        emit(doc.file_id, null);
}