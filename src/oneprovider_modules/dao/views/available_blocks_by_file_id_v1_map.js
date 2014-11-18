// View that allows selecting available_blocks by file_id
function(doc)
{
    if(doc.record__ == "available_blocks")
        emit(doc.file_id, null);
}