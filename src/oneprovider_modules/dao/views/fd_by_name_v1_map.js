// View that allows fetch file_descriptor document assigned to given file UUID
function(doc) {
    if(doc.record__ == "file_descriptor")
        emit([doc.file, doc.fuse_id], 1);
}