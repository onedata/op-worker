// View that allows fetch file_descriptor document assigned to given file UUID
function(doc) {
    if(doc.record__ == "file_attr_watcher")
        emit(doc.fuse_id, 1);
}