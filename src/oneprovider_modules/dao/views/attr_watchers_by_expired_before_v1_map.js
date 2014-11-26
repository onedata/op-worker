// View that allows to list all file descriptors that has expired before/after given time
function(doc) {
    if(doc.record__ == "file_attr_watcher")
        emit(doc.create_time + doc.validity_time, 1);
}