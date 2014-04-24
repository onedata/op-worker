// View that allows fetching fuse_group_name document by fuse group's hash
function(doc) {
    if(doc.record__ == "fuse_group_name")
        emit(doc.hash, 0);
}