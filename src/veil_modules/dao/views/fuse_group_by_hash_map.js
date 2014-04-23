// View that allows fetching fuse_group_hash document by fuse group's hash
function(doc) {
    if(doc.record__ == "fuse_group_hash")
        emit(doc.hash, 0);
}