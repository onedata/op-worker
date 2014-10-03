// The view lists all fuse_sessions records along with 'valid_to' field as key.
// It can be used to select sessions that have expired
function(doc) {
    if(doc.record__ == "fuse_session" && doc.valid_to)
        emit(doc.valid_to, null);
}