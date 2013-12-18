// The view lists all connection_info records along with 'session_id' field as key.
// It can be used to select connections that belongs to same fuse session.
function(doc) {
    if(doc.record__ == "connection_info" && doc.session_id)
        emit(doc.session_id, 1);
}