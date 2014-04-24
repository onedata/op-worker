// The view lists all fuse_session records along with 'uid' (abbreviation for user id) field as key.
// It can be used to select fuse_sessions that belongs to the same user.
function(doc) {
    if(doc.record__ == "fuse_session" && doc.uid)
        // So far we need this view just to navigate from user id to fuse_session ids so emitting only key is ok.
        emit(doc.uid, null);
}