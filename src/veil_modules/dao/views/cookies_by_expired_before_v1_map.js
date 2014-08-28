// View that allows to list all file descriptors that has expired before/after given time
function(doc) {
    if(doc.record__ == "session_cookie")
        emit(doc.valid_till, 1);
}