// This view allows to search by name or by uid (user id) and name or only uid
// startkey=[uid, null], endkey=[uid, {}] - searching for all file documents of user with uid
function(doc) {
    if(doc.record__ == 'file') {
        emit([null, doc.name], {type: doc.type, _id: doc.meta_doc});
        emit([doc.uid, doc.name], {type: doc.type, _id: doc.meta_doc});
    }
}