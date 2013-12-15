// View that allow to search for file documents by file_meta uuids
function(doc) {
    if(doc.record__ == 'file') {
        emit([doc.meta_doc, doc.type], null);
    }
}