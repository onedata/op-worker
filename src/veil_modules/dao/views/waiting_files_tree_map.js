// View that allows listing files by their parent UUID and optionally - name
function(doc) {
    //doc.type == 1 means directory
    if(doc.record__ == "file" && (doc.type == 1 || doc.created == false))
        emit([doc.parent, doc.name], doc.size);
}