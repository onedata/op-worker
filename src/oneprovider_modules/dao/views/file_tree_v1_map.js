// View that allows listing files by their parent UUID and optionally - name
function(doc) {
    if(doc.record__ == "file" && doc.created == true)
        emit([doc.parent, doc.name], doc.size);
}