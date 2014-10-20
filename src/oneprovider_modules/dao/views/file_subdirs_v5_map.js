// View that allows counting files' subdirectories
function(doc) {
    if(doc.record__ == "file" && doc.type == 1)
        emit([doc.parent, doc.name], 1);
}
