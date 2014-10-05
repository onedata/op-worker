// View that allows counting files' childs
function(doc) {
    if(doc.record__ == "file")
        emit([doc.parent, doc.name], 1);
}
