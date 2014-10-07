// View that allows counting files' subdirs and subfiles
function(doc) {
    if(doc.record__ == "file")
        if(doc.type == 1) //directory
            emit([doc.parent, doc.name], [1, 0]);
        else //file
            emit([doc.parent, doc.name], [0, 1]);
}
