// View that allows to search for file_meta docs by ctime and mtime (separately searching by ctime and mtime in one query is not possible)
function(doc){
    if(doc.record__ == 'file_meta') {
        emit([null, doc.mtime], null);
        emit([doc.ctime, null], null);
    }
}