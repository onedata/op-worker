function(doc) {
    emit([doc.parent, doc.name], doc.size);
}