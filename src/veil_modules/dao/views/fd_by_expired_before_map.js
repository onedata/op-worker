function(doc) {
    emit(doc.create_time + doc.validity_time, 1);
}