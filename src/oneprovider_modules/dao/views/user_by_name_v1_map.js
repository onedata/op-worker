// View that allows selecting user by its name
function(doc)
{
    if(doc.record__ == "user")
        emit(doc.name, null);
}
