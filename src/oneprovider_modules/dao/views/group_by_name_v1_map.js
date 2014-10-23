// View that allows selecting group by its name
function(doc)
{
    if(doc.record__ == "group_details")
        emit(doc.name, null);
}
