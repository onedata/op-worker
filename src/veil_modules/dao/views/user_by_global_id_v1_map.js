// View that allows selecting user by his global id
function(doc)
{
    if(doc.record__ == "user")
	   emit(doc.global_id, null);
}