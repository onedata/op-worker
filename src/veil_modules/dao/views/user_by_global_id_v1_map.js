// View that allows selecting user by its login
function(doc)
{
    if(doc.record__ == "user")
	   emit(doc.global_id, null);
}