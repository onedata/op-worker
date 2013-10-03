// View that allows selecting user by its login
function(doc)
{
    if(doc.record__ == "user")
	   emit(doc.login, null);
}