// View that allows selecting user document by user's cert DN
function(doc)
{
    if(doc.record__ == "user")
	   for(key in doc.dn_list)
		  emit(doc.dn_list[key], null);
}