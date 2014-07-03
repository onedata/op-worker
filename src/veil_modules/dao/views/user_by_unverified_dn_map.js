// View that allows selecting user document by user's cert DN
function(doc)
{
    if(doc.record__ == "user")
	   for(key in doc.unverified_dn_list)
		  emit(doc.unverified_dn_list[key], null);
}