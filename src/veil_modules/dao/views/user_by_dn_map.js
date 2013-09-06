function(doc)
{
	for(key in doc.dn_list)
		emit(doc.dn_list[key], null);
}