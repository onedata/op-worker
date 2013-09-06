function(doc)
{
	for(key in doc.email_list)
		emit(doc.email_list[key], null);
}