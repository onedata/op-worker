// View that allows selecting user by its login
function(doc)
{
    if(doc.record__ == "user") {
        for(i in doc.logins) {
            loginEntry = doc.logins[i];
            emit([loginEntry.provider_id, loginEntry.login], null);
        }
    }
}