// For each space emit all its users. This view allows selecting space list for each user by its GRUID
function(doc) {
    if(doc.record__ == "file" && doc.type == 1)
    {
        for(extId in doc.extensions)
        {
            ext = doc.extensions[extId];
            if(ext.tuple_field_1 == "__atom__: space_info")
            {
                spaceInfo = ext.tuple_field_2
                for(id in spaceInfo.users)
                {
                    emit(spaceInfo.users[id], null)
                }
            }
        }
    }
}
