// View that allows counting of groups' files
function(doc) {
    // count only regular files that have at least group name assigned
    if(doc.record__ == "file" && doc.type == 0 && doc.gids.length > 0)
        emit(doc.gids[0], 1); // Emit entry only for main group
}