// View that allows counting of users' files
function(key, values, rereduce) {
    return sum(values);
}