// View that allows counting of users' files size
function(key, values, rereduce) {
    return sum(values);
}