// View that allows counting sum of sizes of user's files
function(key, values, rereduce) {
    return sum(values);
}