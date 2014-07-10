// View that allows counting files' subdirectories
function(key, values, rereduce) {
    return sum(values);
}