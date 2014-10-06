// View that allows counting files' childs
function(key, values, rereduce) {
    return sum(values);
}
