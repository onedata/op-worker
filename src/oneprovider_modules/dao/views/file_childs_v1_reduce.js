// View that allows counting files' subdirs and subfiles
function(key, values, rereduce) {
    var total = [0, 0];
    for(valueIndex in values) {
        total[0] += values[valueIndex][0];
        total[1] += values[valueIndex][1];
    }
    return total;
}
