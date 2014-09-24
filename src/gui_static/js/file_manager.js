var CHMOD_CHECKBOXES = ['chbx_ur', 'chbx_uw', 'chbx_ux',
    'chbx_gr', 'chbx_gw', 'chbx_gx',
    'chbx_or', 'chbx_ow', 'chbx_ox'];

init_chmod_table = function () {
    for (var i = 0; i < CHMOD_CHECKBOXES.length; i++) {
        var chbx = $('#' + CHMOD_CHECKBOXES[i]);
        chbx.checkbox();
    }
    $('#chbx_recursive').checkbox();
    $('#octal_form_submit').click(function (event) {
        var textbox_value = $('#octal_form_textbox').val();
        var mode_oct = parseInt(textbox_value, 8);
        if (textbox_value.length != 3 || isNaN(mode_oct) || mode_oct < 0 || mode_oct > 511) {
            alert('This is not a valid octal representation of mode. Please type in a number between 000 and 777.');
        } else {
            for (var i = 0; i < CHMOD_CHECKBOXES.length; i++) {
                var chbx = $('#' + CHMOD_CHECKBOXES[i]);
                if ((mode_oct & Math.pow(2, 8 - i)) > 0) {
                    chbx.checkbox('check');
                } else {
                    chbx.checkbox('uncheck');
                }
            }
        }
    });
};

submit_chmod = function () {
    var mode = 0;
    for (var i = 0; i < CHMOD_CHECKBOXES.length; i++) {
        if ($('#' + CHMOD_CHECKBOXES[i]).is(':checked')) {
            mode += Math.pow(2, 8 - i);
        }
    }
    // Below function is registered from n2o context
    submit_chmod_event([mode, $('#chbx_recursive').is(':checked')]);
};





