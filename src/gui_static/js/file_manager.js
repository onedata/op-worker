// ===================================================================
// Author: Lukasz Opiola
// Copyright (C): 2014 ACK CYFRONET AGH
// This software is released under the MIT license
// cited in 'LICENSE.txt'.
// ===================================================================
// This file contains JS functions used on file_manager page.
// ===================================================================

// -----------------------------
// chmod handling

// IDs of checkboxes used to set mode
var CHMOD_CHECKBOXES = ['chbx_ur', 'chbx_uw', 'chbx_ux',
    'chbx_gr', 'chbx_gw', 'chbx_gx',
    'chbx_or', 'chbx_ow', 'chbx_ox'];

// Indicates if checkbox change event was generated by the user
var user_event = true;

// Initialize chmod table with checkboxes and add event listeners
init_chmod_table = function (current_mode) {
    // Init checkboxes
    for (var i = 0; i < CHMOD_CHECKBOXES.length; i++) {
        var chbx = $('#' + CHMOD_CHECKBOXES[i]);
        chbx.checkbox();
        chbx.change(function (event) {
            if (user_event) {
                update_chmod_textbox(calculate_mode());
            }
        });
    }
    $('#chbx_recursive').checkbox();

    // Set checkboxes state and textbox value
    update_chmod_checkboxes(current_mode);
    update_chmod_textbox(current_mode);

    $('#octal_form_textbox').keyup(function (event) {
        var textbox_value = $('#octal_form_textbox').val();
        if (textbox_value.length == 3) {
            var digit_1 = parseInt(textbox_value[0]);
            var digit_2 = parseInt(textbox_value[1]);
            var digit_3 = parseInt(textbox_value[2]);
            if (!isNaN(digit_1) && !isNaN(digit_2) && !isNaN(digit_3) &&
                digit_1 >= 0 && digit_1 <= 7 &&
                digit_2 >= 0 && digit_2 <= 7 &&
                digit_3 >= 0 && digit_3 <= 7) {
                var mode_oct = parseInt(textbox_value, 8);
                update_chmod_checkboxes(mode_oct);
            }
        }
    });
};

// Submit newly chosen mode to the server
submit_chmod = function () {
    $('#spinner').delay(150).show();
    // Below function is registered from n2o context
    submit_chmod_event([calculate_mode(), $('#chbx_recursive').is(':checked')]);
};

// Calculate mode depending on checked checkboxes
calculate_mode = function () {
    var mode = 0;
    for (var i = 0; i < CHMOD_CHECKBOXES.length; i++) {
        if ($('#' + CHMOD_CHECKBOXES[i]).is(':checked')) {
            mode += Math.pow(2, 8 - i);
        }
    }
    return mode;
};

// Update all checkboxes to represent given mode
update_chmod_checkboxes = function (mode) {
    // Make sure this doesn't trigger change events
    user_event = false;
    for (var i = 0; i < CHMOD_CHECKBOXES.length; i++) {
        var chbx = $('#' + CHMOD_CHECKBOXES[i]);
        if ((mode & Math.pow(2, 8 - i)) > 0) {
            chbx.checkbox('check');
        } else {
            chbx.checkbox('uncheck');
        }
    }
    user_event = true;
};

// Update value displayed in octal form textbox
update_chmod_textbox = function (mode) {
    var mode_string = mode.toString(8);
    $('#octal_form_textbox').val((mode_string + "00").substring(0, 3));
};

