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

    var checkbox_recursive = $('#chbx_recursive');
    checkbox_recursive.checkbox();
    checkbox_recursive.change(function (e) {
        if (checkbox_recursive.is(':checked')) {
            $('#perms_warning_overwrite').show()
        } else {
            $('#perms_warning_overwrite').hide()
        }
    });

    // Set checkboxes state and textbox value
    update_chmod_checkboxes(current_mode);
    update_chmod_textbox(current_mode);

    $('#posix_octal_form_textbox').keyup(function (event) {
        var textbox_value = $('#posix_octal_form_textbox').val();
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

    // Initialize tabs
    $('.nav-tabs a').on('click', function (e) {
        e.preventDefault();
        $(this).tab("show");
    });
};

// Submit newly chosen mode to the server
submit_perms = function () {
    $('#spinner').delay(150).show();
    submit_perms_event([calculate_mode(), $('#chbx_recursive').is(':checked')]);
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
    $('#posix_octal_form_textbox').val((mode_string + "00").substring(0, 3));
};


// -----------------------------
// ACL handling

var clicked_index = -2;

populate_acl_list = function (json_array, select_index) {
    var acl_list = $('#acl-list');
    acl_list.html('');

    for (var i = 0; i < json_array.length; ++i) {
        acl_list.append(
            render_acl_entry(i, json_array[i].identifier, json_array[i].allow, json_array[i].read, json_array[i].write, json_array[i].exec));
    }

    acl_list.append('<div class="acl-entry" index="-1">' +
        '<a class="glyph-link acl-add-button" title="New ACL Entry"><span class="icomoon-plus"></span></a>' +
        '<span class="acl-info-add">New ACL entry...</span>' +
        '</div>');

    if (select_index > -1) {
        clicked_index = select_index;
        $(acl_list.find('.acl-entry')[select_index]).find('[class*="acl-button-"]').show();
    }

    $('.acl-entry').click(function (event) {
        if ($('#acl-form').css('display') == 'none') {
            document.getSelection().removeAllRanges();
            var new_index = parseInt($(this).attr('index'));
            if (clicked_index != new_index) {
                clicked_index = new_index;
                $('[class*="acl-button-"]').hide();
                $(this).find('[class*="acl-button-"]').show();
            } else {
                $('[class*="acl-button-"]').hide();
                if (clicked_index == -1) {
                    add_acl_event();
                } else {
                    $(this).addClass('acl-entry-selected');
                    edit_acl_event(clicked_index);
                }
            }
        }
    });

    $('.acl-add-button').click(function (event) {
        event.stopPropagation();
        $('[class*="acl-button-"]').hide();
        clicked_index = -1;
        add_acl_event();
    });

    $('.acl-button-delete').click(function (event) {
        event.stopPropagation();
        var entry_div = $(this).parent();
        entry_div.find('[class*="acl-button-"]').hide();
        entry_div.find('[class*="acl-icon-"]').hide();
        entry_div.find('[class*="acl-symbol-"]').hide();
        entry_div.find('[class*="acl-confirm-"]').show();
    });

    $('.acl-confirm-yes').click(function (event) {
        event.stopPropagation();
        delete_acl_event(parseInt($(this).parent().attr('index')));
    });

    $('.acl-confirm-no').click(function (event) {
        event.stopPropagation();
        var entry_div = $(this).parent();
        entry_div.find('[class*="acl-button-"]').show();
        entry_div.find('[class*="acl-icon-"]').show();
        entry_div.find('[class*="acl-symbol-"]').show();
        entry_div.find('[class*="acl-confirm-"]').hide();
    });

    $('.acl-button-edit').click(function (event) {
        event.stopPropagation();
        var entry_div = $(this).parent();
        entry_div.addClass('acl-entry-selected');
        entry_div.find('[class*="acl-button-"]').hide();
        edit_acl_event(parseInt(entry_div.attr('index')));
    });

    $('.acl-button-move-up').click(function (event) {
        event.stopPropagation();
        move_acl_event([parseInt($(this).parent().attr('index')), true]);
    });

    $('.acl-button-move-down').click(function (event) {
        event.stopPropagation();
        move_acl_event([parseInt($(this).parent().attr('index')), false]);
    });


    $('#acl_type_checkbox').change(function (event) {
        if ($(this).is(':checked')) {
            $('#acl_type_checkbox_label').html('allow');
        } else {
            $('#acl_type_checkbox_label').html('deny');
        }
    });
};

// Renders a single ACL entry
render_acl_entry = function (index, identifier, allow_flag, read_flag, write_flag, exec_flag) {
    var entry_class = allow_flag ? 'acl-entry acl-entry-allow' : 'acl-entry acl-entry-deny';
    var icon_type = allow_flag ? 'fui-check-inverted' : 'fui-cross-inverted';
    var icon_read = read_flag ? '<span class="' + icon_type + ' acl-icon-read"></span>' +
        '<span class="acl-symbol-read">R</span>' : '';
    var icon_write = write_flag ? '<span class="' + icon_type + ' acl-icon-write"></span>' +
        '<span class="acl-symbol-write">W</span>' : '';
    var icon_exec = exec_flag ? '<span class="' + icon_type + ' acl-icon-exec"></span>' +
        '<span class="acl-symbol-exec">X</span>' : '';
    var icons_confirm_delete = '<span class="acl-confirm-prompt">Are you sure?</span>' +
        '<a class="glyph-link acl-confirm-yes" title="Yes"><span class="icomoon-checkmark"></span></a>' +
        '<a class="glyph-link acl-confirm-no" title="No"><span class="icomoon-close"></span></a>';

    return '<div class="' + entry_class + '" index="' + index + '"><div class="acl-identifier">' +
        '<span class="icomoon-user acl-ident-icon"></span>' +
        '<span class="acl-ident-name">' + identifier + '</span></div>' +
        icon_read + icon_write + icon_exec +
        '<a class="glyph-link acl-button-delete" title="Delete ACL entry"><span class="icomoon-remove"></span></a>' +
        '<a class="glyph-link acl-button-edit" title="Edit ACL entry"><span class="icomoon-pencil2"></span></a>' +
        '<a class="glyph-link acl-button-move-up" title="Move up"><span class="fui-triangle-up-small"></span></a>' +
        '<a class="glyph-link acl-button-move-down" title="Move down"><span class="fui-triangle-down-small"></span></a>' +
        icons_confirm_delete + '</div>';
};

// Submits ACL form
submit_acl = function () {
    $('#spinner').delay(150).show();
    submit_acl_event([
        clicked_index,
        $('#acl_select_name').val(),
        $('#acl_type_checkbox').is(':checked'),
        $('#acl_read_checkbox').is(':checked'),
        $('#acl_write_checkbox').is(':checked'),
        $('#acl_exec_checkbox').is(':checked')
    ]);
};