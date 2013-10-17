%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains custom nitrogen elements
%% @end
%% ===================================================================

% Button with additional fields
-record(bootstrap_button, {?ELEMENT_BASE(bootstrap_button), 
    text="", 
    title,
    html_encode=true, 
    click, 
    postback, 
    delegate,
    data_toggle="",
    data_target="",
    type=button,
    body=""
}).


% Checkbox with additional fields
-record(bootstrap_checkbox, {?ELEMENT_BASE(bootstrap_checkbox),
    text="", 
    html_encode=true, 
    checked=false, 
    value="on", 
    postback, 
    delegate, 
    html_name,
    data_toggle=""
}).


% Simplest HTML form
-record(form, {?ELEMENT_BASE(form),
    method, 
    action, 
    html_name, 
    enctype, 
    body=[]
}).


% Custom upload element
-record(veil_upload, {?ELEMENT_BASE(veil_upload),
    delegate,
    tag,
    show_button=true,
    file_text="Select files",
    button_text="Start upload", 
    droppable=false,
    droppable_text="Drop files", 
    multiple=false,
    target_dir="/"
}).
