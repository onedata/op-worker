%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains custom n2o elements
%% @end
%% ===================================================================

-ifndef(CUSTOM_ELEMENTS_HRL).
-define(CUSTOM_ELEMENTS_HRL, 1).

% Custom jquery action
-record(jquery, {?ACTION_BASE(action_jquery),
    property,
    method,
    args = [],
    right,
    format = "~s"}).


% Simplest HTML form
-record(form, {?ELEMENT_BASE(element_form),
    method,
    action,
    html_name,
    enctype
}).


% Custom upload element
-record(veil_upload, {?ELEMENT_BASE(veil_upload),
    delegate, % Required to get start / finish reports in given module (will call Delegate:upload_event/1).
    target_dir = <<"/">>,
    file_text = <<"Select files">>,
    button_text = <<"Start upload">>,
    droppable_text = <<"Drop files">>
}).

-endif.