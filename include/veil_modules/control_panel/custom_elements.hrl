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


% Simplest HTML form
-record(form, {?ELEMENT_BASE(element_form),
    method,
    action,
    html_name,
    enctype
}).


% Custom upload element
-record(veil_upload, {?ELEMENT_BASE(veil_upload),
    subscriber_pid, % Required to get start / finish reports.
    target_dir = <<"/">>
}).

-endif.