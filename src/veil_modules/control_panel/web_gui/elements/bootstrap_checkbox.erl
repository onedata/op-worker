%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains element definition for bootstrap-compliant checkbox
%% @end
%% ===================================================================

-module(bootstrap_checkbox).
-include("veil_modules/control_panel/common.hrl").
-export([
    reflect/0,
    render_element/1
]).

reflect() -> record_info(fields, bootstrap_checkbox).

render_element(Record) -> 
    ID = Record#bootstrap_checkbox.id,
    Anchor = case Record#bootstrap_checkbox.anchor of
        "." ++ AnchorNoDot -> AnchorNoDot;
        A -> A
    end,
    CheckedOrNot = case Record#bootstrap_checkbox.checked of
        true -> checked;
        _ -> not_checked
    end,
    case Record#bootstrap_checkbox.postback of
        undefined -> ignore;
        Postback -> wf:wire(Anchor, #event { type=change, postback=Postback, validation_group=ID, delegate=Record#bootstrap_checkbox.delegate })
    end,

    wf_tags:emit_tag(input, [""], [
        {name, Record#bootstrap_checkbox.html_name},
        {id,   Anchor},
        {type, checkbox},
        {class, [bootstrap_checkbox, Record#bootstrap_checkbox.class]},
        {style, Record#bootstrap_checkbox.style},
        {value, Record#bootstrap_checkbox.value},
        {'data-toggle', Record#bootstrap_checkbox.data_toggle},
        {CheckedOrNot, true}
    ]).
