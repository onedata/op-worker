%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains element definition for bootstrap-compliant button
%% @end
%% ===================================================================

-module(bootstrap_button).
-include("veil_modules/control_panel/common.hrl").
-export([
    reflect/0,
    render_element/1
]).

reflect() -> record_info(fields, bootstrap_button).

render_element(Record) ->
    ID = Record#bootstrap_button.id,
    Anchor = Record#bootstrap_button.anchor,
    case Record#bootstrap_button.postback of
        undefined -> ignore;
        Postback -> wf:wire(Anchor, #event { type=click, validation_group=ID, postback=Postback, delegate=Record#bootstrap_button.delegate })
    end,

    case Record#bootstrap_button.click of
        undefined -> ignore;
        ClickActions -> wf:wire(Anchor, #event { type=click, actions=ClickActions })
    end,

    Value = ["  ", wf:html_encode(Record#bootstrap_button.text, Record#bootstrap_button.html_encode), "  "], 

    DataToggleAttributes = 
    	case Record#bootstrap_button.data_toggle of
    		[] -> [];
    		_ ->
    			[
    				{'data-toggle', Record#bootstrap_button.data_toggle},
    				{'data-target', Record#bootstrap_button.data_target}
    			]
    	end,

    UniversalAttributes = [
        {id, Record#bootstrap_button.html_id},
        {title, Record#bootstrap_button.title},
        {class, [bootstrap_button, Record#bootstrap_button.class]},
        {style, Record#bootstrap_button.style},
        {value, Value},
        {type, Record#bootstrap_button.type}
    ] ++ DataToggleAttributes,

    wf_tags:emit_tag(button, [Record#bootstrap_button.body], UniversalAttributes).