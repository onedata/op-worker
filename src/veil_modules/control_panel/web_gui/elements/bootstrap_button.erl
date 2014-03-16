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
    Id = case Record#button.postback of
             undefined -> Record#button.id;
             Postback ->
                 ID = case Record#button.id of undefined -> wf:temp_id(); I -> I end,
                 wf:wire(#event{type=click, postback=Postback, target=ID,
                 source=Record#button.source, delegate=Record#button.delegate }),
                 ID end,

    wf_tags:emit_tag(<<"button">>, wf:render(Record#button.body), [
        {<<"id">>, Id},
        {<<"title">>, Record#bootstrap_button.title},
        {<<"type">>, Record#button.type},
        {<<"name">>, Record#button.name},
        {<<"class">>, Record#button.class},
        {<<"style">>, Record#button.style},
        {<<"disabled">>, if Record#button.disabled == true -> "disabled"; true -> undefined end},
        {<<"value">>, Record#button.value}  | Record#button.data_fields ]).
