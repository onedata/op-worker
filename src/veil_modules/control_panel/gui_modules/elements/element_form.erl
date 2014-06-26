%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains element definition for simplest HTML form.
%% @end
%% ===================================================================

-module(element_form).
-include("veil_modules/control_panel/vcn_common.hrl").
-export([render_element/1]).

render_element(Record) ->
    wf_tags:emit_tag(<<"form">>, wf:render(Record#form.body), [
        {<<"id">>, Record#form.id},
        {<<"class">>, Record#form.class},
        {<<"style">>, Record#form.style},
        {<<"action">>, Record#form.action},
        {<<"method">>, Record#form.method},
        {<<"enctype">>, Record#form.enctype}
    ]).