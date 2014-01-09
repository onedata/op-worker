%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains element definition for simplest HTML form
%% @end
%% ===================================================================

-module(form).
-include("veil_modules/control_panel/common.hrl").
-export([
    reflect/0,
    render_element/1
]).

reflect() -> record_info(fields, form).

render_element(Record) ->
    wf_tags:emit_tag(form, Record#form.body, [
        {class, Record#form.class},
        wf_tags:html_name(Record#form.id,
                          Record#form.html_name),
        {action, Record#form.action},
        {method, Record#form.method},
        {enctype, Record#form.enctype}
    ]).