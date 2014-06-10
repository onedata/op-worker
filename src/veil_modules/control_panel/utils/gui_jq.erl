%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains page manipulation and asynchronous updates
%% functions based on jquery.
%% IMPORTANT: n2o's wf module must not be used directly!
%% These functions are a wrapper to that module, which gives control over
%% such aspects as javascript escaping.
%% @end
%% ===================================================================

-module(gui_jq).
%% -include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

% General javascript wiring
-export([wire/1]).

% DOM updates
-export([update/2, replace/2, insert_top/2, insert_bottom/2, insert_before/2, insert_after/2, remove/1]).


%% wire/1
%% ====================================================================
%% @doc Sends a javascript code snippet to the client for immediate evaluation.
%% NOTE! Does not js_escape the script, the developer has to make sure 
%% the wired javascript is safe, or use DOM manipulation functions, which 
%% include safe escaping.
%% @end
-spec wire(string() | binary()) -> ok.
%% ====================================================================
wire(Script) ->
    wf:wire(gui_convert:to_list(Script)).


%% update/2
%% ====================================================================
%% @doc Updates contents of a DOM element.
%% @end
-spec update(ID :: binary(), Content :: term()) -> ok.
%% ====================================================================
update(Target, Elements) ->
    RenderedElements = gui_convert:js_escape(wf:render(Elements)),
    Script = <<"$('#", Target/binary, "').html(", RenderedElements/binary, ");">>,
    wire(Script).


%% replace/2
%% ====================================================================
%% @doc Replaces a DOM element with another.
%% @end
-spec replace(ID :: binary(), Content :: term()) -> ok.
%% ====================================================================
replace(Target, Elements) ->
    RenderedElements = gui_convert:js_escape(wf:render(Elements)),
    Script = <<"$('#", Target/binary, "').replaceWith(", RenderedElements/binary, ");">>,
    wire(Script).


%% insert_top/2
%% ====================================================================
%% @doc Prepends an element to a DOM element.
%% @end
-spec insert_top(ID :: binary(), Content :: term()) -> ok.
%% ====================================================================
insert_top(Target, Elements) ->
    RenderedElements = gui_convert:js_escape(wf:render(Elements)),
    Script = <<"$('#", Target/binary, "').prepend(", RenderedElements/binary, ");">>,
    wire(Script).


%% insert_bottom/2
%% ====================================================================
%% @doc Appends an element to a DOM element.
%% @end
-spec insert_bottom(ID :: binary(), Content :: term()) -> ok.
%% ====================================================================
insert_bottom(Target, Elements) ->
    RenderedElements = gui_convert:js_escape(wf:render(Elements)),
    Script = <<"$('#", Target/binary, "').append(", RenderedElements/binary, ");">>,
    wire(Script).


%% insert_before/2
%% ====================================================================
%% @doc Inserts an element before a DOM element.
%% @end
-spec insert_before(ID :: binary(), Content :: term()) -> ok.
%% ====================================================================
insert_before(Target, Elements) ->
    RenderedElements = gui_convert:js_escape(wf:render(Elements)),
    Script = <<"$('#", Target/binary, "').before(", RenderedElements/binary, ");">>,
    wire(Script).


%% insert_after/2
%% ====================================================================
%% @doc Inserts an element after a DOM element.
%% @end
-spec insert_after(ID :: binary(), Content :: term()) -> ok.
%% ====================================================================
insert_after(Target, Elements) ->
    RenderedElements = gui_convert:js_escape(wf:render(Elements)),
    Script = <<"$('#", Target/binary, "').after(", RenderedElements/binary, ");">>,
    wire(Script).


%% remove/1
%% ====================================================================
%% @doc Updates an element from DOM.
%% @end
-spec remove(ID :: binary()) -> ok.
%% ====================================================================
remove(Target) ->
    Script = <<"$('#", Target/binary, "').remove();">>,
    wire(Script).

