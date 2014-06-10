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
%% such aspects as javascript escaping and event wiring sequence.
%% @end
%% ===================================================================

-module(gui_jq).
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

% General javascript wiring
-export([wire/1, wire/2, wire/4]).

% DOM updates
-export([update/2, replace/2, insert_top/2, insert_bottom/2, insert_before/2, insert_after/2, remove/1]).

% Commonly used jquery functions
-export([show/1, hide/1, add_class/2, remove_class/2]).


%% wire/1
%% ====================================================================
%% @doc Sends a javascript code snippet to the client for immediate evaluation.
%% NOTE! Does not js_escape the script, the developer has to make sure 
%% the wired javascript is safe, or use DOM manipulation functions, which 
%% include safe escaping.
%% @end
-spec wire(Script :: string() | binary()) -> ok.
%% ====================================================================
wire(Script) ->
    wire(Script, false).


%% wire/2
%% ====================================================================
%% @doc Sends a javascript code snippet to the client for immediate evaluation.
%% NOTE! Does not js_escape the script, the developer has to make sure
%% the wired javascript is safe, or use DOM manipulation functions, which
%% include safe escaping.
%% Eager flag can be used. It is ensured that all eager actions
%% will be evaluated before normal actions.
%% @end
-spec wire(Script :: string() | binary(), Eager :: boolean()) -> ok.
%% ====================================================================
wire(Script, Eager) ->
    Actions = case get(actions) of undefined -> []; E -> E end,
    case Eager of
        true ->
            put(actions, [#wire{actions = gui_convert:to_list(Script)} | Actions]);
        false ->
            put(actions, Actions ++ [#wire{actions = gui_convert:to_list(Script)}])
    end.


%% wire/4
%% ====================================================================
%% @doc Convienience function to create javascript code.
%% Eager flag can be used.
%% @end
-spec wire(Target :: binary(), Method :: binary(), Args :: binary(), Eager :: boolean()) -> ok.
%% ====================================================================
wire(Target, Method, Args, Eager) ->
    Script = <<"$('#", Target/binary, "').", Method/binary, "('", Args/binary, "');">>,
    wire(Script, Eager).


%% update/2
%% ====================================================================
%% @doc Updates contents of a DOM element.
%% @end
-spec update(Target :: binary(), Content :: term()) -> ok.
%% ====================================================================
update(Target, Elements) ->
    RenderedElements = gui_convert:js_escape(wf:render(Elements)),
    wire(Target, <<"html">>, RenderedElements, true).


%% replace/2
%% ====================================================================
%% @doc Replaces a DOM element with another.
%% @end
-spec replace(Target :: binary(), Content :: term()) -> ok.
%% ====================================================================
replace(Target, Elements) ->
    RenderedElements = gui_convert:js_escape(wf:render(Elements)),
    wire(Target, <<"replaceWith">>, RenderedElements, true).


%% insert_top/2
%% ====================================================================
%% @doc Prepends an element to a DOM element.
%% @end
-spec insert_top(Target :: binary(), Content :: term()) -> ok.
%% ====================================================================
insert_top(Target, Elements) ->
    RenderedElements = gui_convert:js_escape(wf:render(Elements)),
    wire(Target, <<"prepend">>, RenderedElements, true).


%% insert_bottom/2
%% ====================================================================
%% @doc Appends an element to a DOM element.
%% @end
-spec insert_bottom(Target :: binary(), Content :: term()) -> ok.
%% ====================================================================
insert_bottom(Target, Elements) ->
    RenderedElements = gui_convert:js_escape(wf:render(Elements)),
    wire(Target, <<"append">>, RenderedElements, true).


%% insert_before/2
%% ====================================================================
%% @doc Inserts an element before a DOM element.
%% @end
-spec insert_before(Target :: binary(), Content :: term()) -> ok.
%% ====================================================================
insert_before(Target, Elements) ->
    RenderedElements = gui_convert:js_escape(wf:render(Elements)),
    wire(Target, <<"before">>, RenderedElements, true).


%% insert_after/2
%% ====================================================================
%% @doc Inserts an element after a DOM element.
%% @end
-spec insert_after(Target :: binary(), Content :: term()) -> ok.
%% ====================================================================
insert_after(Target, Elements) ->
    RenderedElements = gui_convert:js_escape(wf:render(Elements)),
    wire(Target, <<"after">>, RenderedElements, true).


%% remove/1
%% ====================================================================
%% @doc Removes an element from DOM.
%% @end
-spec remove(Target :: binary()) -> ok.
%% ====================================================================
remove(Target) ->
    wire(Target, <<"remove">>, <<"">>, true).


%% show/1
%% ====================================================================
%% @doc Displays an HTML element.
%% @end
-spec show(Target :: binary()) -> ok.
%% ====================================================================
show(Target) ->
    wire(Target, <<"show">>, <<"">>, false).


%% hide/1
%% ====================================================================
%% @doc Hides an HTML element.
%% @end
-spec hide(Target :: binary()) -> ok.
%% ====================================================================
hide(Target) ->
    wire(Target, <<"hide">>, <<"">>, false).


%% add_class/2
%% ====================================================================
%% @doc Adds a class to an HTML element.
%% @end
-spec add_class(Target :: binary(), Class :: binary()) -> ok.
%% ====================================================================
add_class(Target, Class) ->
    wire(Target, <<"addClass">>, <<"\"", Class/binary, "\"">>, false).


%% remove_class/2
%% ====================================================================
%% @doc Removes a class from an HTML element.
%% @end
-spec remove_class(Target :: binary(), Class :: binary()) -> ok.
%% ====================================================================
remove_class(Target, Class) ->
    wire(Target, <<"removeClass">>, <<"\"", Class/binary, "\"">>, false).

