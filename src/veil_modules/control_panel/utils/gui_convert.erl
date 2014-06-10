%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains various conversion functions.
%% @end
%% ===================================================================

-module(gui_convert).
-include("logging.hrl").

% Conversion utils
-export([to_list/1, to_binary/1, join_to_binary/1, js_escape/1]).


%% to_list/1
%% ====================================================================
%% @doc Converts any term to list.
%% @end
-spec to_list(term()) -> list().
%% ====================================================================
to_list(undefined) -> [];
to_list(Term) when is_list(Term) -> Term;
to_list(Term) when is_binary(Term) -> binary_to_list(Term);
to_list(Term) ->
    try
        wf:to_list(Term)
    catch _:_ ->
        lists:flatten(io_lib:format("~p", [Term]))
    end.


%% to_binary/1
%% ====================================================================
%% @doc Converts any term to binary.
%% @end
-spec to_binary(term()) -> binary().
%% ====================================================================
to_binary(Term) when is_binary(Term) -> Term;
to_binary(Term) -> list_to_binary(to_list(Term)).


%% join_to_binary/1
%% ====================================================================
%% @doc Joins any terms to a js-escaped binary.
%% @end
-spec join_to_binary([term()]) -> binary().
%% ====================================================================
join_to_binary(Terms) ->
    join_to_binary(Terms, <<"">>).

join_to_binary([], Acc) ->
    Acc;

join_to_binary([H | T], Acc) ->
    join_to_binary(T, <<Acc/binary, (to_binary(H))/binary>>).


%% js_escape/1
%% ====================================================================
%% @doc Escapes all javascript - sensitive characters.
%% @end
-spec join_to_binary([term()]) -> binary().
%% ====================================================================
js_escape(Term) ->
    wf:js_escape(Term).