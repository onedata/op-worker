%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains various functions operating on strings (both lists and binaries).
%% @end
%% ===================================================================

-module(gui_str).
-include_lib("ctool/include/logging.hrl").

% Conversion
-export([to_list/1, to_binary/1, join_to_binary/1]).

% Formatting, escaping and encoding
-export([format/2, format_bin/2, js_escape/1, html_encode/1, url_encode/1, url_decode/1]).


%% ====================================================================
%% API functions
%% ====================================================================

%% to_list/1
%% ====================================================================
%% @doc Converts any term to list.
%% @end
-spec to_list(Term :: term()) -> list().
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
-spec to_binary(Term :: term()) -> binary().
%% ====================================================================
to_binary(Term) when is_binary(Term) -> Term;
to_binary(Term) -> list_to_binary(to_list(Term)).


%% join_to_binary/1
%% ====================================================================
%% @doc Joins any terms to a binary.
%% @end
-spec join_to_binary(List :: [term()]) -> binary().
%% ====================================================================
join_to_binary(Terms) ->
    join_to_binary(Terms, <<"">>).

join_to_binary([], Acc) ->
    Acc;

join_to_binary([H | T], Acc) ->
    join_to_binary(T, <<Acc/binary, (to_binary(H))/binary>>).


%% format/2
%% ====================================================================
%% @doc Escapes all javascript - sensitive characters.
%% @end
-spec format(Format :: string(), Args :: [term()]) -> binary().
%% ====================================================================
format(Format, Args) ->
    wf:f(Format, Args).


%% format_bin/2
%% ====================================================================
%% @doc Escapes all javascript - sensitive characters.
%% @end
-spec format_bin(Format :: string(), Args :: [term()]) -> binary().
%% ====================================================================
format_bin(Format, Args) ->
    to_binary(wf:f(Format, Args)).


%% js_escape/1
%% ====================================================================
%% @doc Escapes all javascript - sensitive characters.
%% @end
-spec js_escape(String :: binary() | string()) -> binary().
%% ====================================================================
js_escape(undefined) -> <<"">>;
js_escape(Value) when is_list(Value) -> js_escape(iolist_to_binary(Value));
js_escape(Value) -> js_escape(Value, <<"">>).
js_escape(<<"\\", Rest/binary>>, Acc) -> js_escape(Rest, <<Acc/binary, "\\\\">>);
js_escape(<<"\r", Rest/binary>>, Acc) -> js_escape(Rest, <<Acc/binary, "\\r">>);
js_escape(<<"\n", Rest/binary>>, Acc) -> js_escape(Rest, <<Acc/binary, "\\n">>);
js_escape(<<"\"", Rest/binary>>, Acc) -> js_escape(Rest, <<Acc/binary, "\\\"">>);
js_escape(<<"'", Rest/binary>>, Acc) -> js_escape(Rest, <<Acc/binary, "\\'">>);
js_escape(<<"<script", Rest/binary>>, Acc) -> js_escape(Rest, <<Acc/binary, "&lt;script">>);
js_escape(<<"script>", Rest/binary>>, Acc) -> js_escape(Rest, <<Acc/binary, "script&gt;">>);
js_escape(<<C, Rest/binary>>, Acc) -> js_escape(Rest, <<Acc/binary, C>>);
js_escape(<<"">>, Acc) -> Acc.


%% html_encode/1
%% ====================================================================
%% @doc Performs safe URL encoding
%% @end
-spec html_encode(String :: binary() | string()) -> binary().
%% ====================================================================
html_encode(String) ->
    to_binary(wf:html_encode(to_list(String))).


%% url_encode/1
%% ====================================================================
%% @doc Performs safe URL encoding
%% @end
-spec url_encode(String :: binary() | string()) -> binary().
%% ====================================================================
url_encode(String) ->
    to_binary(wf:url_encode(to_list(String))).


%% url_decode/1
%% ====================================================================
%% @doc Performs URL-uncoded string decoding
%% @end
-spec url_decode(String :: binary() | string()) -> binary().
%% ====================================================================
url_decode(String) ->
    to_binary(wf:url_decode(to_list(String))).