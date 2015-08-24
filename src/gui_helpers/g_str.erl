%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Aug 2015 13:07
%%%-------------------------------------------------------------------
-module(g_str).
-author("lopiola").

%% API
-export([encode_to_json/1, decode_from_json/1]).


%%--------------------------------------------------------------------
%% @doc
%% Convenience function that convert an erlang term to JSON, producing
%% binary result. The output is in UTF8 encoding.
%% Possible terms, can be nested:
%% {struct, Props} - Props is a structure as a proplist, e.g.: [{id, 13}, {message, "mess"}]
%% {Props} - alias for above
%% {array, Array} - Array is a list, e.g.: [13, "mess"]
%% @end
%%--------------------------------------------------------------------
-spec encode_to_json(term()) -> binary().
encode_to_json(Term) ->
    Encoder = mochijson2:encoder([{utf8, true}]),
    iolist_to_binary(Encoder(Term)).


%%--------------------------------------------------------------------
%% @doc
%% Convenience function that convert JSON binary to an erlang term.
%% @end
%%--------------------------------------------------------------------
-spec decode_from_json(binary()) -> term().
decode_from_json(JSON) ->
    try mochijson2:decode(JSON, [{format, proplist}]) catch _:_ -> throw(invalid_json) end.