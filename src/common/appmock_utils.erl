%%
-module(appmock_utils).

%% API
-export([https_request/6, encode_to_json/1, decode_from_json/1]).


% Performs a single request using ibrowse
https_request(Hostname, Port, Path, Method, Headers, Body) ->
    {ok, Code, RespHeaders, RespBody} = ibrowse:send_req(
        "https://" ++ Hostname ++ ":" ++ integer_to_list(Port) ++ Path,
        Headers, Method, Body),
    {Code, RespHeaders, RespBody}.


%% encode_to_json/1
%% ====================================================================
%% @doc Convinience function that convert an erlang term to JSON, producing
%% binary result. The output is in UTF8 encoding.
%%
%% Possible terms, can be nested:
%% {struct, Props} - Props is a structure as a proplist, e.g.: [{id, 13}, {message, "mess"}]
%% {Props} - alias for above
%% {array, Array} - Array is a list, e.g.: [13, "mess"]
%% @end
-spec encode_to_json(term()) -> binary().
%% ====================================================================
encode_to_json(Term) ->
    Encoder = mochijson2:encoder([{utf8, true}]),
    iolist_to_binary(Encoder(Term)).


%% decode_from_json/1
%% ====================================================================
%% @doc Convinience function that convert JSON binary to an erlang term.
%% @end
-spec decode_from_json(binary()) -> term().
%% ====================================================================
decode_from_json(JSON) ->
    try mochijson2:decode(JSON, [{format, proplist}]) catch _:_ -> throw(invalid_json) end.