%%%-------------------------------------------------------------------
%%% @author RoXeon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Jul 2014 15:36
%%%-------------------------------------------------------------------
-module(global_registry).
-author("RoXeon").

%% API
-export([provider_request/2, provider_request/3, user_request/3, user_request/4]).
-export([get_provider_cert_path/0, get_provider_key_path/0]).

provider_request(Method, URI) ->
    request(Method, URI, <<"">>, []).

provider_request(Method, URI, Body) ->
    request(Method, URI, Body, []).

user_request(Token, Method, URI) ->
    user_request(Token, Method, URI, <<"">>).

user_request(Token, Method, URI, Body) ->
    TokenBin = vcn_utils:ensure_binary(Token),
    request(Method, URI, Body, [{"authorization", binary_to_list(<<"Bearer ", TokenBin/binary>>)}]).


request(Method, URI, Body, Headers) ->
    URL = "https://globalregistry.org:8443/" ++ URI,
    case ibrowse:send_req(URL, [{"Content-Type", "application/json"}] ++ Headers, Method, Body,
        [{ssl_options, [{verify, verify_none}, {certfile, get_provider_cert_path()}, {keyfile, get_provider_key_path()}]}]) of
        {ok, "200", _, Response} -> {ok, jiffy:decode(Response, [return_maps])};
        {ok, "404", _, _} -> {error, not_found};
        {ok, Status, _, _} -> {error, {invalid_status, Status}};
        {error, Reason} -> {error, Reason}
    end.

get_provider_cert_path() ->
    "./certs/grpcert.pem".

get_provider_key_path() ->
    "./certs/grpkey.pem".

