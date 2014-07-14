%%%-------------------------------------------------------------------
%%% @author RoXeon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Jul 2014 23:56
%%%-------------------------------------------------------------------
-module(registry_spaces).
-author("RoXeon").

-include_lib("ctool/include/logging.hrl").
-include("veil_modules/dao/dao_vfs.hrl").

%% API
-export([get_space_info/1]).


%% ====================================================================
%% API functions
%% ====================================================================


get_space_info(SpaceId) ->
    case request(get, "spaces/" ++ SpaceId) of
        {ok, Response} ->
            ?info("Resp: ~p", [Response]),
            Json = jiffy:decode(Response, [return_maps]),
            #{<<"name">> := SpaceName} = Json,
            {ok, #space_info{uuid = SpaceId, name = SpaceName}};
        {error, Reason} ->
            {error, Reason}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

request(Method, URI) ->
    request(Method, URI, "").
request(Method, URI, Body) ->
    URL = "https://globalregistry.org/" ++ URI,
    case httpc:request(Method, {URL, [], "application/json", Body},
            [{ssl, [{verify_peer, none}, {certfile, get_provider_cert_path()}, {keyfile, get_provider_key_path()}]}], []) of
        {ok, {{_, 200, _}, _, Response}} -> {ok, Response};
        {ok, {{_, 404, _}, _, _}} -> {error, not_found};
        {ok, Other} -> {error, {invalid_status, Other}};
        {error, Reason} -> {error, Reason}
    end.

get_provider_cert_path() ->
    "./certs/grpcert.pem".

get_provider_key_path() ->
    "./certs/grpkey.pem".