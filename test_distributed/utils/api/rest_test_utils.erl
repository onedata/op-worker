%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains utils functions for tests using op_worker
%%% REST API.
%%% @end
%%%-------------------------------------------------------------------
-module(rest_test_utils).
-author("Jakub Kudzia").

-include("middleware/middleware.hrl").
-include("http/rest.hrl").
-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([
    request/5, request/6,
    cacerts_opts/1,
    user_token_header/1,
    get_rest_error/1
]).

%%%===================================================================
%%% API
%%%===================================================================

request(Node, URL, Method, Headers, Body) ->
    request(Node, URL, Method, Headers, Body, [{recv_timeout, 60000}]).

request(Node, URL, Method, Headers, Body, Opts) ->
    Headers2 = case is_map(Headers) of
        true -> Headers;
        false -> maps:from_list(Headers)
    end,
    Result = http_client:request(
        Method, <<(rest_endpoint(Node))/binary, URL/binary>>,
        Headers2, Body, cacerts_opts(Node) ++ Opts
    ),
    case Result of
        {ok, RespCode, RespHeaders, RespBody} ->
            {ok, RespCode, maps:to_list(RespHeaders), RespBody};
        Other ->
            Other
    end.

cacerts_opts(Node) ->
    CaCerts = opw_test_rpc:call(Node, https_listener, get_cert_chain_ders, []),
    [{ssl_options, [{cacerts, CaCerts}]}].

user_token_header(AccessToken) ->
    case rand:uniform(3) of
        1 -> {?HDR_X_AUTH_TOKEN, AccessToken};
        2 -> {?HDR_AUTHORIZATION, <<"Bearer ", AccessToken/binary>>};
        %% @todo VFS-5554 Deprecated, included for backward compatibility
        3 -> {?HDR_MACAROON, AccessToken}
    end.

get_rest_error(Error) ->
    #rest_resp{code = ExpCode, body = ExpBody} = rest_translator:error_response(Error),
    {ExpCode, ExpBody}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

rest_endpoint(Node) ->
    Port = api_test_utils:get_https_server_port_str(Node),
    Domain = opw_test_rpc:get_provider_domain(Node),
    str_utils:format_bin("https://~ts~ts/api/v3/oneprovider/", [Domain, Port]).
