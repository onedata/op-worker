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
    assert_request_error/2,
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

assert_request_error(ExpectedError = {error, _}, RequestParams) ->
    #rest_resp{code = ExpCode, body = ExpBody} = rest_translator:error_response(ExpectedError),
    assert_request_error({ExpCode, ExpBody}, RequestParams);
assert_request_error(_ExpectedError = {ExpectedCode, ExpectedBody},
    _RequestParams = {Node, URL, Method, Headers, Body}) ->
    assert_request_error({ExpectedCode, ExpectedBody}, {Node, URL, Method, Headers, Body, []});
assert_request_error(_ExpectedError = {ExpectedCode, ExpectedBody},
    _RequestParams = {Node, URL, Method, Headers, Body, Opts}) ->
    Response = request(Node, URL, Method, Headers, Body, Opts),
    {ok, RespCode, _, RespBody} = ?assertMatch({ok, _RespCode, _, _RespBody}, Response),

    CodeMatched = case ExpectedCode == RespCode of
        true ->
            true;
        false ->
            ct:pal("Wrong response code: ~n"
            "    Expected: ~tp~n"
            "    Got: ~tp~n", [ExpectedCode, RespCode]),
            print_request(Node, URL, Method, Headers, Body),
            false
    end,

    BodyMatched = case ExpectedBody of
        {binary, ExpBin} ->
            ExpBin == RespBody;
        _ ->
            ExpectedBody == json_utils:decode(RespBody)
    end,

    case BodyMatched of
        true ->
            true;
        false ->
            ct:pal("Wrong response body: ~n"
            "Expected: ~tp~n"
            "Got: ~tp~n", [ExpectedBody, RespBody]),
            print_request(Node, URL, Method, Headers, Body),
            false
    end,
    CodeMatched andalso BodyMatched.

get_rest_error(Error) ->
    #rest_resp{code = ExpCode, body = ExpBody} = rest_translator:error_response(Error),
    {ExpCode, ExpBody}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

rest_endpoint(Node) ->
    Port = case get(port) of
        undefined ->
            PStr = case opw_test_rpc:get_env(Node, https_server_port) of
                443 -> <<"">>;
                P when is_integer(P) -> <<":", (integer_to_binary(P))/binary>>
            end,
            put(port, PStr),
            PStr;
        P -> P
    end,
    Domain = opw_test_rpc:get_provider_domain(Node),
    <<"https://", (str_utils:to_binary(Domain))/binary, Port/binary, "/api/v3/oneprovider/">>.

print_request(URL, Method, Headers, Body, Opts) ->
    ct:pal("Failed for request: ~n"
    "   ReqMethod: ~tp~n"
    "   URL: ~tp~n"
    "   Headers: ~tp~n"
    "   ReqBody: ~tp~n"
    "   Opts: ~tp~n", [
        Method, URL, Headers, Body, Opts
    ]).
