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

-include("http/rest/rest.hrl").
-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([request/5, request/6, user_token_header/2, assert_request_error/2]).

%%%===================================================================
%%% API
%%%===================================================================

request(Node, URL, Method, Headers, Body) ->
    request(Node, URL, Method, Headers, Body, [{recv_timeout, 15000}]).

request(Node, URL, Method, Headers, Body, Opts) ->
    CaCerts = rpc:call(Node, https_listener, get_cert_chain_pems, []),
    Opts2 = [{ssl_options, [{cacerts, CaCerts}]} | Opts],
    Result = http_client:request(
        Method, <<(rest_endpoint(Node))/binary, URL/binary>>,
        maps:from_list(Headers), Body, Opts2
    ),
    case Result of
        {ok, RespCode, RespHeaders, RespBody} ->
            {ok, RespCode, maps:to_list(RespHeaders), RespBody};
        Other ->
            Other
    end.

user_token_header(Config, User)  ->
    #macaroon_auth{macaroon = Macaroon} = ?config({auth, User}, Config),
    case rand:uniform(3) of
        1 -> {<<"Macaroon">>, Macaroon};
        2 -> {<<"X-Auth-Token">>, Macaroon};
        3 -> {<<"Authorization">>, <<"Bearer ", Macaroon/binary>>}
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
                   "    Expected: ~p~n"
                   "    Got: ~p~n", [ExpectedCode, RespCode]),
            print_request(Node, URL, Method, Headers, Body),
            false
    end,

    DecodedBody = json_utils:decode(RespBody),
    BodyMatched = case ExpectedBody == DecodedBody of
        true ->
            true;
        false ->
            ct:pal("Wrong response body: ~n"
            "Expected: ~p~n"
            "Got: ~p~n", [ExpectedBody, DecodedBody]),
            print_request(Node, URL, Method, Headers, Body),
            false
    end,
    CodeMatched andalso BodyMatched.

%%%===================================================================
%%% Internal functions
%%%===================================================================

rest_endpoint(Node) ->
    Port = case get(port) of
        undefined ->
            {ok, P} = test_utils:get_env(Node, ?APP_NAME, https_server_port),
            PStr = case P of
                443 -> <<"">>;
                _ -> <<":", (integer_to_binary(P))/binary>>
            end,
            put(port, PStr),
            PStr;
        P -> P
    end,
    {ok, Domain} = test_utils:get_env(Node, ?APP_NAME, test_web_cert_domain),
    <<"https://", (str_utils:to_binary(Domain))/binary, Port/binary, "/api/v3/oneprovider/">>.

print_request(URL, Method, Headers, Body, Opts) ->
    ct:pal("Failed for request: ~n"
    "   ReqMethod: ~p~n"
    "   URL: ~p~n"
    "   Headers: ~p~n"
    "   ReqBody: ~p~n"
    "   Opts: ~p~n", [
        Method, URL, Headers, Body, Opts
    ]).
