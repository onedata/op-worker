%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements functions to contact the appmock server
%%% without the knowledge of underlying communication protocol. They are used
%%% for remote control of appmock instances during tests.
%%% @end
%%%-------------------------------------------------------------------
-module(appmock_client).
-author("Lukasz Opiola").

-include_lib("ctool/include/logging.hrl").
-include("appmock_internal.hrl").

%% API
-export([verify_mock/4, verify_all_mocks/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Performs a request to an appmock instance to verify if a given endpoint mock
%% has been requested certain amount of times. Returns:
%% ok - when verification succeded
%% {different, ActualNumber} - when verification failed; ActualNumber informs how many times has the endpoint been requested
%% {error, term()} - when there has been an error in verification procedure (this implies a bug in appmock).
%% @end
%%--------------------------------------------------------------------
-spec verify_mock(Hostname :: binary(), Port :: integer(), Path :: binary(), ExpectedCalls :: integer()) ->
    ok | {different, integer()} | {error, term()}.
verify_mock(Hostname, Port, Path, ExpectedCalls) ->
    try
        JSON = appmock_utils:encode_to_json(?VERIFY_MOCK_PACK_REQUEST(Port, Path, ExpectedCalls)),
        {ok, RemoteControlPort} = application:get_env(?APP_NAME, remote_control_port),
        {200, _, RespBodyJSON} = appmock_utils:https_request(Hostname, RemoteControlPort,
            <<?VERIFY_MOCK_PATH>>, post, [], JSON),
        RespBody = appmock_utils:decode_from_json(RespBodyJSON),
        case RespBody of
            ?OK_RESULT ->
                ok;
            _ ->
                case ?VERIFY_MOCK_UNPACK_ERROR(RespBody) of
                    {error, wrong_endpoint} -> {error, wrong_endpoint};
                    {error, Number} when is_integer(Number) -> {different, Number}
                end
        end
    catch T:M ->
        ?error("Error in verify_mock - ~p:~p", [T, M]),
        {error, M}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Performs a request to an appmock instance to verify if the mocked endpoints
%% had been requested in correct order. ExpectedOrder is a list of {Port, Path} pairs
%% that define the expected order of requests. Returns:
%% ok - when verification succeded
%% {different, ActualOrder} - when verification failed; ActualOrder is a list holding the requests in actual order.
%% {error, term()} - when there has been an error in verification procedure (this implies a bug in appmock).
%% @end
%%--------------------------------------------------------------------
-spec verify_all_mocks(Hostname :: binary(), ExpectedOrder :: PortPathMap) ->
    ok | {different, PortPathMap} | {error, term()} when PortPathMap :: [{Port :: integer(), Path :: binary()}].
verify_all_mocks(Hostname, ExpectedOrder) ->
    try
        JSON = appmock_utils:encode_to_json(?VERIFY_ALL_PACK_REQUEST(ExpectedOrder)),
        {ok, RemoteControlPort} = application:get_env(?APP_NAME, remote_control_port),
        {200, _, RespBodyJSON} = appmock_utils:https_request(Hostname, RemoteControlPort,
            <<?VERIFY_ALL_PATH>>, post, [], JSON),
        RespBody = appmock_utils:decode_from_json(RespBodyJSON),
        case RespBody of
            ?OK_RESULT ->
                ok;
            _ ->
                History = ?VERIFY_ALL_UNPACK_ERROR(RespBody),
                {different, History}
        end
    catch T:M ->
        ?error("Error in verify_all_mocks - ~p:~p", [T, M]),
        {error, M}
    end.
