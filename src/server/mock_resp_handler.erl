%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is a cowboy handler and processes requests to mocked endpoints.
%%% @end
%%%-------------------------------------------------------------------
-module(mock_resp_handler).

-include_lib("ctool/include/logging.hrl").
-include("appmock.hrl").

%% Cowboy API
-export([init/3, handle/2, terminate/3]).


%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback, called to initialize the state of the handler.
%% @end
%%--------------------------------------------------------------------
-spec init(Type :: term(), Req :: term(), [ETSKey]) -> {ok, term(), [ETSKey]} when ETSKey :: {Port :: integer(), Path :: binary()}.
init(_Type, Req, [ETSKey]) ->
    {ok, Req, [ETSKey]}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback, called to process a request.
%% Handles requests to mocked endpoints by delegating them to appmock_logic.
%% @end
%%--------------------------------------------------------------------
-spec handle(Req :: term(), [ETSKey]) -> {ok, term(), [ETSKey]} when ETSKey :: {Port :: integer(), Path :: binary()}.
handle(Req, [ETSKey]) ->
    {ok, Req2} =
        try
            {ok, _NewReq} = appmock_logic:produce_mock_resp(Req, ETSKey)
        catch T:M ->
            {Port, Path} = ETSKey,
            ?error_stacktrace("Error in mock_resp_handler. Path: ~p. Port: ~p. ~p:~p.",
                [Path, Port, T, M]),
            {ok, _ErrorReq} = cowboy_req:reply(500, Req)
        end,
    {ok, Req2, [ETSKey]}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback, called to perform cleanup after the request is handled.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), Req :: term(), State :: term()) -> ok.
terminate(_Reason, _Req, _State) ->
    ok.