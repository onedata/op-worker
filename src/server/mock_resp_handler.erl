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
    {ok, NewReq} =
        try
            {ok, _NewReq} = appmock_logic:produce_mock_resp(Req, ETSKey)
        catch T:M ->
            {Port, Path} = ETSKey,
            Stacktrace = erlang:get_stacktrace(),
            ?error("Error in mock_resp_handler. Path: ~p. Port: ~p. ~p:~p.~nStacktrace: ~p",
                [Path, Port, T, M, Stacktrace]),
            Error = gui_str:format_bin("500 Internal server error - make sure that your description file does not " ++
            "contain errors.~nType:       ~p~nMessage:    ~p~nStacktrace: ~p", [T, M, Stacktrace]),
            Req2 = cowboy_req:set_resp_body(Error, Req),
            Req3 = gui_utils:cowboy_ensure_header(<<"content-type">>, <<"text/plain">>, Req2),
            {ok, _ErrorReq} = cowboy_req:reply(500, Req3)
        end,
    {ok, NewReq, [ETSKey]}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback, called to perform cleanup after the request is handled.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), Req :: term(), State :: term()) -> ok.
terminate(_Reason, _Req, _State) ->
    ok.