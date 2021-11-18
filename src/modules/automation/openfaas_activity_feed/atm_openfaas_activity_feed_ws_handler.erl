%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements a WebSocket server for the OpenFaaS activity feed.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_activity_feed_ws_handler).
-author("Lukasz Opiola").

-behaviour(cowboy_websocket).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% Cowboy WebSocket handler callbacks
-export([
    init/2,
    websocket_init/1,
    websocket_handle/2,
    websocket_info/2,
    terminate/3
]).

-type state() :: no_state.

-define(AUTHORIZATION_SECRET_ENV_NAME, openfaas_container_status_feed_secret).

-define(MAX_LOGGED_REQUEST_SIZE, 500).

%%%===================================================================
%%% Cowboy WebSocket handler callbacks
%%%===================================================================

-spec init(Req :: cowboy_req:req(), Opts :: any()) ->
    {ok | cowboy_websocket, cowboy_req:req(), state()}.
init(Req, []) ->
    % @TODO VFS-8615 Support different secret per OpenFaaS instance when op-worker
    % supports multiple OpenFaaS instances
    IsAuthorized = case op_worker:get_env(?AUTHORIZATION_SECRET_ENV_NAME, undefined) of
        undefined ->
            ?alert("The ~p env variable is not set, the OpenFaaS activity feed will decline all requests", [
                ?AUTHORIZATION_SECRET_ENV_NAME
            ]),
            false;
        Secret ->
            try
                <<"Basic ", SecretB64/binary>> = cowboy_req:header(?HDR_AUTHORIZATION, Req, undefined),
                Secret == base64:decode(SecretB64)
            catch Class:Reason:Stacktrace ->
                ?debug_stacktrace("Authorization for OpenFaaS activity feed failed due to ~w:~p", [
                    Class, Reason
                ], Stacktrace),
                false
            end
    end,
    case IsAuthorized of
        false ->
            {ok, cowboy_req:reply(?HTTP_401_UNAUTHORIZED, Req), no_state};
        true ->
            {cowboy_websocket, Req, no_state}
    end.


-spec websocket_init(state()) -> {ok, state()}.
websocket_init(State) ->
    {ok, State}.


%%--------------------------------------------------------------------
%% @doc
%% Handles the data received from Websocket.
%% @end
%%--------------------------------------------------------------------
-spec websocket_handle(InFrame, State) ->
    {ok, State} | {ok, State, hibernate} |
    {reply, OutFrame | [OutFrame], State} |
    {reply, OutFrame | [OutFrame], State, hibernate} |
    {stop, State} when
    InFrame :: {text | binary | ping | pong, binary()},
    State :: state(),
    OutFrame :: cow_ws:frame().
websocket_handle({text, Data}, State) ->
    try
        ActivityReport = jsonable_record:from_json(json_utils:decode(Data), atm_openfaas_function_activity_report),
        atm_openfaas_function_activity_registry:consume_report(ActivityReport),
        {ok, State}
    catch Class:Reason:Stacktrace ->
        TrimmedPayload = case byte_size(Data) > ?MAX_LOGGED_REQUEST_SIZE of
            true ->
                binary:part(Data, 0, ?MAX_LOGGED_REQUEST_SIZE);
            false ->
                Data
        end,
        ?error_stacktrace(
            "Error while processing a request in ~p - ~w:~p~n"
            "Request payload: ~ts",
            [?MODULE, Class, Reason, TrimmedPayload],
            Stacktrace
        ),
        {reply, {text, <<"Bad request: ", Data/binary>>}, State}
    end;

websocket_handle(ping, State) ->
    {ok, State};

websocket_handle({ping, _Payload}, State) ->
    {ok, State};

websocket_handle(Msg, SessionData) ->
    ?warning("Unexpected frame in ~p: ~p", [?MODULE, Msg]),
    {ok, SessionData}.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when a message is sent to the process handling
%% the connection.
%% @end
%%--------------------------------------------------------------------
-spec websocket_info(Info, State) ->
    {ok, State} | {ok, State, hibernate} |
    {reply, OutFrame | [OutFrame], State} |
    {reply, OutFrame | [OutFrame], State, hibernate} |
    {stop, State} when
    Info :: any(),
    State :: state(),
    OutFrame :: cow_ws:frame().
websocket_info(terminate, SessionData) ->
    {stop, SessionData};

websocket_info(Msg, SessionData) ->
    ?warning("Unexpected message in ~p: ~p", [?MODULE, Msg]),
    {ok, SessionData}.


%%--------------------------------------------------------------------
%% @doc
%% Performs any necessary cleanup.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason, Req, State) -> ok when
    Reason :: normal | stop | timeout |
    remote | {remote, cow_ws:close_code(), binary()} |
    {error, badencoding | badframe | closed | atom()} |
    {crash, error | exit | throw, any()},
    Req :: cowboy_req:req(),
    State :: state().
terminate(_Reason, _Req, _State) ->
    ok.
