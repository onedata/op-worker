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

-type connection_ref() :: pid().
% state specific for the report handler, opaque to this module
-type handler_state() :: term().
-record(state, {
    connection_ref :: connection_ref(),
    handler_state = undefined :: handler_state()
}).
-type state() :: #state{}.
-export_type([connection_ref/0, handler_state/0, state/0]).

-define(AUTHORIZATION_SECRET_ENV_NAME, openfaas_activity_feed_secret).

% NOTE: lager defaults to truncating messages at 4096 bytes
-define(MAX_LOGGED_REQUEST_SIZE, 1024).


%%%===================================================================
%%% Cowboy WebSocket handler callbacks
%%%===================================================================

-spec init(Req :: cowboy_req:req(), Opts :: any()) ->
    {ok | cowboy_websocket, cowboy_req:req(), undefined}.
init(Req, []) ->
    case is_authorized(Req) of
        true ->
            {cowboy_websocket, Req, undefined};
        false ->
            {ok, cowboy_req:reply(?HTTP_401_UNAUTHORIZED, Req), undefined}
    end.


-spec websocket_init(any()) -> {ok, state()}.
websocket_init(_) ->
    {ok, #state{connection_ref = self()}}.


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
websocket_handle({text, Payload}, State) ->
    handle_text_message(Payload, State);

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
websocket_info(terminate, State) ->
    {stop, State};

websocket_info(Msg, State) ->
    case atm_openfaas_activity_feed_ws_connection:interpret_info_message(Msg) of
        {reply, TextMessage} ->
            {reply, {text, TextMessage}, State};
        no_reply ->
            {ok, State}
    end.


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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec is_authorized(cowboy_req:req()) -> boolean().
is_authorized(Req) ->
    % @TODO VFS-8615 Support different secret per OpenFaaS instance when op-worker
    % supports multiple OpenFaaS instances
    case op_worker:get_env(?AUTHORIZATION_SECRET_ENV_NAME, undefined) of
        undefined ->
            ?alert("The ~p env variable is not set, the OpenFaaS activity feed will decline all requests", [
                ?AUTHORIZATION_SECRET_ENV_NAME
            ]),
            false;
        Secret ->
            try
                <<"Basic ", SecretB64/binary>> = cowboy_req:header(?HDR_AUTHORIZATION, Req, undefined),
                str_utils:to_binary(Secret) == base64:decode(SecretB64)
            catch Class:Reason:Stacktrace ->
                ?debug_stacktrace("Authorization for OpenFaaS activity feed failed due to ~w:~p", [
                    Class, Reason
                ], Stacktrace),
                false
            end
    end.


%% @private
-spec handle_text_message(binary(), state()) ->
    {ok, state()} | {reply, {text, binary()}, state()}.
handle_text_message(Payload, #state{handler_state = HandlerState} = State) ->
    try
        ActivityReport = jsonable_record:from_json(json_utils:decode(Payload), atm_openfaas_activity_report),
        handle_activity_report(ActivityReport, State)
    catch Class:Reason:Stacktrace ->
        TrimmedPayload = case byte_size(Payload) > ?MAX_LOGGED_REQUEST_SIZE of
            true ->
                binary:part(Payload, 0, ?MAX_LOGGED_REQUEST_SIZE);
            false ->
                Payload
        end,
        ?error_stacktrace(
            "Error when parsing an openfaas activity report - ~w:~p~n"
            "Request payload: ~ts",
            [Class, Reason, TrimmedPayload],
            Stacktrace
        ),
        atm_openfaas_activity_report:handle_reporting_error(HandlerState, ?ERROR_BAD_MESSAGE(TrimmedPayload)),
        {reply, {text, <<"Bad request: ", Payload/binary>>}, State}
    end.


%% @private
-spec handle_activity_report(atm_openfaas_activity_report:record(), state()) ->
    {ok, state()} | {reply, {text, binary()}, state()}.
handle_activity_report(ActivityReport, #state{connection_ref = ConnRef, handler_state = HandlerState} = State) ->
    try
        NewHandlerState = atm_openfaas_activity_report:consume(ConnRef, HandlerState, ActivityReport),
        {ok, State#state{handler_state = NewHandlerState}}
    catch Class:Reason:Stacktrace ->
        ?error_stacktrace(
            "Unexpected error when processing an openfaas activity report - ~w:~p~n"
            "Activity report: ~tp",
            [Class, Reason, ActivityReport],
            Stacktrace
        ),
        atm_openfaas_activity_report:handle_reporting_error(HandlerState, ?ERROR_INTERNAL_SERVER_ERROR),
        {reply, {text, <<"Internal server error while processing the request">>}, State}
    end.
