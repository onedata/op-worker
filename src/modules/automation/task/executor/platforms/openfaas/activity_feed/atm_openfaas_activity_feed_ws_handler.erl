%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements a WebSocket server for the OpenFaaS activity feed.
%%% Currently, two types of clients can connect to the server:
%%%   * pod_status_monitor - sends reports with OpenFaaS pod status changes
%%%   * result_streamer - sends reports with lambda results relayed via a file pipe
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

-type client_type() :: pod_status_monitor | result_streamer.
% the pid of the connection process
-type connection_ref() :: pid().
% state specific for the report handler, opaque to this module
-type handler_state() :: term().
-record(state, {
    handler_module :: module(),
    handler_state :: handler_state()
}).
-type state() :: #state{}.
-export_type([client_type/0, connection_ref/0, handler_state/0, state/0]).

-define(AUTHORIZATION_SECRET_ENV_NAME, openfaas_activity_feed_secret).

% NOTE: lager defaults to truncating messages at 4096 bytes
-define(MAX_LOGGED_REQUEST_SIZE, 1024).


%%%===================================================================
%%% Cowboy WebSocket handler callbacks
%%%===================================================================

-spec init(Req :: cowboy_req:req(), Opts :: any()) ->
    {ok | cowboy_websocket, cowboy_req:req(), undefined | client_type()}.
init(Req, []) ->
    case identify_client_type(Req) of
        {error, ErrorCode} ->
            {ok, cowboy_req:reply(ErrorCode, Req), undefined};
        {ok, ClientType} ->
            case is_authorized(Req) of
                false ->
                    {ok, cowboy_req:reply(?HTTP_401_UNAUTHORIZED, Req), undefined};
                true ->
                    {cowboy_websocket, Req, ClientType}
            end
    end.


-spec websocket_init(client_type()) -> {ok, state()}.
websocket_init(ClientType) ->
    {ok, #state{
        handler_module = case ClientType of
            pod_status_monitor ->
                atm_openfaas_function_pod_status_registry;
            result_streamer ->
                atm_openfaas_result_stream_handler
        end,
        handler_state = undefined
    }}.


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
    ?warning("Unexpected frame in ~tp: ~tp", [?MODULE, Msg]),
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
-spec identify_client_type(cowboy_req:req()) -> {ok, client_type()} | {error, integer()}.
identify_client_type(Req) ->
    case cowboy_req:binding(client_type, Req, undefined) of
        <<"pod_status_monitor">> ->
            {ok, pod_status_monitor};
        <<"result_streamer">> ->
            {ok, result_streamer};
        _ ->
            {error, ?HTTP_404_NOT_FOUND}
    end.


%% @private
-spec is_authorized(cowboy_req:req()) -> boolean().
is_authorized(Req) ->
    % @TODO VFS-8615 Support different secret per OpenFaaS instance when op-worker
    % supports multiple OpenFaaS instances
    case op_worker:get_env(?AUTHORIZATION_SECRET_ENV_NAME, undefined) of
        undefined ->
            utils:throttle(3600, fun() ->
                ?alert("The ~tp env variable is not set, the OpenFaaS activity feed will decline all requests", [
                    ?AUTHORIZATION_SECRET_ENV_NAME
                ])
            end),
            false;
        Secret ->
            try
                <<"Basic ", SecretB64/binary>> = cowboy_req:header(?HDR_AUTHORIZATION, Req, undefined),
                str_utils:to_binary(Secret) == base64:decode(SecretB64)
            catch Class:Reason:Stacktrace ->
                ?debug_exception("Authorization for OpenFaaS activity feed failed", Class, Reason, Stacktrace),
                false
            end
    end.


%% @private
-spec handle_text_message(binary(), state()) ->
    {ok, state()} | {reply, [cow_ws:frame()], state()}.
handle_text_message(Payload, #state{handler_module = HandlerModule, handler_state = HandlerState} = State) ->
    try
        ActivityReport = jsonable_record:from_json(json_utils:decode(Payload), atm_openfaas_activity_report),
        handle_activity_report(ActivityReport, State)
    catch Class:Reason:Stacktrace ->
        PayloadSample = str_utils:truncate_overflow(Payload, ?MAX_LOGGED_REQUEST_SIZE),
        ?error_exception(?autoformat(PayloadSample), Class, Reason, Stacktrace),
        HandlerModule:handle_error(self(), ?ERROR_BAD_MESSAGE(PayloadSample), HandlerState),
        {reply, [{text, <<"Bad request: ", Payload/binary>>}], State}
    end.


%% @private
-spec handle_activity_report(atm_openfaas_activity_report:record(), state()) ->
    {ok, state()} | {reply, [cow_ws:frame()], state()}.
handle_activity_report(ActivityReport, #state{handler_module = HandlerModule, handler_state = HandlerState} = State) ->
    try
        #atm_openfaas_activity_report{batch = Batch} = ActivityReport,
        {Results, FinalHandlerState} = lists:mapfoldl(fun(ReportBody, HandlerStateAcc) ->
            HandlerModule:consume_report(self(), ReportBody, HandlerStateAcc)
        end, HandlerState, Batch),
        ReplyFrames = lists:filtermap(fun
            (no_reply) -> false;
            ({reply_json, JsonTerm}) -> {true, {text, json_utils:encode(JsonTerm)}}
        end, Results),
        case ReplyFrames of
            [] ->
                {ok, State#state{handler_state = FinalHandlerState}};
            _ ->
                {reply, ReplyFrames, State#state{handler_state = FinalHandlerState}}
        end
    catch Class:Reason:Stacktrace ->
        ?error_exception(?autoformat(HandlerModule, ActivityReport, HandlerState), Class, Reason, Stacktrace),
        HandlerModule:handle_error(self(), ?ERROR_INTERNAL_SERVER_ERROR, HandlerState),
        {reply, [{text, <<"Internal server error while processing the request">>}], State}
    end.
