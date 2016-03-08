%%%--------------------------------------------------------------------
%%% @author Michal Żmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% @end
%%%--------------------------------------------------------------------
-module(subscription_wss).
-author("Michal Zmuda").

-behaviour(websocket_client_handler_behaviour).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

-export([init/2, websocket_handle/3, websocket_info/3, websocket_terminate/3]).
-export([start_link/0]).

%%--------------------------------------------------------------------
%% @doc
%% Start the connection with OZ.
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    Port = integer_to_list(application:get_env(?APP_NAME, oz_wss_port, 9443)),
    Address = "wss://" ++ oneprovider:get_oz_domain() ++ ":" ++ Port ++ "/subscriptions",

    CACertFile = oz_plugin:get_cacert_path(),
    KeyFile = oz_plugin:get_key_path(),
    CertFile = oz_plugin:get_cert_path(),
    Options = [{keyfile, KeyFile}, {certfile, CertFile}, {cacertfile, CACertFile}],

    {ok, Pid} = websocket_client:start_link(Address, ?MODULE, [], Options),
    Pid ! register,
    {ok, Pid}.

%%--------------------------------------------------------------------
%% @doc
%% Callback called when connection is received.
%% @end
%%--------------------------------------------------------------------
-spec init([term()], websocket_req:req()) ->
    {ok, State :: term()} | {ok, State :: term(), Keepalive :: integer()}.
init([], _ConnState) ->
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% Callback called when data is received via WebSocket protocol.
%% @end
%%--------------------------------------------------------------------
-spec websocket_handle({text | binary | ping | pong, binary()},
    websocket_req:req(), State :: term()) ->
    {ok, State :: term()} |
    {reply, websocket_req:frame(), State :: term()} |
    {close, Reply :: binary(), State :: term()}.
websocket_handle({binary, RawUpdates}, _ConnState, _State) ->
    Updates = subscription_translator:json_to_updates(RawUpdates),
    worker_proxy:call(subscriptions_worker, {process_updates, Updates}),
    {ok, _State};
websocket_handle(_Msg, _ConnState, _State) ->
    ?log_bad_request(_Msg),
    {ok, _State}.

%%--------------------------------------------------------------------
%% @doc
%% Callback called when a message is sent to the process handling
%% the connection.
%% @end
%%--------------------------------------------------------------------
-spec websocket_info(term(), websocket_req:req(), State :: term()) ->
    {ok, State :: term()} |
    {reply, websocket_req:frame(), State :: term()} |
    {close, Reply :: binary(), State :: term()}.
websocket_info(register, _ConnState, _State) ->
    try
        true = register(?MODULE, self()),
        {ok, _State}
    catch
        E:R ->
            ?error_stacktrace("Unable to register ~p:~p", [E, R]),
            {close, <<"closed">>, _State}
    end;
websocket_info({push, Binary}, _ConnState, _State) ->
    {reply, {binary, Binary}, _State};

websocket_info(_Msg, _ConnState, _State) ->
    ?log_bad_request(_Msg),
    {ok, _State}.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when the connection is closed.
%% @end
%%--------------------------------------------------------------------
-spec websocket_terminate({Reason, term()} | {Reason, integer(), binary()},
    websocket_req:req(), State :: term()) -> ok when
    Reason :: normal | error | remote.

websocket_terminate(_Reason, _ConnState, _State) ->
    ok.