%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements {@link worker_plugin_behaviour} and is responsible
%%% for creating and removing event dispatchers for client sessions.
%%% @end
%%%-------------------------------------------------------------------
-module(event_manager_worker).
-author("Krzysztof Trzepla").

-behaviour(worker_plugin_behaviour).

-include("workers/datastore/datastore_models.hrl").
-include("cluster_elements/protocol_handler/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/2, cleanup/0]).

%% API
-export([supervisor_spec/0, supervisor_child_spec/0]).

-define(EVENT_MANAGER_WORKER_SUP, event_manager_worker_sup).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State :: term()} | {error, Reason :: term()}.
init(_Args) ->
    {ok, []}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request, State :: term()) -> Result when
    Request :: ping | healthcheck |
    {get_or_create_event_dispatcher, SessionId :: session_id()} |
    {remove_event_dispatcher, SessionId :: session_id()},
    Result :: nagios_handler:healthcheck_reponse() | ok | pong | {ok, Response} |
    {error, Reason},
    Response :: term(),
    Reason :: term().
handle(ping, _) ->
    pong;

handle(healthcheck, _) ->
    ok;

handle({get_or_create_event_dispatcher, SessionId}, _) ->
    get_or_create_event_dispatcher(SessionId);

handle({remove_event_dispatcher, SessionId}, _) ->
    remove_event_dispatcher(SessionId);

handle(_Request, _) ->
    ?log_bad_request(_Request).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    ok.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a supervisor spec for a event manager worker supervisor.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_spec() ->
    {RestartStrategy :: supervisor:strategy(), MaxR :: integer(), MaxT :: integer()}.
supervisor_spec() ->
    RestartStrategy = simple_one_for_one,
    MaxR = 3,
    MaxT = timer:minutes(1),
    {RestartStrategy, MaxR, MaxT}.

%%--------------------------------------------------------------------
%% @doc
%% Returns a supervisor child_spec for a event dispatcher supervisor.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_child_spec() -> [supervisor:child_spec()].
supervisor_child_spec() ->
    Id = Module = event_dispatcher_sup,
    Restart = permanent,
    Shutdown = timer:seconds(10),
    Type = supervisor,
    [{Id, {Module, start_link, []}, Restart, Shutdown, Type, [Module]}].

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns pid of event dispatcher for client session. If event
%% dispatcher does not exist it is instantiated.
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_event_dispatcher(SessionId :: session_id()) ->
    {ok, Pid :: pid()} | {error, Reason :: term()}.
get_or_create_event_dispatcher(SessionId) ->
    case get_event_dispatcher_model(SessionId) of
        {ok, #event_dispatcher_model{pid = EvtDisp}} ->
            {ok, EvtDisp};
        {error, {not_found, _}} ->
            create_event_dispatcher(SessionId);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns model of existing event dispatcher for client session.
%% @end
%%--------------------------------------------------------------------
-spec get_event_dispatcher_model(SessionId :: session_id()) ->
    {ok, #event_dispatcher_model{}} | {error, Reason :: term()}.
get_event_dispatcher_model(SessionId) ->
    case event_dispatcher_model:get(SessionId) of
        {ok, #document{value = EvModel}} ->
            {ok, EvModel};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates event dispatcher for client session.
%% @end
%%--------------------------------------------------------------------
-spec create_event_dispatcher(SessionId :: session_id()) ->
    {ok, Pid :: pid()} | {error, Reason :: term()}.
create_event_dispatcher(SessionId) ->
    Node = node(),
    {ok, EvtDispSup} = start_event_dispatcher_sup(),
    {ok, EvtStmSup} = event_dispatcher_sup:start_event_stream_sup(EvtDispSup),
    {ok, EvtDisp} = event_dispatcher_sup:start_event_dispatcher(EvtDispSup, EvtStmSup),
    case event_dispatcher_model:create(#document{key = SessionId, value = #event_dispatcher_model{
        node = Node, pid = EvtDisp, sup = EvtDispSup
    }}) of
        {ok, SessionId} ->
            {ok, EvtDisp};
        {error, already_exists} ->
            ok = stop_event_dispatcher_sup(Node, EvtDispSup),
            get_or_create_event_dispatcher(SessionId);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes event dispatcher for client session.
%% @end
%%--------------------------------------------------------------------
-spec remove_event_dispatcher(SessionId :: session_id()) ->
    ok | {error, Reason :: term()}.
remove_event_dispatcher(SessionId) ->
    case get_event_dispatcher_model(SessionId) of
        {ok, #event_dispatcher_model{node = Node, sup = EvtDispSup}} ->
            ok = stop_event_dispatcher_sup(Node, EvtDispSup),
            event_dispatcher_model:delete(SessionId);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts event dispatcher supervisor supervised by event manager
%% worker supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_event_dispatcher_sup() -> supervisor:startchild_ret().
start_event_dispatcher_sup() ->
    supervisor:start_child(?EVENT_MANAGER_WORKER_SUP, []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops event dispatcher supervisor and its children.
%% @end
%%--------------------------------------------------------------------
-spec stop_event_dispatcher_sup(Node :: node(), Pid :: pid()) ->
    ok | {error, Reason :: term()}.
stop_event_dispatcher_sup(Node, Pid) ->
    supervisor:terminate_child({?EVENT_MANAGER_WORKER_SUP, Node}, Pid).