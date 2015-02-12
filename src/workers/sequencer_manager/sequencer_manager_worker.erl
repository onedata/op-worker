%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements {@link worker_plugin_behaviour} and is responsible
%%% for creating and removing sequencer dispatchers for client sessions.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_manager_worker).
-author("Krzysztof Trzepla").

-behaviour(worker_plugin_behaviour).

-include("workers/datastore/datastore_models.hrl").
-include("cluster_elements/protocol_handler/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/2, cleanup/0]).

%% API
-export([supervisor_spec/0, supervisor_child_spec/0]).

-define(SEQUENCER_MANAGER_WORKER_SUP, sequencer_manager_worker_sup).

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
    {get_or_create_sequencer_dispatcher, SessionId :: session_id(), Connection :: pid()} |
    {remove_sequencer_dispatcher, SessionId :: session_id()},
    Result :: nagios_handler:healthcheck_reponse() | ok | pong | {ok, Response} |
    {error, Reason},
    Response :: term(),
    Reason :: term().
handle(ping, _) ->
    pong;

handle(healthcheck, _) ->
    ok;

handle({get_or_create_sequencer_dispatcher, SessionId, Connection}, _) ->
    get_or_create_sequencer_dispatcher(SessionId, Connection);

handle({remove_sequencer_dispatcher, SessionId}, _) ->
    remove_sequencer_dispatcher(SessionId);

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
%% Returns a supervisor spec for a sequencer manager worker supervisor.
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
%% Returns a supervisor child_spec for a sequencer dispatcher supervisor.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_child_spec() -> [supervisor:child_spec()].
supervisor_child_spec() ->
    Id = Module = sequencer_dispatcher_sup,
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
%% Returns pid of sequencer dispatcher for client session. If sequencer
%% dispatcher does not exist it is instantiated.
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_sequencer_dispatcher(SessionId :: session_id(),
    Connection :: pid()) -> {ok, Pid :: pid()} | {error, Reason :: term()}.
get_or_create_sequencer_dispatcher(SessionId, Connection) ->
    case get_sequencer_dispatcher_model(SessionId) of
        {ok, #sequencer_dispatcher_model{pid = SeqDisp}} ->
            ok = gen_server:call(SeqDisp, {add_connection, Connection}),
            {ok, SeqDisp};
        {error, {not_found, _}} ->
            create_sequencer_dispatcher(SessionId, Connection);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns model of existing sequencer dispatcher for client session.
%% @end
%%--------------------------------------------------------------------
-spec get_sequencer_dispatcher_model(SessionId :: session_id()) ->
    {ok, #sequencer_dispatcher_model{}} | {error, Reason :: term()}.
get_sequencer_dispatcher_model(SessionId) ->
    case sequencer_dispatcher_model:get(SessionId) of
        {ok, #document{value = SeqModel}} ->
            {ok, SeqModel};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates sequencer dispatcher for client session.
%% @end
%%--------------------------------------------------------------------
-spec create_sequencer_dispatcher(SessionId :: session_id(), Connection :: pid()) ->
    {ok, Pid :: pid()} | {error, Reason :: term()}.
create_sequencer_dispatcher(SessionId, Connection) ->
    Node = node(),
    {ok, SeqDispSup} = start_sequencer_dispatcher_sup(),
    {ok, SeqStmSup} = sequencer_dispatcher_sup:start_sequencer_stream_sup(SeqDispSup),
    {ok, SeqDisp} = sequencer_dispatcher_sup:start_sequencer_dispatcher(SeqDispSup, SeqStmSup, Connection),
    case sequencer_dispatcher_model:create(#document{key = SessionId, value = #sequencer_dispatcher_model{
        node = Node, pid = SeqDisp, sup = SeqDispSup
    }}) of
        {ok, SessionId} ->
            {ok, SeqDisp};
        {error, already_exists} ->
            ok = stop_sequencer_dispatcher_sup(Node, SeqDispSup),
            get_or_create_sequencer_dispatcher(SessionId, Connection);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes sequencer dispatcher for client session.
%% @end
%%--------------------------------------------------------------------
-spec remove_sequencer_dispatcher(SessionId :: session_id()) ->
    ok | {error, Reason :: term()}.
remove_sequencer_dispatcher(SessionId) ->
    case get_sequencer_dispatcher_model(SessionId) of
        {ok, #sequencer_dispatcher_model{node = Node, sup = SeqDispSup}} ->
            ok = stop_sequencer_dispatcher_sup(Node, SeqDispSup),
            sequencer_dispatcher_model:delete(SessionId);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts sequencer dispatcher supervisor supervised by sequencer manager
%% worker supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_sequencer_dispatcher_sup() -> supervisor:startchild_ret().
start_sequencer_dispatcher_sup() ->
    supervisor:start_child(?SEQUENCER_MANAGER_WORKER_SUP, []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops sequencer dispatcher supervisor and its children.
%% @end
%%--------------------------------------------------------------------
-spec stop_sequencer_dispatcher_sup(Node :: node(), Pid :: pid()) ->
    ok | {error, Reason :: term()}.
stop_sequencer_dispatcher_sup(Node, Pid) ->
    supervisor:terminate_child({?SEQUENCER_MANAGER_WORKER_SUP, Node}, Pid).