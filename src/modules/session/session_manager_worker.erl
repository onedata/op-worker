%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements {@link worker_plugin_behaviour} and is responsible
%%% for creation and removal of clients' sessions.
%%% @end
%%%-------------------------------------------------------------------
-module(session_manager_worker).
-author("Krzysztof Trzepla").

-behaviour(worker_plugin_behaviour).

-include("cluster/worker/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([supervisor_spec/0, supervisor_child_spec/0]).

-define(SESSION_WORKER_SUP, session_manager_worker_sup).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State :: worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck |
    {get_or_create_session, SessId :: session:id(), Iden :: session:identity(),
        Con :: pid()} |
    {remove_session, SessId :: session:id()},
    Result :: nagios_handler:healthcheck_response() | ok | pong | {ok, Response} |
    {error, Reason},
    Response :: term(),
    Reason :: term().
handle(ping) ->
    pong;

handle(healthcheck) ->
    ok;

handle({reuse_or_create_session, SessId, Iden, Con}) ->
    reuse_or_create_session(SessId, Iden, Con);

handle({update_session_auth, SessId, Auth}) ->
    update_session_auth(SessId, Auth);

handle({remove_session, SessId}) ->
    remove_session(SessId);

handle(_Request) ->
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
    MaxRestarts = 0,
    RestartTimeWindowSecs = 1,
    {RestartStrategy, MaxRestarts, RestartTimeWindowSecs}.

%%--------------------------------------------------------------------
%% @doc
%% Returns a supervisor child_spec for a event dispatcher supervisor.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_child_spec() -> [supervisor:child_spec()].
supervisor_child_spec() ->
    Id = Module = session_sup,
    Restart = temporary,
    Shutdown = infinity,
    Type = supervisor,
    [{Id, {Module, start_link, []}, Restart, Shutdown, Type, [Module]}].

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to reuse active session. If session does not exist it is created.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_session(SessId :: session:id(),
    Iden :: session:identity(), Con :: pid()) ->
    {ok, reused | created} | {error, Reason :: term()}.
reuse_or_create_session(SessId, Iden, Con) ->
    case reuse_session(SessId, Iden, Con) of
        {ok, reused} -> {ok, reused};
        {error, {not_found, _}} -> create_session(SessId, Iden, Con);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Reuses active session or returns with an error.
%% @end
%%--------------------------------------------------------------------
-spec reuse_session(SessId :: session:id(), Iden :: session:identity(),
    Con :: pid()) -> {ok, reused} |{error, Reason :: invalid_identity | term()}.
reuse_session(SessId, Iden, Con) ->
    case session:get(SessId) of
        {ok, #document{value = #session{identity = Iden, communicator = undefined}}} ->
            reuse_session(SessId, Iden, Con);
        {ok, #document{value = #session{identity = Iden, communicator = Comm}}} ->
            ok = communicator:add_connection(Comm, Con),
            {ok, reused};
        {ok, #document{}} ->
            {error, invalid_identity};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new session or if session exists tries to reuse it.
%% @end
%%--------------------------------------------------------------------
-spec create_session(SessId :: session:id(), Iden :: session:identity(),
    Con :: pid()) -> {ok, created} | {error, Reason :: term()}.
create_session(SessId, Iden, Con) ->
    case session:create(#document{key = SessId, value = #session{identity = Iden}}) of
        {ok, SessId} ->
            {ok, _} = start_session_sup(SessId, Con),
            {ok, created};
        {error, already_exists} ->
            reuse_or_create_session(SessId, Iden, Con);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates the #auth{} record in given session.
%% @end
%%--------------------------------------------------------------------
update_session_auth(SessId, #auth{} = Auth) ->
    {ok, Doc = #document{
        value = #session{} = Session}} = session:get(SessId),
    {ok, _} = session:save(Doc#document{
        value = Session#session{auth = Auth}}),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes session from cache and stops session supervisor.
%% @end
%%--------------------------------------------------------------------
-spec remove_session(SessId :: session:id()) -> ok | {error, Reason :: term()}.
remove_session(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{session_sup = undefined}}} ->
            session:delete(SessId);
        {ok, #document{value = #session{session_sup = SessSup, node = Node}}} ->
            ok = session:delete(SessId),
            stop_session_sup(Node, SessSup);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts session supervisor supervised by event manager
%% worker supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_session_sup(SessId :: session:id(), Con :: pid()) ->
    supervisor:startchild_ret().
start_session_sup(SessId, Con) ->
    supervisor:start_child(?SESSION_WORKER_SUP, [SessId, Con]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops event dispatcher supervisor and its children.
%% @end
%%--------------------------------------------------------------------
-spec stop_session_sup(Node :: node(), SessSup :: pid()) ->
    ok | {error, Reason :: term()}.
stop_session_sup(Node, SessSup) ->
    supervisor:terminate_child({?SESSION_WORKER_SUP, Node}, SessSup).