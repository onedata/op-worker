%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements {@link worker_plugin_behaviour} and is responsible
%%% for creating and removing sequencer managers for FUSE clients.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_worker).
-author("Krzysztof Trzepla").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("workers/datastore/datastore_models.hrl").
-include("cluster_elements/protocol_handler/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/2, cleanup/0]).

%% API
-export([start_sequencer_manager_sup/0, stop_sequencer_manager_sup/2]).
-export([supervisor_spec/0, supervisor_children_spec/0]).

-define(SEQUENCER_WORKER, sequencer_worker).
-define(SEQUENCER_WORKER_SUP, sequencer_worker_sup).

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
    {get_or_create_sequencer_manager, FuseId :: fuse_id(), Connection :: pid()} |
    {remove_sequencer_manager, FuseId :: fuse_id()},
    Result :: healthcheck_reponse() | ok | pong | {ok, Response} | {error, Reason},
    Response :: term(),
    Reason :: term().
handle(ping, _) ->
    pong;

handle(healthcheck, _) ->
    ok;

handle({get_or_create_sequencer_manager, FuseId, Connection}, _) ->
    get_or_create_sequencer_manager(FuseId, Connection);

handle({remove_sequencer_manager, FuseId}, _) ->
    remove_sequencer_manager(FuseId);

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
%% Starts sequencer manager supervisor supervised by sequencer dispatcher
%% supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_sequencer_manager_sup() -> supervisor:startchild_ret().
start_sequencer_manager_sup() ->
    supervisor:start_child(?SEQUENCER_WORKER_SUP, []).

%%--------------------------------------------------------------------
%% @doc
%% Stops sequencer manager supervisor and its children.
%% @end
%%--------------------------------------------------------------------
-spec stop_sequencer_manager_sup(Node :: node(), Pid :: pid()) ->
    ok | {error, Reason :: term()}.
stop_sequencer_manager_sup(Node, Pid) ->
    supervisor:terminate_child({?SEQUENCER_WORKER_SUP, Node}, Pid).

%%--------------------------------------------------------------------
%% @doc
%% Creates spec for sequencer worker supervisor.
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
%% Creates spec for a sequencer worker supervisor child.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_children_spec() -> [supervisor:child_spec()].
supervisor_children_spec() ->
    Id = Module = sequencer_manager_sup,
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
%% Returns pid of sequencer manager for FUSE client. If sequencer manager
%% does not exist it is instantiated.
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_sequencer_manager(FuseId :: fuse_id(), Connection :: pid()) ->
    {ok, Pid :: pid()} | {error, Reason :: term()}.
get_or_create_sequencer_manager(FuseId, Connection) ->
    case get_sequencer_manager(FuseId) of
        {ok, #sequencer_manager_model{pid = SeqMan}} ->
            ok = gen_server:call(SeqMan, {add_connection, Connection}),
            {ok, SeqMan};
        {error, {not_found, _}} ->
            create_sequencer_manager(FuseId, Connection);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns pid of existing sequencer manager for FUSE client.
%% @end
%%--------------------------------------------------------------------
-spec get_sequencer_manager(FuseId :: fuse_id()) ->
    {ok, #sequencer_manager_model{}} | {error, Reason :: term()}.
get_sequencer_manager(FuseId) ->
    case sequencer_manager_model:get(FuseId) of
        {ok, #document{value = SeqModel}} ->
            {ok, SeqModel};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates sequencer manager for FUSE client.
%% @end
%%--------------------------------------------------------------------
-spec create_sequencer_manager(FuseId :: fuse_id(), Connection :: pid()) ->
    {ok, Pid :: pid()} | {error, Reason :: term()}.
create_sequencer_manager(FuseId, Connection) ->
    Node = node(),
    {ok, SeqManSup} = sequencer_worker:start_sequencer_manager_sup(),
    {ok, SeqSup} = sequencer_manager_sup:start_sequencer_sup(SeqManSup),
    {ok, SeqMan} = sequencer_manager_sup:start_sequencer_manager(SeqManSup, SeqSup, Connection),
    case sequencer_manager_model:create(#document{key = FuseId, value = #sequencer_manager_model{
        node = Node, pid = SeqMan, sup = SeqManSup
    }}) of
        {ok, FuseId} ->
            {ok, SeqMan};
        {error, already_exists} ->
            ok = sequencer_worker:stop_sequencer_manager_sup(Node, SeqManSup),
            get_or_create_sequencer_manager(FuseId, Connection);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes sequencer manager for FUSE client.
%% @end
%%--------------------------------------------------------------------
-spec remove_sequencer_manager(FuseId :: fuse_id()) ->
    ok | {error, Reason :: term()}.
remove_sequencer_manager(FuseId) ->
    case get_sequencer_manager(FuseId) of
        {ok, #sequencer_manager_model{node = Node, sup = SeqManSup}} ->
            ok = sequencer_worker:stop_sequencer_manager_sup(Node, SeqManSup),
            sequencer_manager_model:delete(FuseId);
        {error, Reason} ->
            {error, Reason}
    end.