%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements {@link worker_plugin_behaviour} and is responsible
%%% for creating and removing sequencer dispatchers for clients' sessions.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_manager_worker).
-author("Krzysztof Trzepla").

-behaviour(worker_plugin_behaviour).

-include("workers/datastore/datastore_models.hrl").
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
    {get_or_create_sequencer_dispatcher, SessId :: session:id()} |
    {remove_sequencer_dispatcher, SessId :: session:id()},
    Result :: nagios_handler:healthcheck_reponse() | ok | pong | {ok, Response} |
    {error, Reason},
    Response :: term(),
    Reason :: term().
handle(ping, _) ->
    pong;

handle(healthcheck, _) ->
    ok;

handle({get_or_create_sequencer_dispatcher, SessId}, _) ->
    get_or_create_sequencer_dispatcher(SessId);

handle({remove_sequencer_dispatcher, SessId}, _) ->
    remove_sequencer_dispatcher(SessId);

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
-spec get_or_create_sequencer_dispatcher(SessId :: session:id()) ->
    {ok, SeqDisp :: pid()} | {error, Reason :: term()}.
get_or_create_sequencer_dispatcher(SessId) ->
    case get_sequencer_dispatcher_data(SessId) of
        {ok, #sequencer_dispatcher_data{pid = SeqDisp}} ->
            {ok, SeqDisp};
        {error, {not_found, _}} ->
            create_sequencer_dispatcher(SessId);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns model of existing sequencer dispatcher for client session.
%% @end
%%--------------------------------------------------------------------
-spec get_sequencer_dispatcher_data(SessId :: session:id()) ->
    {ok, #sequencer_dispatcher_data{}} | {error, Reason :: term()}.
get_sequencer_dispatcher_data(SessId) ->
    case sequencer_dispatcher_data:get(SessId) of
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
-spec create_sequencer_dispatcher(SessId :: session:id()) ->
    {ok, Pid :: pid()} | {error, Reason :: term()}.
create_sequencer_dispatcher(SessId) ->
    {ok, SeqDispSup} = start_sequencer_dispatcher_sup(),
    {ok, SeqStmSup} = sequencer_dispatcher_sup:start_sequencer_stream_sup(SeqDispSup),
    case sequencer_dispatcher_sup:start_sequencer_dispatcher(SeqDispSup,
        SeqStmSup, SessId) of
        {ok, SeqDisp} ->
            {ok, SeqDisp};
        {error, {already_exists, _}} ->
            ok = stop_sequencer_dispatcher_sup(node(), SeqDispSup),
            get_or_create_sequencer_dispatcher(SessId);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes sequencer dispatcher for client session.
%% @end
%%--------------------------------------------------------------------
-spec remove_sequencer_dispatcher(SessId :: session:id()) ->
    ok | {error, Reason :: term()}.
remove_sequencer_dispatcher(SessId) ->
    case get_sequencer_dispatcher_data(SessId) of
        {ok, #sequencer_dispatcher_data{node = Node, sup = SeqDispSup}} ->
            stop_sequencer_dispatcher_sup(Node, SeqDispSup);
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
-spec stop_sequencer_dispatcher_sup(Node :: node(), SeqDispSup :: pid()) ->
    ok | {error, Reason :: term()}.
stop_sequencer_dispatcher_sup(Node, SeqDispSup) ->
    supervisor:terminate_child({?SEQUENCER_MANAGER_WORKER_SUP, Node}, SeqDispSup).
