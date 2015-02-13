%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements supervisor behaviour and is responsible
%%% for supervising and restarting sequencer managers.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_dispatcher_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

%% API
-export([start_link/0, start_sequencer_stream_sup/1, start_sequencer_dispatcher/4]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    supervisor:start_link(?MODULE, []).

%%--------------------------------------------------------------------
%% @doc
%% Starts sequencer stream supervisor supervised by sequencer manager
%% supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_sequencer_stream_sup(SeqDispSup :: pid()) ->
    supervisor:startchild_ret().
start_sequencer_stream_sup(SeqDispSup) ->
    ChildSpec = sequencer_stream_sup_spec(),
    supervisor:start_child(SeqDispSup, ChildSpec).

%%--------------------------------------------------------------------
%% @doc
%% Starts sequencer dispatcher supervised by sequencer dispatcher
%% supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_sequencer_dispatcher(SeqDispSup :: pid(), SeqStmSup :: pid(),
    SessionId :: session:session_id(), Connection :: pid()) -> supervisor:startchild_ret().
start_sequencer_dispatcher(SeqDispSup, SeqStmSup, SessionId, Connection) ->
    ChildSpec = sequencer_dispatcher_spec(SeqDispSup, SeqStmSup, SessionId, Connection),
    supervisor:start_child(SeqDispSup, ChildSpec).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }} |
    ignore.
init([]) ->
    RestartStrategy = one_for_all,
    MaxR = 3,
    MaxT = timer:minutes(1),
    {ok, {{RestartStrategy, MaxR, MaxT}, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a supervisor child_spec for a sequencer stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec sequencer_stream_sup_spec() -> supervisor:child_spec().
sequencer_stream_sup_spec() ->
    Id = Module = sequencer_stream_sup,
    Restart = permanent,
    Shutdown = timer:seconds(10),
    Type = supervisor,
    {Id, {Module, start_link, []}, Restart, Shutdown, Type, [Module]}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a supervisor child_spec for a sequencer dispatcher.
%% @end
%%--------------------------------------------------------------------
-spec sequencer_dispatcher_spec(SeqDispSup :: pid(), SeqStmSup :: pid(),
    SessionId :: session:session_id(), Connection :: pid()) -> supervisor:child_spec().
sequencer_dispatcher_spec(SeqDispSup, SeqStmSup, SessionId, Connection) ->
    Id = Module = sequencer_dispatcher,
    Restart = permanent,
    Shutdown = timer:seconds(10),
    Type = worker,
    {Id, {Module, start_link, [SeqDispSup, SeqStmSup, SessionId, Connection]},
        Restart, Shutdown, Type, [Module]}.