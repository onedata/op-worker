%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements supervisor behaviour and is responsible
%%% for supervising and restarting sequencer manager and sequencer stream
%%% supervisors. It is initialized on session creation by session supervisor
%%% and in turn it initializes sequencer manager along with sequencer stream
%%% supervisors, both for incoming and outgoing messages.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_manager_sup).
-author("Krzysztof Trzepla").

% MWevents

-behaviour(supervisor).

%% API
-export([start_link/1, get_sequencer_stream_sup/2]).

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
-spec start_link(SessId :: session:id()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SessId) ->
    supervisor:start_link(?MODULE, [SessId]).

%%--------------------------------------------------------------------
%% @doc
%% Returns event stream supervisor associated with event manager.
%% @end
%%--------------------------------------------------------------------
-spec get_sequencer_stream_sup(SeqManSup :: pid(), Id :: atom()) ->
    {ok, SeqStmSup :: pid()} | {error, not_found}.
get_sequencer_stream_sup(SeqManSup, Id) ->
    Children = supervisor:which_children(SeqManSup),
    case lists:keyfind(Id, 1, Children) of
        {Id, SeqStmSup, _, _} when is_pid(SeqStmSup) -> {ok, SeqStmSup};
        _ -> {error, not_found}
    end.

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
    {ok, {SupFlags :: supervisor:sup_flags(), [ChildSpec :: supervisor:child_spec()]}}.
init([SessId]) ->
    {ok, {#{strategy => one_for_all, intensity => 3, period => 1}, [
        sequencer_stream_sup_spec(sequencer_in_stream_sup, sequencer_in_stream),
        sequencer_stream_sup_spec(sequencer_out_stream_sup, sequencer_out_stream),
        sequencer_manager_spec(self(), SessId)
    ]}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a supervisor child_spec for a sequencer stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec sequencer_stream_sup_spec(Id :: atom(), Child :: atom()) ->
    supervisor:child_spec().
sequencer_stream_sup_spec(Id, Child) ->
    #{
        id => Id,
        start => {sequencer_stream_sup, start_link, [Child]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [sequencer_stream_sup]
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a worker child_spec for a sequencer manager.
%% @end
%%--------------------------------------------------------------------
-spec sequencer_manager_spec(SeqManSup :: pid(), SessId :: session:id()) ->
    supervisor:child_spec().
sequencer_manager_spec(SeqManSup, SessId) ->
    #{
        id => sequencer_manager,
        start => {sequencer_manager, start_link, [SeqManSup, SessId]},
        restart => transient,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [sequencer_manager]
    }.