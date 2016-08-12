%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(session_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

%% API
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SessId :: session:id(), SessType :: session:type()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SessId, SessType) ->
    supervisor:start_link(?MODULE, [SessId, SessType]).

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
init([SessId, SessType]) ->
    SupFlags = #{strategy => one_for_all, intensity => 0, period => 1},
    {ok, SessId} = session:update(SessId, #{supervisor => self(), node => node()}),

    SequencerEnabled = [fuse, provider_outgoing],

    case SessType of
        monitoring ->
            {ok, {SupFlags, [
                event_manager_sup_spec(SessId, SessType)
            ]}};
        _ ->
            case lists:member(SessType, SequencerEnabled) of
                true ->
                    {ok, {SupFlags, [
                        session_watcher_spec(SessId, SessType),
                        sequencer_manager_sup_spec(SessId),
                        event_manager_sup_spec(SessId, SessType)
                    ]}};
                _ ->
                    {ok, {SupFlags, [
                        session_watcher_spec(SessId, SessType),
                        event_manager_sup_spec(SessId, SessType)
                    ]}}
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a worker child_spec for a session watcher child.
%% @end
%%--------------------------------------------------------------------
-spec session_watcher_spec(SessId :: session:id(), SessType :: session:type()) ->
    supervisor:child_spec().
session_watcher_spec(SessId, SessType) ->
    #{
        id => session_watcher,
        start => {session_watcher, start_link, [SessId, SessType]},
        restart => transient,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [session_watcher]
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a supervisor child_spec for a sequencer manager child.
%% @end
%%--------------------------------------------------------------------
-spec sequencer_manager_sup_spec(SessId :: session:id()) ->
    supervisor:child_spec().
sequencer_manager_sup_spec(SessId) ->
    #{
        id => sequencer_manager_sup,
        start => {sequencer_manager_sup, start_link, [SessId]},
        restart => transient,
        shutdown => infinity,
        type => supervisor,
        modules => [sequencer_manager_sup]
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a supervisor child_spec for a event manager child.
%% @end
%%--------------------------------------------------------------------
-spec event_manager_sup_spec(SessId :: session:id(), SessType :: session:type()) ->
    supervisor:child_spec().
event_manager_sup_spec(SessId, SessType) ->
    #{
        id => event_manager_sup,
        start => {event_manager_sup, start_link, [SessId, SessType]},
        restart => transient,
        shutdown => infinity,
        type => supervisor,
        modules => [event_manager_sup]
    }.