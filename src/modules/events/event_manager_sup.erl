%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements supervisor behaviour and is responsible
%%% for supervising and restarting event manager. It is initialized on session
%%% creation by session supervisor and in turn it initializes event manager
%%% along with event stream supervisor.
%%% @end
%%%-------------------------------------------------------------------
-module(event_manager_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

%% API
-export([start_link/2, get_event_stream_sup/1]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the event manager supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SessId :: session:id(), SessType :: session:type()) ->
    {ok, EvtManPid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SessId, SessType) ->
    supervisor:start_link(?MODULE, [SessId, SessType]).

%%--------------------------------------------------------------------
%% @doc
%% Returns event stream supervisor associated with event manager.
%% @end
%%--------------------------------------------------------------------
-spec get_event_stream_sup(ManSupPid :: pid()) ->
    {ok, StmSup :: pid()} | {error, not_found}.
get_event_stream_sup(EvtManSup) ->
    Id = event_stream_sup,
    Children = supervisor:which_children(EvtManSup),
    case lists:keyfind(Id, 1, Children) of
        {Id, StmSup, _, _} when is_pid(StmSup) -> {ok, StmSup};
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
init([SessId, SessType]) ->
    {ok, {#{strategy => one_for_all, intensity => 3, period => 1}, [
        event_stream_sup_spec(SessType),
        event_manager_spec(self(), SessId)
    ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a supervisor child_spec for an event stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec event_stream_sup_spec(SessType :: session:type()) -> supervisor:child_spec().
event_stream_sup_spec(SessType) ->
    #{
        id => event_stream_sup,
        start => {event_stream_sup, start_link, [SessType]},
        restart => permanent,
        shutdown => infinity,
        type =>supervisor,
        modules => [event_stream_sup]
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a worker child_spec for an event manager.
%% @end
%%--------------------------------------------------------------------
-spec event_manager_spec(EvtManSup :: pid(), SessId :: session:id()) ->
    supervisor:child_spec().
event_manager_spec(EvtManSup, SessId) ->
    #{
        id => event_manager,
        start => {event_manager, start_link, [EvtManSup, SessId]},
        restart => transient,
        shutdown => timer:seconds(10),
        type =>worker,
        modules => [event_manager]
    }.