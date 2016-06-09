%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements supervisor behaviour and is responsible
%%% for supervising and restarting rrdtool.
%%% @end
%%%-------------------------------------------------------------------
-module(rrdtool_supervisor).
-author("Michal Wrona").

-behaviour(supervisor).

%% API
-export([start_link/0, specification/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts the rrdtool supervisor.
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns a supervisor specification.
%% @end
%%--------------------------------------------------------------------
-spec specification() -> supervisor:child_spec().
specification() ->
    #{
        id => rrdtool_supervisor,
        start => {rrdtool_supervisor, start_rrdtool, []},
        restart => transient,
        shutdown => timer:seconds(10),
        type => supervisor,
        modules => [rrdtool_supervisor]
    }.

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
init([]) ->
    {ok, {#{strategy => one_for_one, intensity => 3, period => 1}, [
        #{
            id => rrdtool,
            start => {gen_server, start_link, [{local, rrdtool}, rrdtool,
                [os:find_executable("rrdtool")], []]},
            restart => permanent,
            shutdown => timer:seconds(10),
            type => worker,
            modules => [rrdtool]
    }]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
