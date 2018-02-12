%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% rtransfer_server_sup is responsible for supervising rtransfer_server
%%% and gateway gen_servers.
%%% @end
%%%-------------------------------------------------------------------
-module(rtransfer_server_sup).
-author("Jakub Kudzia").

-behaviour(supervisor).

-include("modules/rtransfer/registered_names.hrl").


%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts rtransfer_server_sup supervisor
%% @end
%%--------------------------------------------------------------------
-spec(start_link([rtransfer:opt()]) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(RtransferOpts) ->
    supervisor:start_link({local, ?RTRANSFER_SUPERVISOR}, ?MODULE, RtransferOpts).

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
-spec(init([rtransfer:opt()]) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}} |
    ignore |
    {error, Reason :: term()}).
init(RtransferOpts) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 1000,
        period => 3600
    },
    {ok, {SupFlags, [
        rtransfer_server_spec(RtransferOpts),
        gateway_spec(RtransferOpts)
    ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates a supervisor child_spec for rtransfer_server child.
%% @end
%%-------------------------------------------------------------------
-spec rtransfer_server_spec([rtransfer:opt()]) -> supervisor:child_spec().
rtransfer_server_spec(RtransferOpts) ->
    #{
        id => ?RTRANSFER,
        start => {rtransfer_server, start, [RtransferOpts]},
        restart => permanent,
        shutdown => timer:seconds(10),
        type => worker
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates a supervisor child_spec for gateway child.
%% @end
%%-------------------------------------------------------------------
-spec gateway_spec([rtransfer:opt()]) -> supervisor:child_spec().
gateway_spec(RtransferOpts) ->
    #{
        id => ?GATEWAY,
        start => {gateway, start_link, [RtransferOpts]},
        restart => permanent,
        shutdown => timer:seconds(10),
        type => worker
    }.