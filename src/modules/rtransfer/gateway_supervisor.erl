%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% gateway_supervisor is responsible for supervising gateway gen_server,
%%% gateway_dispatcher_supervisor and rt_priority_queue.
%%% @end
%%%-------------------------------------------------------------------
-module(gateway_supervisor).
-author("Jakub Kudzia").

-behaviour(supervisor).

-include("modules/rtransfer/registered_names.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).


-define(default_block_size,
    application:get_env(?APP_NAME, rtransfer_block_size, 104857600)).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link([rtransfer:opt()]) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(RtransferOpts) ->
    supervisor:start_link({local, ?GATEWAY_SUPERVISOR}, ?MODULE, RtransferOpts).

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
%%
%% @end
%%--------------------------------------------------------------------
-spec init([rtransfer:opt()]) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}} | ignore.
init(RtransferOpts) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 1000,
        period => 3600
    },
    {ok, {SupFlags, [
        rt_priority_queue_spec(RtransferOpts),
        gateway_spec(RtransferOpts),
        gateway_dispatcher_supervisor_spec(RtransferOpts)
    ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns supervisor child_spec for gateway gen_server.
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

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns supervisor child_spec for gateway gen_server.
%% @end
%%-------------------------------------------------------------------
-spec gateway_dispatcher_supervisor_spec([rtransfer:opt()]) -> supervisor:child_spec().
gateway_dispatcher_supervisor_spec(RtransferOpts) ->
    NICs = proplists:get_value(bind, RtransferOpts, [{0, 0, 0, 0}]),
    #{
        id => ?GATEWAY_DISPATCHER_SUPERVISOR,
        start => {gateway_dispatcher_supervisor, start_link, [NICs, RtransferOpts]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns a supervisor child_spec for a rt_priority_queue child.
%% @end
%%--------------------------------------------------------------------
-spec rt_priority_queue_spec([rtransfer:opt()]) -> supervisor:child_spec().
rt_priority_queue_spec(RtransferOpts) ->
    DefaultBlockSize = ?default_block_size,
    BlockSize = proplists:get_value(block_size, RtransferOpts, DefaultBlockSize),
    ChildId = Module = rt_priority_queue,
    #{
        id => ChildId,
        start => {Module, new, [{local, ?GATEWAY_INCOMING_QUEUE}, BlockSize]},
        restart => permanent,
        shutdown => timer:seconds(10),
        type => worker
    }.
