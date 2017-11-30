%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% gateway_dispatcher_supervisor is responsible for supervising
%%% a gateway dispatcher and a connection_manager supervisor. The children
%%% are restarted using one_for_all strategy to ensure connection_managers
%%% are always properly registered with a gateway_dispatcher.
%%% @end
%%%-------------------------------------------------------------------
-module(gateway_dispatcher_supervisor).
-author("Konrad Zemek").
-behavior(supervisor).

-include("modules/rtransfer/registered_names.hrl").
-include("global_definitions.hrl").

-export([start_link/2]).
%% supervisor callbacks
-export([init/1]).

-define(default_block_size,
    application:get_env(?APP_NAME, rtransfer_block_size, 104857600)).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_link(NetworkInterfaces, RtransferOpts) -> Result when
    NetworkInterfaces :: [inet:ip_address()],
    RtransferOpts :: [rtransfer:opt()],
    Result :: {ok, pid()} | ignore | {error, Error},
    Error :: {already_started, pid()} | {shutdown, term()} | term().
start_link(NetworkInterfaces, RtransferOpts) ->
    supervisor:start_link({local, ?GATEWAY_DISPATCHER_SUPERVISOR}, ?MODULE,
        {NetworkInterfaces, RtransferOpts}).


%%--------------------------------------------------------------------
%% @doc
%% Initializes supervisor parameters. The NetworkInterfaces list is passed
%% to a created dispatcher.
%% @end
%%--------------------------------------------------------------------
-spec init({NetworkInterfaces, RtransferOpts}) -> Result when
    NetworkInterfaces :: [inet:ip_address()],
    RtransferOpts :: [rtransfer:opt()],
    Result :: {ok, {{RestartStrategy, MaxR, MaxT}, [ChildSpec]}} | ignore,
    RestartStrategy :: supervisor:strategy(),
    MaxR :: non_neg_integer(),
    MaxT :: pos_integer(),
    ChildSpec :: supervisor:child_spec().
init({NetworkInterfaces, RtransferOpts}) ->
    RestartStrategy = one_for_all,
    MaxR = 3,
    MaxT = timer:minutes(1),
    {ok, {{RestartStrategy, MaxR, MaxT},
        [connection_manager_supervisor_spec(RtransferOpts),
            dispatcher_spec(NetworkInterfaces),
            rt_priority_queue_spec(RtransferOpts),
            rt_map_spec()]}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates a supervisor child_spec for a dispatcher child.
%% @end
%%--------------------------------------------------------------------
-spec dispatcher_spec(NetworkInterfaces :: [inet:ip_address()]) ->
    supervisor:child_spec().
dispatcher_spec(NetworkInterfaces) ->
    ChildId = Module = gateway_dispatcher,
    Function = {Module, start_link, [NetworkInterfaces]},
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ChildId, Function, Restart, ExitTimeout, Type, [Module]}.


%%--------------------------------------------------------------------
%% @doc
%% Creates a supervisor child_spec for a connection manager supervisor child.
%% @end
%%--------------------------------------------------------------------
-spec connection_manager_supervisor_spec([rtransfer:opt()]) -> supervisor:child_spec().
connection_manager_supervisor_spec(RtransferOpts) ->
    ChildId = Module = gateway_connection_manager_supervisor,
    Function = {Module, start_link, [RtransferOpts]},
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = supervisor,
    {ChildId, Function, Restart, ExitTimeout, Type, [Module]}.


%%--------------------------------------------------------------------
%% @doc
%% Creates a supervisor child_spec for a rt_containter child.
%% @end
%%--------------------------------------------------------------------
-spec rt_priority_queue_spec([rtransfer:opt()]) -> supervisor:child_spec().
rt_priority_queue_spec(RtransferOpts) ->
    DefaultBlockSize = ?default_block_size,
    BlockSize = proplists:get_value(block_size,
        RtransferOpts, DefaultBlockSize),
    ChildId = Module = rt_priority_queue,
    Function = {Module, new, [{local, ?GATEWAY_INCOMING_QUEUE}, BlockSize]},
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ChildId, Function, Restart, ExitTimeout, Type, [Module]}.


%%--------------------------------------------------------------------
%% @doc
%% Creates a supervisor child_spec for a rt_containter child.
%% @end
%%--------------------------------------------------------------------
-spec rt_map_spec() -> supervisor:child_spec().
rt_map_spec() ->
    ChildId = Module = rt_map,
    Function = {Module, new, [{local, ?GATEWAY_NOTIFY_MAP}]},
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ChildId, Function, Restart, ExitTimeout, Type, [Module]}.
