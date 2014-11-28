%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module implements worker_plugin_behaviour to provide
%% gateway functionality (transfer of files between data centers).
%% @end
%% ===================================================================

-module(gateway).
-author("Konrad Zemek").
-behaviour(worker_plugin_behaviour).

-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/gateway/registered_names.hrl").
-include("oneprovider_modules/rtransfer/rt_container.hrl").
-include("registered_names.hrl").

-include("oneprovider_modules/dao/dao_spaces.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").

%% How many simultaneous operations can be performed per gateway_connection.
-define(connection_load_factor, 2).

-export([init/1, handle/2, cleanup/0]).
-export([notify/3, compute_request_hash/1]).
-export([start_queue_loop/1, queue_loop/3]).
-export([compute_request_hash/1, handle_node_lifecycle_notification/4]).


%% ====================================================================
%% API functions
%% ====================================================================


%% init/1
%% ====================================================================
%% @doc Initialize the module, starting all necessary services.
%% @see worker_plugin_behaviour
-spec init(Args :: term()) -> ok | {error, Error :: any()}.
%% ====================================================================
init(_Args) ->
    {ok, GwPort} = application:get_env(?APP_Name, gateway_listener_port),
    {ok, GwProxyPort} = application:get_env(?APP_Name, gateway_proxy_port),
    {ok, Cert} = application:get_env(?APP_Name, global_registry_provider_cert_path),
    {ok, Acceptors} = application:get_env(?APP_Name, gateway_acceptor_number),
    {ok, NICs} = application:get_env(?APP_Name, gateway_network_interfaces),

    LocalServerPort = oneproxy:get_local_port(GwPort),
    {ok, _} = ranch:start_listener(?GATEWAY_LISTENER, Acceptors, ranch_tcp,
        [{port, LocalServerPort}], gateway_protocol_handler, []),

    OpPid = spawn_link(fun() -> oneproxy:start_proxy(GwProxyPort, Cert, verify_none) end),
    register(gw_oneproxy_outgoing, OpPid),

    OpPid2 = spawn_link(fun() -> oneproxy:start_rproxy(GwPort, LocalServerPort, Cert, verify_none, no_http) end),
    register(gw_oneproxy_incoming, OpPid2),

    %% @todo On supervisor's exit we should be able to reinitialize the module.
	{ok, _} = gateway_dispatcher_supervisor:start_link(NICs),

    QueueLoopPid = spawn_link(?MODULE, start_queue_loop, [length(NICs) * ?connection_load_factor]),
    register(gw_queue_loop, QueueLoopPid),

    ok.


%% handle/2
%% ====================================================================
%% @doc Handle a message.
%% @see worker_plugin_behaviour
-spec handle(ProtocolVersion :: term(), Request :: term()) ->
    {ok, Ans :: term()} | {error, Error :: any()}.
%% ====================================================================
handle(_ProtocolVersion, ping) ->
    pong;

handle(_ProtocolVersion, healthcheck) ->
    ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

handle(_ProtocolVersion, #gw_fetch{} = Request) ->
    Block = repackage(Request),
    rt_utils:push(?GATEWAY_INCOMING_QUEUE, Block),
    ok;

handle(_ProtocolVersion, {node_lifecycle_notification, Node, Module, Action, Pid}) ->
  handle_node_lifecycle_notification(Node, Module, Action, Pid),
  ok;


handle(_ProtocolVersion, _Msg) ->
    ?log_call(_Msg),
    ok.


%% cleanup/0
%% ====================================================================
%% @doc Cleanup any state associated with the module.
%% @see worker_plugin_behaviour
-spec cleanup() -> ok | {error, Error :: any()}.
%% ====================================================================
cleanup() ->
    ranch:stop_listener(?GATEWAY_LISTENER),
    catch exit(whereis(gw_queue_loop), shutdown),
    catch exit(whereis(gw_oneproxy_outgoing), shutdown),
    catch exit(whereis(gw_oneproxy_incoming), shutdown),
    catch exit(whereis(?GATEWAY_DISPATCHER_SUPERVISOR), shutdown).


%% compute_request_hash/1
%% ====================================================================
%% @doc Computes a sha256 hash of an encoded protobuf #filerequest
-spec compute_request_hash(RequestBytes :: iodata()) -> Hash :: binary().
%% ====================================================================
compute_request_hash(RequestBytes) ->
    crypto:hash(sha256, RequestBytes).


%% notify/3
%% ====================================================================
%% @doc Notifies a process about something related to the action it required.
-spec notify(What :: atom(), Reason :: term(), Action :: #gw_fetch{}) -> ok.
notify(What, Details, #gw_fetch{notify = Notify} = Action) when is_atom(What) ->
    lists:foreach(fun(Pid) -> Pid ! {What, Details, Action} end, Notify),
    ok.


%% queue_loop/3
%% ====================================================================
%% @doc Takes elements from the incoming queue and passes them to the communicator.
%% Ensures that at most Max operations are currently running.
-spec queue_loop(Max :: pos_integer(), Running :: non_neg_integer(), SubRef :: reference()) -> no_return().
queue_loop(Max, Running, SubRef) when Running =:= Max ->
    RunningDelta = wait_for_messages(SubRef),
    ?MODULE:queue_loop(Max, Running + RunningDelta, SubRef);

queue_loop(Max, Running, SubRef) ->
    FetchResult = rt_utils:pop(?GATEWAY_INCOMING_QUEUE),
    RunningDelta = act_on_fetch_result(FetchResult, SubRef),
    ?MODULE:queue_loop(Max, Running + RunningDelta, SubRef).


%% ====================================================================
%% Internal functions
%% ====================================================================


%% handle_node_lifecycle_notification/4
%% ====================================================================
%% @doc Handles lifecycle calls
-spec handle_node_lifecycle_notification(Node :: list(), Module :: atom(), Action :: atom(), Pid :: pid()) -> ok.
%% ====================================================================
handle_node_lifecycle_notification(Node, Module, Action, Pid) ->
  ?debug("Lifecycle notification ~p",[{Node, Module, Action, Pid}]),
  ok.

%% start_queue_loop/1
%% ====================================================================
%% @doc Subscribes for notifications from the incoming queue and enters a queue_loop.
-spec start_queue_loop(Max :: pos_integer()) -> no_return().
start_queue_loop(Max) ->
    SubRef = make_ref(),
    rt_priority_queue:subscribe(?GATEWAY_INCOMING_QUEUE, self(), SubRef),
    queue_loop(Max, 0, SubRef).


%% act_on_fetch_result/2
%% ====================================================================
%% @doc Performs an operation on result of rt_priority_queue:pop. When the pop
%% was successful, passess the retrieved block to gateway dispatcher; otherwise
%% waits for new elements. Returns the change in number of currently running jobs.
%% @end
-spec act_on_fetch_result(Result :: {ok, #rt_block{}} | {error, term}, SubRef :: reference()) -> integer().
act_on_fetch_result({ok, Block}, _SubRef) ->
    #gw_fetch{notify = Notify} = Request = repackage(Block),
    MyRequest = Request#gw_fetch{notify = [self() | Notify]},
    gen_server:cast(?GATEWAY_DISPATCHER, MyRequest),
    1;

act_on_fetch_result({error, empty}, SubRef) ->
    wait_for_messages(SubRef).


%% wait_for_messages/1
%% ====================================================================
%% @doc Awaits notifications about new elements in the incoming queue, or
%% end notifications of scheduled fetch jobs. Returns the change in number of
%% current jobs.
%% @end
-spec wait_for_messages(SubRef :: reference()) -> integer().
wait_for_messages(SubRef) ->
    receive
        {not_empty, SubRef} -> 0;
        {_What, _Details, #gw_fetch{}} -> -1
    after
        timer:minutes(1) -> 0
    end.


%% repackage/1
%% ====================================================================
%% @doc Repackages #gw_fetch record into #rt_block and vice versa.
-spec repackage(#gw_fetch{}) -> #rt_block{}; (#rt_block{}) -> #gw_fetch{}.
repackage(#gw_fetch{file_id = FileId, offset = Offset, size = Size, remote = Remote, notify = Notify, retry = Retry}) ->
    #rt_block{file_id = FileId, offset = Offset, size = Size, provider_ref = Remote, terms = Notify, retry = Retry};

repackage(#rt_block{file_id = FileId, offset = Offset, size = Size, provider_ref = Remote, terms = Notify, retry = Retry}) ->
    #gw_fetch{file_id = FileId, offset = Offset, size = Size, remote = Remote, notify = Notify, retry = Retry}.
