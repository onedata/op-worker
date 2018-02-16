%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements worker_plugin_behaviour to provide
%%% gateway functionality (transfer of files between data centers).
%%% @end
%%%-------------------------------------------------------------------
-module(gateway).
-author("Konrad Zemek").
-behavior(gen_server).

-include("global_definitions.hrl").
-include("modules/rtransfer/gateway.hrl").
-include("modules/rtransfer/registered_names.hrl").
-include("modules/rtransfer/rt_container.hrl").
-include_lib("ctool/include/logging.hrl").

%% How many simultaneous operations can be performed per gateway_connection.
-define(connection_load_factor,
    application:get_env(?APP_NAME, gateway_connection_load_factor, 5)).

%% API
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3, start_link/1]).

-export([notify/3, compute_request_hash/1, start_queue_loop/1, queue_loop/3]).


%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Starts gateway gen_server.
%% @end
%%-------------------------------------------------------------------
-spec start_link(rtransfer:config()) -> {ok, pid()}.
start_link(RtransferOpts) ->
    gen_server2:start_link({local, ?GATEWAY}, ?MODULE, RtransferOpts, []).

%%--------------------------------------------------------------------
%% @doc
%% Initialize the module, starting all necessary services.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: [rtransfer:opt()]) ->
    {ok, State :: [rtransfer:opt()]} |
    {ok, State :: [rtransfer:opt()], timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init(RtransferOpts) ->
    process_flag(trap_exit, true),
    RanchOpts = proplists:get_value(ranch_opts, RtransferOpts, []),
    NbAcceptors = proplists:get_value(num_acceptors, RanchOpts, 100),
    Transport = proplists:get_value(transport, RanchOpts, ranch_tcp),
    TransOpts = proplists:get_value(trans_opts, RanchOpts, [{port, 8877}]),
    NICs = proplists:get_value(bind, RtransferOpts, [{0, 0, 0, 0}]),

    {ok, _} = ranch:start_listener(?GATEWAY_LISTENER, NbAcceptors, Transport,
        TransOpts, gateway_protocol_handler, RtransferOpts),

    QueueLoopPid = proc_lib:spawn_link(?MODULE, start_queue_loop,
        [length(NICs) * ?connection_load_factor]),

    register(gw_queue_loop, QueueLoopPid),

    ok = pg2:create(gateway),
    ok = pg2:join(gateway, self()),
    {ok, RtransferOpts}.


%%--------------------------------------------------------------------
%% @doc
%% Handle a message.
%% @end
%%--------------------------------------------------------------------
handle_cast(#gw_fetch{} = Request, State) ->
    Block = repackage(Request),
    rt_utils:push(?GATEWAY_INCOMING_QUEUE, Block),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @doc
%% Cleanup any state associated with the module.
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok = ranch:stop_listener(?GATEWAY_LISTENER),
    catch exit(whereis(gw_queue_loop), shutdown),
    ok.


handle_call(_Request, _From, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%% @doc
%% Computes a sha256 hash of an encoded protobuf #filerequest
%% @end
%%--------------------------------------------------------------------
-spec compute_request_hash(RequestBytes :: iodata()) -> Hash :: binary().
compute_request_hash(RequestBytes) ->
    crypto:hash(sha256, RequestBytes).


%%--------------------------------------------------------------------
%% @doc
%% Notifies a process about something related to the action it required.
%% @end
%%--------------------------------------------------------------------
-spec notify(What :: atom(), Details :: term(), Action :: #gw_fetch{}) -> ok.
notify(What, Details, #gw_fetch{notify = Notify} = Action) when is_atom(What) ->
    lists:foreach(fun(Pid) -> Pid ! {What, Details, Action} end, Notify),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Takes elements from the incoming queue and passes them to the communicator.
%% Ensures that at most Max operations are currently running.
%% @end
%%--------------------------------------------------------------------
-spec queue_loop(Max :: pos_integer(), Running :: non_neg_integer(),
    SubRef :: reference()) -> no_return().
queue_loop(Max, Running, SubRef) when Running =:= Max ->
    RunningDelta = wait_for_messages(SubRef),
    gateway:queue_loop(Max, Running + RunningDelta, SubRef);

queue_loop(Max, Running, SubRef) ->
    FetchResult = rt_utils:pop(?GATEWAY_INCOMING_QUEUE),
    RunningDelta = act_on_fetch_result(FetchResult, SubRef),
    gateway:queue_loop(Max, Running + RunningDelta, SubRef).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Subscribes for notifications from the incoming queue and enters a queue_loop.
%% @end
%%--------------------------------------------------------------------
-spec start_queue_loop(Max :: pos_integer()) -> no_return().
start_queue_loop(Max) ->
    SubRef = make_ref(),
    rt_priority_queue:subscribe(?GATEWAY_INCOMING_QUEUE, self(), SubRef),
    queue_loop(Max, 0, SubRef).


%%--------------------------------------------------------------------
%% @doc
%% Performs an operation on result of rt_priority_queue:pop. When the pop was
%% successful, passess the retrieved block to gateway dispatcher; otherwise
%% waits for new elements. Returns the change in number of currently running
%% jobs.
%% @end
%%--------------------------------------------------------------------
-spec act_on_fetch_result(Result :: {ok, #rt_block{}} | {error, term()},
    SubRef :: reference()) -> integer().
act_on_fetch_result({ok, Block}, _SubRef) ->
    #gw_fetch{notify = Notify} = Request = repackage(Block),
    MyRequest = Request#gw_fetch{notify = [self() | Notify]},
    gen_server2:cast(?GATEWAY_DISPATCHER, MyRequest),
    1;

act_on_fetch_result({error, empty}, SubRef) ->
    wait_for_messages(SubRef).


%%--------------------------------------------------------------------
%% @doc
%% Awaits notifications about new elements in the incoming queue, or end
%% notifications of scheduled fetch jobs. Returns the change in number of
%% current jobs.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_messages(SubRef :: reference()) -> integer().
wait_for_messages(SubRef) ->
    receive
        {not_empty, SubRef} -> 0;
        {_What, _Details, #gw_fetch{}} -> -1
    after
        timer:minutes(1) ->
            % TODO VFS-4025
            0
    end.


%%--------------------------------------------------------------------
%% @doc
%% Repackages #gw_fetch record into #rt_block and vice versa.
%% @end
%%--------------------------------------------------------------------
-spec repackage(#gw_fetch{}) -> #rt_block{}; (#rt_block{}) -> #gw_fetch{}.
repackage(#gw_fetch{file_id = FileId, offset = Offset, size = Size, remote = Remote, notify = Notify, retry = Retry}) ->
    #rt_block{file_id = FileId, offset = Offset, size = Size, provider_ref = Remote, terms = Notify, retry = Retry};

repackage(#rt_block{file_id = FileId, offset = Offset, size = Size, provider_ref = Remote, terms = Notify, retry = Retry}) ->
    #gw_fetch{file_id = FileId, offset = Offset, size = Size, remote = Remote, notify = Notify, retry = Retry}.
