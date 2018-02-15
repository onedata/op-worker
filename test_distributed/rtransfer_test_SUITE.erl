%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2015, ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% RTransfer tests.
%%% @end
%%%-------------------------------------------------------------------
-module(rtransfer_test_SUITE).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/oz/oz_providers.hrl").


%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
%%-export([all/0, init_per_testcase/2, end_per_testcase/2, init_per_suite/1, end_per_suite/1]).

-export([less_than_block_fetch_test/1, exact_block_size_fetch_test/1,
    more_than_block_fetch_test/1, more_than_block_fetch_test2/1, cancel_fetch_test/1,
    error_open_fun_test/1, error_read_fun_test/1, error_write_fun_test/1, many_requests_test/1,
    many_same_requests_test/1, many_requests_to_one_file/1, request_bigger_than_file_test/1,
    offset_greater_than_file_size_test/1, restart_gateway_test/1, restart_rtransfer_server_test/1]).

-export([read_fun/1, write_fun/1, counter/1, onCompleteCounter/1, data_counter/2]).

all() ->
    ?ALL([
        less_than_block_fetch_test,
        restart_gateway_test,
        restart_rtransfer_server_test,
        exact_block_size_fetch_test,
        more_than_block_fetch_test,
        more_than_block_fetch_test2,
%%         TODO - uncomment below test after resolving VFS-1573
%%         cancel_fetch_test,
        many_requests_test,
        many_same_requests_test,
        many_requests_to_one_file,
%%         TODO - uncomment below 3 tests after resolving VFS-1574
%%         error_open_fun_test,
%%         error_read_fun_test,
%%         error_write_fun_test,
        offset_greater_than_file_size_test,
        request_bigger_than_file_test
    ]).

-define(FILE_HANDLE, <<"file_handle">>).
-define(FILE_HANDLE2, <<"file_handle2">>).
-define(TEST_FILE_UUID, <<"file_uuid">>).
-define(TEST_BLOCK_SIZE, 1024).
-define(NUM_ACCEPTORS, 10).
-define(RTRANSFER_PORT, 6665).
-define(TEST_OFFSET, 0).
-define(TIMEOUT, timer:seconds(120)).
-define(SLEEP_TIMEOUT, timer:seconds(120)).
-define(STREAMS_NUM, 50).

-define(DEFAULT_RTRANSFER_OPTS,
    [
        {block_size, ?TEST_BLOCK_SIZE},
        {get_nodes_fun, get_nodes()},
        {open_fun, open_fun({ok, ?FILE_HANDLE})},
        {close_fun, close_fun()},
        {ranch_opts,
            [
                {num_acceptors, ?NUM_ACCEPTORS},
                {transport, ranch_tcp},
                {trans_opts, [{port, ?RTRANSFER_PORT}]}
            ]
        },
        {bind, lists:duplicate(?STREAMS_NUM, {0, 0, 0, 0})}
    ]
).

-define(REMOTE_APPLIER, applier).


%%%===================================================================
%%% Test functions
%%%===================================================================
less_than_block_fetch_test(Config) ->
    %% test fetching data of size smaller then rtransfer block_size
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 32,
    Data = generate_binary(DataSize),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    CounterPid = spawn(?MODULE, counter, [0]),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    prepare_rtransfer({Worker1, RtransferOpts1}, {Worker2, RtransferOpts2},
        ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize,
        notify_fun(CounterPid), on_complete_fun(self())
    ),

    %% then
    ?assertReceivedMatch({on_complete, {ok, DataSize}}, ?TIMEOUT),
    stop_counter(CounterPid),
    ?assertReceivedMatch({counter, 1}, ?TIMEOUT).

restart_gateway_test(Config) ->
    %% test fetching data of size smaller then rtransfer block_size
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 32,
    Data = generate_binary(DataSize),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    CounterPid = spawn(?MODULE, counter, [0]),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    ?assertMatch({ok, _}, start_gateway(Worker1, RtransferOpts1)),
    ?assertMatch({ok, _}, start_gateway(Worker2, RtransferOpts2)),

    Ref = prepare_fetch_request(Worker1, Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize),
    ok = rpc:call(Worker1, gen_server2, stop, [gateway, test_reason, infinity]),
    timer:sleep(timer:seconds(1)),
    fetch_data(Worker1, Ref, notify_fun(CounterPid), on_complete_fun(self())),


    %% then
    ?assertReceivedMatch({on_complete, {ok, DataSize}}, ?TIMEOUT),
    stop_counter(CounterPid),
    ?assertReceivedMatch({counter, 1}, ?TIMEOUT).

restart_rtransfer_server_test(Config) ->
    %% test fetching data of size smaller then rtransfer block_size
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 32,
    Data = generate_binary(DataSize),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    CounterPid = spawn(?MODULE, counter, [0]),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    ?assertMatch({ok, _}, start_gateway(Worker1, RtransferOpts1)),
    ?assertMatch({ok, _}, start_gateway(Worker2, RtransferOpts2)),

    Ref = prepare_fetch_request(Worker1, Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize),
    ok = rpc:call(Worker2, gen_server2, stop, [{global, rtransfer}, test_reason, infinity]),
    fetch_data(Worker1, Ref, notify_fun(CounterPid), on_complete_fun(self())),

    %% then
    ?assertReceivedMatch({on_complete, {ok, DataSize}}, ?TIMEOUT),
    stop_counter(CounterPid),
    ?assertReceivedMatch({counter, 1}, ?TIMEOUT).

exact_block_size_fetch_test(Config) ->
    %% test fetching data of size equal to rtransfer block_size
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = ?TEST_BLOCK_SIZE,
    Data = generate_binary(DataSize),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    CounterPid = spawn(?MODULE, counter, [0]),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    prepare_rtransfer({Worker1, RtransferOpts1}, {Worker2, RtransferOpts2},
        ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize,
        notify_fun(CounterPid), on_complete_fun(self())
    ),

    %% then
    ?assertReceivedMatch({on_complete, {ok, DataSize}}, ?TIMEOUT),
    stop_counter(CounterPid),
    ?assertReceivedMatch({counter, 1}, ?TIMEOUT).

more_than_block_fetch_test(Config) ->
    %% test fetching data of size bigger then rtransfer block_size
    %% data size is the multiple of block size

    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    Blocks = 1024,
    DataSize = Blocks * ?TEST_BLOCK_SIZE,
    Data = generate_binary(DataSize),
    CounterPid = spawn(?MODULE, counter, [0]),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, ?TEST_BLOCK_SIZE}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    prepare_rtransfer({Worker1, RtransferOpts1}, {Worker2, RtransferOpts2},
        ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize,
        notify_fun(CounterPid), on_complete_fun(self())
    ),

    %% then
    ?assertReceivedMatch({on_complete, {ok, DataSize}}, ?TIMEOUT),
    stop_counter(CounterPid),
    ?assertReceivedMatch({counter, Blocks}, ?TIMEOUT).

more_than_block_fetch_test2(Config) ->
    %% test fetching data of size bigger then rtransfer block_size
    %% data size is not the multiple of block size

    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    Blocks = 1025,
    DataSize = (Blocks - 1) * ?TEST_BLOCK_SIZE + 576,
    Data = generate_binary(DataSize),
    CounterPid = spawn(?MODULE, counter, [0]),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    %% write will return different values beacuse DataSize is not
    %% the multiple of block size
    WriteFunOpt = {write_fun,
        fun(_Handle, _Offset, _Data) ->
            {ok, ?FILE_HANDLE2, byte_size(_Data) }
        end
    },
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    prepare_rtransfer({Worker1, RtransferOpts1}, {Worker2, RtransferOpts2},
        ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize,
        notify_fun(CounterPid), on_complete_fun(self())
    ),

    %% then
    ?assertReceivedMatch({on_complete, {ok, DataSize}}, ?TIMEOUT),
    stop_counter(CounterPid),
    ?assertReceivedMatch({counter, Blocks}, ?TIMEOUT).

cancel_fetch_test(Config) ->
    %% test cancelling request during fetching
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 1024 * ?TEST_BLOCK_SIZE,
    Data = generate_binary(DataSize),
    ReadFunOpt = {read_fun,
        fun(_Handle, _Offset, _size) ->
            timer:sleep(timer:seconds(60)),
            {ok, ?FILE_HANDLE2, Data}
        end
    },
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    Ref = prepare_rtransfer({Worker1, RtransferOpts1}, {Worker2, RtransferOpts2},
        ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize,
        notify_fun(), on_complete_fun(self())
    ),
    cancel_fetching(Worker1, Ref),

    timer:sleep(?SLEEP_TIMEOUT),
    %% then
    ?assertReceivedMatch({on_complete, {error, canceled}}, ?TIMEOUT).

many_requests_test(Config) ->
    %% test fetching many files
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 32,
    RequestsNum = 1000,
    Data = generate_binary(DataSize),
    CounterPid = spawn(?MODULE, onCompleteCounter, [#{ok => 0, errors => 0, reason => []}]),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    ?assertMatch({ok, _}, start_gateway(Worker1, RtransferOpts1)),
    ?assertMatch({ok, _}, start_gateway(Worker2, RtransferOpts2)),
    Refs = generate_requests_to_many_files(RequestsNum, Worker1, Worker2, DataSize),
    fetch_many(Refs, Worker1, notify_fun(), on_complete_fun(CounterPid)),

    timer:sleep(?SLEEP_TIMEOUT),
    stop_counter(CounterPid),
    %% then
    ?assertReceivedMatch(
        {counterOnComplete, {ok, RequestsNum}, {errors, 0}}, ?TIMEOUT).

many_same_requests_test(Config) ->
    %% test fetching many files
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 32,
    RequestsNum = 1000,
    Data = generate_binary(DataSize),
    CounterPid = spawn(?MODULE, onCompleteCounter, [#{ok => 0, errors => 0, reason => []}]),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    ?assertMatch({ok, _}, start_gateway(Worker1, RtransferOpts1)),
    ?assertMatch({ok, _}, start_gateway(Worker2, RtransferOpts2)),
    Refs = generate_many_same_requests(RequestsNum, Worker1, Worker2, DataSize),
    fetch_many(Refs, Worker1, notify_fun(), on_complete_fun(CounterPid)),

    timer:sleep(?SLEEP_TIMEOUT),
    stop_counter(CounterPid),
    %% then
    ?assertReceivedMatch(
        {counterOnComplete, {ok, RequestsNum}, {errors, 0}}, ?TIMEOUT).

many_requests_to_one_file(Config) ->
    %% test sending many requests to one file
    %% requested blocks of file are intersected
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 1024,
    Chunk = 4,
    %% should be 511 requests
    RequestsNum = DataSize div Chunk + (DataSize - 1) div Chunk,
    Data = generate_binary(DataSize),
    CounterPid = spawn(?MODULE, onCompleteCounter, [#{ok => 0, errors => 0, reason => []}]),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    ?assertMatch({ok, _}, start_gateway(Worker1, RtransferOpts1)),
    ?assertMatch({ok, _}, start_gateway(Worker2, RtransferOpts2)),
    Refs = generate_requests_to_one_file(RequestsNum, Worker1, Worker2, Chunk),
    fetch_many(Refs, Worker1, notify_fun(), on_complete_fun(CounterPid)),

    timer:sleep(?SLEEP_TIMEOUT),
    stop_counter(CounterPid),
    %% then
    ?assertReceivedMatch(
        {counterOnComplete, {ok, RequestsNum}, {errors, 0}}, ?TIMEOUT).

error_open_fun_test(Config) ->
    %% test with open_fun callback returning an error
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = ?TEST_BLOCK_SIZE,
    Data = generate_binary(DataSize),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 =
        change_rtransfer_opt({open_fun, {error, test_error}}, RtransferOpts),

    %% when
    prepare_rtransfer(
        {Worker1, [RtransferOpts2]},
        {Worker2, [RtransferOpts2]},
        ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize,
        notify_fun(), on_complete_fun(self())
    ),

    %% then
    ?assertReceivedMatch({on_complete, {error,{storage,_}}}, ?TIMEOUT).

error_read_fun_test(Config) ->
    %% test with read_fun callback returning an error
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = ?TEST_BLOCK_SIZE,
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {error, ?FILE_HANDLE2, test_error}),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    prepare_rtransfer(
        {Worker1, [RtransferOpts1]},
        {Worker2, [RtransferOpts2]},
        ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize,
        notify_fun(), on_complete_fun(self())
    ),

    %% then
    ?assertReceivedMatch({on_complete, {error,{storage,_}}}, ?TIMEOUT).

error_write_fun_test(Config) ->
    %% test with write_fun callback returning an error
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = ?TEST_BLOCK_SIZE,
    Data = generate_binary(DataSize),
    WriteFunOpt = make_opt_fun(write_fun, {error, ?FILE_HANDLE2, test_error}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    prepare_rtransfer(
        {Worker1, [RtransferOpts1]},
        {Worker2, [RtransferOpts2]},
        ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize,
        notify_fun(), on_complete_fun(self())
    ),

    %% then
    ?assertReceivedMatch({on_complete, {error,{storage,_}}}, ?TIMEOUT).

offset_greater_than_file_size_test(Config) ->
    %% test calling fetch with file offset greater than file size
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = ?TEST_BLOCK_SIZE,
    %% offset is bigger than DataSize
    Offset = DataSize + 10,
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, 0}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, <<"">>}),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    prepare_rtransfer({Worker1, RtransferOpts1}, {Worker2, RtransferOpts2},
        ?TEST_FILE_UUID, Offset, DataSize,
        notify_fun(), on_complete_fun(self())
    ),

    %% then
    ?assertReceivedMatch({on_complete, {ok, 0}}, ?TIMEOUT).

request_bigger_than_file_test(Config) ->
    %% testing case when requested size is greater than file's size
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 8 * ?TEST_BLOCK_SIZE,
    %% offset is bigger than DataSize
    RequestOffset = DataSize div 3,
    SizeToFetch = DataSize - RequestOffset,
    DataCounterPid = spawn(?MODULE, data_counter, [DataSize, SizeToFetch]),
    CounterPid = spawn(?MODULE, counter, [0]),

    WriteFunOpt = {write_fun,
        fun(_Handle, _Offset, Data) ->
            {ok, ?FILE_HANDLE2, byte_size(Data) }
        end
    },
    %% read_fun that will simulate subsequent reads from file
    %% last read will return <<"">> when it reaches end of file
    ReadFunOpt = {read_fun,
        fun(_Handle, Offset, Size) ->
            DataCounterPid ! {self(), request, Offset, Size},
            ReadSize = receive
                {data_counter, Return} -> Return
            end,
            {ok, ?FILE_HANDLE2, generate_binary(ReadSize) }
        end
    },

    RtransferOpts1 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    prepare_rtransfer({Worker1, RtransferOpts1}, {Worker2, RtransferOpts2},
        ?TEST_FILE_UUID, RequestOffset, DataSize, notify_fun(CounterPid), on_complete_fun(self())
    ),

    ?assertReceivedMatch({on_complete, {ok, SizeToFetch}}, ?TIMEOUT),
    stop_counter(CounterPid),
    ?assertReceivedMatch({counter, 6}, ?TIMEOUT).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        Workers = ?config(op_worker_nodes, NewConfig),
        application:start(ssl),
        hackney:start(),
        lists:foreach(fun(Worker) ->
            start_applier(Worker, ?REMOTE_APPLIER)
        end, Workers),
        mock_resolve_ips(Workers),
        NewConfig
    end,
    ssl:start(),
    hackney:start(),
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer]}
        | Config
    ].

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        start_applier(Worker, ?REMOTE_APPLIER)
    end, Workers),
    Config.

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun stop_applier/1, Workers),
    Config.

end_per_suite(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    hackney:stop(),
    ssl:stop(),
    unmock_resolve_ips(Workers).

%%%===================================================================
%%% Internal functions
%%%===================================================================

mock_resolve_ips(Nodes) ->
    [Node1, Node2] = Nodes,
    {ok, Ip1} = inet:parse_address(binary_to_list(test_utils:get_docker_ip(Node1))),
    {ok, Ip2} = inet:parse_address(binary_to_list(test_utils:get_docker_ip(Node2))),
    ok = test_utils:mock_new(Nodes, provider_logic),
    ok = test_utils:mock_expect(Nodes, provider_logic, resolve_ips, fun(ProviderId) ->
        case ProviderId of
            <<"p1">> ->
                {ok, [Ip2]};
            <<"p2">> ->
                {ok, [Ip1]};
            _ ->
                meck:passthrough([ProviderId])
        end
    end).

unmock_resolve_ips(Nodes) ->
    ok = test_utils:mock_unload(Nodes, provider_logic).

get_nodes() ->
    fun(ProviderId) ->
        [{utils:get_host(ProviderId), ?RTRANSFER_PORT}]
    end.

read_fun(Expected) ->
    fun(_Handle, _Offset, _Size) ->
        Expected
    end.

write_fun(Expected) ->
    fun(_Handle, _Offset, _Data) ->
        Expected
    end.

open_fun(Expected) ->
    fun(_UUID, _Mode) -> Expected end.

close_fun() ->
    fun(_Handle) -> ok end.

notify_fun() ->
    fun(_Ref, _Offset,_Size) ->
        ok
    end.

notify_fun(CounterPid) ->
    fun(_Ref, _Offset,_Size) ->
        CounterPid ! increase,
        ok
    end.

on_complete_fun(Pid) ->
    fun(_Ref, Arg) ->
        Pid ! {on_complete, Arg},
        ok
    end.

change_rtransfer_opt({OptKey, OptValue}, Opts) ->
    case lists:keysearch(OptKey, 1, Opts) of
        false -> [{OptKey, OptValue} | Opts];
        _ -> lists:keyreplace(OptKey, 1, Opts, {OptKey, OptValue})
    end.

make_opt_fun(OptName, Expected) ->
    {OptName, apply(?MODULE, OptName, [Expected])}.

start_applier(Node, ApplierName) ->
    rpc:call(Node, erlang, register,
        [ApplierName, spawn_link(Node,
            fun Loop() ->
                receive
                    {Pid, Mod, Fun, Args} ->
                        Pid ! apply(Mod, Fun, Args),
                        Loop();
                    stop -> ok
                end
            end)
        ], ?TIMEOUT
    ).

stop_applier(Node) ->
    {?REMOTE_APPLIER, Node} ! stop.

remote_apply(Node, Mod, Fun, Args) ->
    {?REMOTE_APPLIER, Node} ! {self(), Mod, Fun, Args},
    receive
        Return -> Return
    end.

generate_binary(Length) ->
    Generate =
        fun Gen(Len, Acc) ->
            case (Len =< 255) of
                true ->
                    NewBin = list_to_binary(lists:seq(1, Len)),
                    <<Acc/binary, NewBin/binary>>;
                _ ->
                    NewBin = list_to_binary(lists:seq(1, 255)),
                    Gen(Len - 255, <<Acc/binary, NewBin/binary>>)
            end
        end,
    Generate(Length, <<>>).

counter(State) ->
    receive
        increase ->
            counter(State + 1);
        {return, Pid} ->
            Pid ! {counter, State}
    end.

stop_counter(CounterPid) ->
    CounterPid ! {return, self()}.

data_counter(DataSize, ActualSize) ->
    receive
        {Pid, request, Offset, RequestedSize} when RequestedSize >= ActualSize ->
            AvailableSize = erlang:max(erlang:min(DataSize - Offset, ActualSize), 0),
            Pid ! {data_counter, AvailableSize},
            data_counter(DataSize, ActualSize - AvailableSize);
        {Pid, request, Offset, RequestedSize} ->
            AvailableSize = erlang:max(erlang:min(DataSize - Offset, RequestedSize), 0),
            Pid ! {data_counter, AvailableSize},
            data_counter(DataSize, ActualSize - AvailableSize)
    end.

onCompleteCounter(#{ok := OK, errors := Errors, reason := Reason} = State) ->
    receive
        {on_complete, {ok, _DataSize}} ->
            onCompleteCounter(State#{ok => OK + 1});
        {on_complete, {error, Error}} ->
            onCompleteCounter(State#{errors => Errors + 1, reason => [Error | Reason]});
        {return, Pid} ->
            case maps:get(errors, State) of
                0 ->
                    Pid ! {counterOnComplete, {ok, maps:get(ok, State)},
                        {errors, maps:get(errors, State)}};
                _ ->
                    Pid ! {counterOnComplete, {ok, maps:get(ok, State)},
                        {errors, maps:get(errors, State)}, {reason, Reason}}
            end
    end.

generate_requests_to_many_files(RequestsNum, Worker1, Worker2, DataSize) ->
    ProviderId2 = provider_id(Worker2),
    [
        remote_apply(Worker1, rtransfer, prepare_request,
            [ProviderId2, <<?TEST_FILE_UUID/binary, (list_to_binary(integer_to_list(I)))/binary>>,
                ?TEST_OFFSET, DataSize])
        || I <- lists:seq(1, RequestsNum)
    ].

generate_many_same_requests(RequestsNum, Worker1, Worker2, DataSize) ->
    ProviderId2 = provider_id(Worker2),
    [
        remote_apply(Worker1, rtransfer, prepare_request,
            [ProviderId2, <<?TEST_FILE_UUID/binary>>, ?TEST_OFFSET, DataSize])
        || _ <- lists:seq(1, RequestsNum)
    ].

generate_requests_to_one_file(RequestsNum, Worker1, Worker2, Chunk) ->
    ProviderId2 = provider_id(Worker2),
    [
        remote_apply(Worker1, rtransfer, prepare_request,
            [ProviderId2, ?TEST_FILE_UUID, Offset, Chunk])
        || Offset <- lists:seq(0, 2 * RequestsNum - 1 , Chunk div 2)
    ].

fetch_many(Refs, Worker1, Notify, OnComplete) ->
    lists:foreach(
        fun(Ref) ->
            remote_apply(
                Worker1, rtransfer, fetch,
                [Ref, Notify, OnComplete]
            )
        end, Refs).

%% returns ip (as a string) of given node
get_node_ip(Node) ->
    CMD = "docker inspect --format '{{ .NetworkSettings.IPAddress }}'" ++ " " ++
        utils:get_host(Node),
    re:replace(os:cmd(CMD), "\\s+", "", [global, {return, list}]).


start_gateway(Node, RtransferOpts) ->
    remote_apply(Node, fslogic_worker, restart_gateway, [RtransferOpts]).

prepare_fetch_request(Node1, Node2, FileUUID, Offset, DataSize) ->
%% Node1 is the one who fetches data from Node2
    ProviderId2 = provider_id(Node2),
    remote_apply(
        Node1, rtransfer, prepare_request, [ProviderId2, FileUUID, Offset, DataSize]
    ).

fetch_data(Node, Ref, NotifyFun, OnCompleteFun) ->
    remote_apply(Node, rtransfer, fetch, [Ref, NotifyFun, OnCompleteFun]).

cancel_fetching(Node, Ref) ->
    remote_apply(Node, rtransfer, cancel, [Ref]).

prepare_rtransfer({Worker1, Ropts1}, {Worker2, Ropts2}, FileUUID, Offset, DataSize, NotifyFun, OnCompleteFun) ->
    ?assertMatch({ok, _}, start_gateway(Worker1, Ropts1)),
    ?assertMatch({ok, _}, start_gateway(Worker2, Ropts2)),
    Ref = prepare_fetch_request(Worker1, Worker2, FileUUID, Offset, DataSize),
    fetch_data(Worker1, Ref, NotifyFun, OnCompleteFun).

provider_id(Node) ->
    Hostname = list_to_binary(utils:get_host(Node)),
    [_, Id | _] = binary:split(Hostname, <<".">>, [global]),
    Id.