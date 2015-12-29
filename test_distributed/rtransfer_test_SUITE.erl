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
-include_lib("annotations/include/annotations.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([less_then_block_fetch_test/1, exact_block_size_fetch_test/1,
    more_then_block_fetch_test/1, cancel_fetch_test/1, error_open_fun_test/1,
    error_read_fun_test/1, error_write_fun_test/1, many_requests_test/1,
    many_requests_to_one_file/1]).

-performance({test_cases, []}).
all() ->
    [
        less_then_block_fetch_test,
        exact_block_size_fetch_test,
        more_then_block_fetch_test,
        cancel_fetch_test,
        many_requests_test,
        many_requests_to_one_file,
        error_open_fun_test,
        error_read_fun_test,
        error_write_fun_test
    ].

-define(FILE_HANDLE, <<"file_handle">>).
-define(FILE_HANDLE2, <<"file_handle2">>).
-define(TEST_FILE_UUID, <<"file_uuid">>).
-define(TEST_BLOCK_SIZE, 1024).
-define(NUM_ACCEPTORS, 10).
-define(RTRANSFER_PORT, 6666).
-define(TEST_OFFSET, 0).
-define(TIMEOUT, timer:seconds(60)).

-define(COOKIE_OP1, 'test_cookie').
-define(COOKIE_OP2, 'test_cookie2').

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
        }
    ]
).

-define(REMOTE_APPLIER, applier).


%%%===================================================================
%%% Test functions
%%%===================================================================

less_then_block_fetch_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 32,
    Data = generate_binary(DataSize),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    CounterPid = spawn(?MODULE, counter, [0]),
    RtransferOpts = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    remote_apply(Worker1, rtansfer, start_link, [RtransferOpts]),
    remote_apply(Worker2, rtansfer, start_link, [RtransferOpts]),

    Ref1 = remote_apply(
        Worker1, rtransfer, prepare_request,
        [Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize]
    ),

    remote_apply(
        Worker1, rtransfer, fetch,
        [Ref1, notify_fun(CounterPid), on_complete_fun(self())]
    ),

    %% then
    CounterPid ! {return, self()},
    ?assertReceivedEqual({counter, 1}, ?TIMEOUT),
    ?assertReceivedEqual({on_complete, {ok, DataSize}}, ?TIMEOUT).

exact_block_size_fetch_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = ?TEST_BLOCK_SIZE,
    Data = generate_binary(DataSize),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    CounterPid = spawn(?MODULE, counter, [0]),
    RtransferOpts = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    remote_apply(Worker1, rtansfer, start_link, [RtransferOpts]),
    remote_apply(Worker2, rtansfer, start_link, [RtransferOpts]),

    Ref1 = remote_apply(
        Worker1, rtransfer, prepare_request,
        [Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize]
    ),

    remote_apply(
        Worker1, rtransfer, fetch,
        [Ref1, notify_fun(CounterPid), on_complete_fun(self())]
    ),

    %% then
    CounterPid ! {return, self()},
    ?assertReceivedEqual({counter, 1}, ?TIMEOUT),
    ?assertReceivedEqual({on_complete, {ok, DataSize}}, ?TIMEOUT).

more_then_block_fetch_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 1024 * ?TEST_BLOCK_SIZE,
    Data = generate_binary(DataSize),
    CounterPid = spawn(?MODULE, counter, [0]),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    remote_apply(Worker1, rtansfer, start_link, [RtransferOpts]),
    remote_apply(Worker2, rtansfer, start_link, [RtransferOpts]),

    Ref1 = remote_apply(
        Worker1, rtransfer, prepare_request,
        [Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize]
    ),

    remote_apply(
        Worker1, rtransfer, fetch,
        [Ref1, notify_fun(CounterPid), on_complete_fun(self())]
    ),

    %% then
    CounterPid ! {return, self()},
    ?assertReceivedEqual({counter, 1024}, ?TIMEOUT),
    ?assertReceivedEqual({on_complete, {ok, DataSize}}, ?TIMEOUT).

cancel_fetch_test(Config) ->
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
    RtransferOpts = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    remote_apply(Worker1, rtansfer, start_link, [RtransferOpts]),
    remote_apply(Worker2, rtansfer, start_link, [RtransferOpts]),

    Ref1 = remote_apply(
        Worker1, rtransfer, prepare_request,
        [Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize]
    ),

    remote_apply(
        Worker1, rtransfer, fetch,
        [Ref1, notify_fun(), on_complete_fun(self())]
    ),
    remote_apply(Worker1, rtransfer, cancel, [Ref1]),

    %% then
    ?assertReceivedEqual({on_complete, {error, canceled}}, ?TIMEOUT).

many_requests_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 32,
    RequestsNum = 1000,
    Data = generate_binary(DataSize),
    CounterPid = spawn(?MODULE, counterOnComplete, [#{ok => 0, errors => 0}]),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    remote_apply(Worker1, rtansfer, start_link, [RtransferOpts]),
    remote_apply(Worker2, rtansfer, start_link, [RtransferOpts]),

    Refs = generate_requests_to_many_files(RequestsNum, Worker1, Worker2, DataSize),

    fetch_many(Refs, Worker1, notify_fun(), on_complete_fun(CounterPid)),

    %% then
    CounterPid ! {return, self()},
    ?assertReceivedEqual(
        {counterOnComplete, {ok, RequestsNum}, {erros, 0}}, ?TIMEOUT),
    ?assertReceivedEqual({on_complete, {ok, DataSize}}, ?TIMEOUT).

many_requests_to_one_file(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 1024,
    Chunk = 4,
    RequestsNum = DataSize div Chunk + (DataSize -1) div Chunk,
    Data = generate_binary(DataSize),
    CounterPid = spawn(?MODULE, counterOnComplete, [#{ok => 0, errors => 0}]),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    remote_apply(Worker1, rtansfer, start_link, [RtransferOpts]),
    remote_apply(Worker2, rtansfer, start_link, [RtransferOpts]),

    Refs =
        generate_requests_to_one_file(RequestsNum, Worker1, Worker2, Chunk),

    fetch_many(Refs, Worker1, notify_fun(), on_complete_fun(CounterPid)),

    %% then
    CounterPid ! {return, self()},
    ?assertReceivedEqual(
        {counterOnComplete, {ok,RequestsNum}, {erros, 0}}, ?TIMEOUT),
    ?assertReceivedEqual({on_complete, {ok, DataSize}}, ?TIMEOUT).

error_open_fun_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 1024 * ?TEST_BLOCK_SIZE,
    Data = generate_binary(DataSize),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    RtransferOpts2 =
        change_rtransfer_opt({open_fun, {error, test_error}}, RtransferOpts),

    %% when
    remote_apply(Worker1, rtansfer, start_link, [RtransferOpts2]),
    remote_apply(Worker2, rtansfer, start_link, [RtransferOpts2]),

    Ref1 = remote_apply(
        Worker1, rtransfer, prepare_request,
        [Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize]
    ),

    remote_apply(
        Worker1, rtransfer, fetch,
        [Ref1, notify_fun(), on_complete_fun(self())]
    ),
    remote_apply(Worker1, rtransfer, cancel, [Ref1]),

    %% then
    ?assertReceivedEqual({on_complete, {error, test_error}}, ?TIMEOUT).

error_read_fun_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 1024 * ?TEST_BLOCK_SIZE,
    Data = generate_binary(DataSize),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {error, ?FILE_HANDLE2, test_error}),
    RtransferOpts = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    remote_apply(Worker1, rtansfer, start_link, [RtransferOpts]),
    remote_apply(Worker2, rtansfer, start_link, [RtransferOpts]),

    Ref1 = remote_apply(
        Worker1, rtransfer, prepare_request,
        [Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize]
    ),

    remote_apply(
        Worker1, rtransfer, fetch,
        [Ref1, notify_fun(), on_complete_fun(self())]
    ),
    remote_apply(Worker1, rtransfer, cancel, [Ref1]),

    %% then
    ?assertReceivedEqual({on_complete, {error, test_error}}, ?TIMEOUT).

error_write_fun_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 1024 * ?TEST_BLOCK_SIZE,
    Data = generate_binary(DataSize),
    WriteFunOpt = make_opt_fun(write_fun, {error, ?FILE_HANDLE2, test_error}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts = [ReadFunOpt, WriteFunOpt | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    remote_apply(Worker1, rtansfer, start_link, [RtransferOpts]),
    remote_apply(Worker2, rtansfer, start_link, [RtransferOpts]),

    Ref1 = remote_apply(
        Worker1, rtransfer, prepare_request,
        [Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize]
    ),

    remote_apply(
        Worker1, rtransfer, fetch,
        [Ref1, notify_fun(), on_complete_fun(self())]
    ),
    remote_apply(Worker1, rtransfer, cancel, [Ref1]),

    %% then
    ?assertReceivedEqual({on_complete, {error, test_error}}, ?TIMEOUT).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    application:start(ssl2),
    hackney:start(),
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),
    start_applier(Worker1, ?REMOTE_APPLIER, ?COOKIE_OP1),
    start_applier(Worker2, ?REMOTE_APPLIER, ?COOKIE_OP2),
    Config.

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun stop_applier/1, Workers),
    hackney:stop(),
    application:stop(ssl2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
    fun(_Ref, _Size) ->
        ok
    end.

notify_fun(CounterPid) ->
    fun(_Ref, _Size) ->
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

start_applier(Node, ApplierName, Cookie) ->
    ct_rpc:call(Node, erlang, register,
        [ApplierName, spawn_link(Node,
            fun Loop() ->
                receive
                    {Pid, Mod, Fun, Args} ->
                        Pid ! apply(Mod, Fun, Args),
                        Loop();
                    stop -> ok
                end
            end)
        ], ?TIMEOUT, Cookie
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
        increase -> counter(State + 1);
        {return, Pid} -> Pid ! {counter, State}
    end.

onCompleteCounter(#{ok := OK, errors := Errors} = State) ->
    receive
        {on_complete, {ok, _DataSize}} ->
            onCompleteCounter(State#{ok => OK + 1});
        {on_complete, {error, _Error}} ->
            onCompleteCounter(State#{errors => Errors + 1});
        {return, Pid} ->
            Pid ! {counterOnComplete, {ok, maps:get(ok, State)}, {errors, maps:get(errors, State)}}
    end.

generate_requests_to_many_files(RequestsNum, Worker1, Worker2, DataSize) ->
    [
        remote_apply(Worker1, rtransfer, prepare_request,
            [Worker2, <<?TEST_FILE_UUID/binary, I>>, ?TEST_OFFSET, DataSize])
        || I <- lists:seq(1,RequestsNum)
    ].

generate_requests_to_one_file(RequestsNum, Worker1, Worker2, Chunk) ->
    [
        remote_apply(Worker1, rtransfer, prepare_request,
            [Worker2, ?TEST_FILE_UUID, I, Chunk/2])
        || I <- lists:seq(0, RequestsNum - 2, 2)
    ].

fetch_many(Refs, Worker1, Notify, OnComplete) ->
    lists:foreach(
        fun(Ref) ->
            remote_apply(
                Worker1, rtransfer, fetch,
                [Ref, Notify, OnComplete]
            )
        end, Refs).