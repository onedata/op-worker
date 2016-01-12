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

-export([less_than_block_fetch_test/1, exact_block_size_fetch_test/1,
    more_than_block_fetch_test/1, cancel_fetch_test/1, error_open_fun_test/1,
    error_read_fun_test/1, error_write_fun_test/1, many_requests_test/1,
    many_requests_to_one_file/1, bad_formatted_request_test/1, bad_file_offset_test/1]).

-export([read_fun/1, write_fun/1, counter/1, onCompleteCounter/1]).

-performance({test_cases, []}).
all() ->
    [
        less_than_block_fetch_test,
        exact_block_size_fetch_test,
        more_than_block_fetch_test,
        cancel_fetch_test,
        many_requests_test,
        many_requests_to_one_file,
        error_open_fun_test,
        error_read_fun_test,
        error_write_fun_test,
        bad_formatted_request_test,
        bad_file_offset_test
    ].

-define(FILE_HANDLE, <<"file_handle">>).
-define(FILE_HANDLE2, <<"file_handle2">>).
-define(TEST_FILE_UUID, <<"file_uuid">>).
-define(TEST_BLOCK_SIZE, 1024).
-define(NUM_ACCEPTORS, 10).
-define(RTRANSFER_PORT, 6666).
-define(TEST_OFFSET, 0).
-define(TIMEOUT, timer:seconds(120)).

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
less_than_block_fetch_test(Config) ->
    %% test fetching data of size smaller then rtransfer block_size
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 32,
    Data = generate_binary(DataSize),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    CounterPid = spawn(?MODULE, counter, [0]),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt, get_binding(Worker1) | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt, get_binding(Worker2) | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    ?assertMatch({ok, _},
        remote_apply(Worker1, rtransfer, start_link, [RtransferOpts1])
    ),
    ?assertMatch({ok, _},
        remote_apply(Worker2, rtransfer, start_link, [RtransferOpts2])
    ),

    Ref1 = remote_apply(
        Worker1, rtransfer, prepare_request,
        [Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize]
    ),

    ?assertEqual(ok,
        remote_apply(Worker1, rtransfer, fetch,
            [Ref1, notify_fun(CounterPid), on_complete_fun(self())])
    ),

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
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt, get_binding(Worker1) | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt, get_binding(Worker2) | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    ?assertMatch({ok, _},
        remote_apply(Worker1, rtransfer, start_link, [RtransferOpts1])),

    ?assertMatch({ok, _},
        remote_apply(Worker2, rtransfer, start_link, [RtransferOpts2])),

    Ref1 = remote_apply(
        Worker1, rtransfer, prepare_request,
        [Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize]
    ),

    ?assertEqual(ok,
        remote_apply(Worker1, rtransfer, fetch,
            [Ref1, notify_fun(CounterPid), on_complete_fun(self())])
    ),

    %% then
    ?assertReceivedMatch({on_complete, {ok, DataSize}}, ?TIMEOUT),
    stop_counter(CounterPid),
    ?assertReceivedMatch({counter, 1}, ?TIMEOUT).

%% TODO not working, only on notify message is sent
more_than_block_fetch_test(Config) ->
    %% test fetching data of size bigger then rtransfer block_size

    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 1024 * ?TEST_BLOCK_SIZE,
    Data = generate_binary(DataSize),
    CounterPid = spawn(?MODULE, counter, [0]),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt, get_binding(Worker1) | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt, get_binding(Worker2) | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    ?assertMatch({ok, _},
        remote_apply(Worker1, rtransfer, start_link, [RtransferOpts1])
    ),

    ?assertMatch({ok, _},
        remote_apply(Worker2, rtransfer, start_link, [RtransferOpts2])
    ),

    Ref1 = remote_apply(
        Worker1, rtransfer, prepare_request,
        [Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize]
    ),

    ?assertEqual(ok,
        remote_apply(Worker1, rtransfer, fetch,
            [Ref1, notify_fun(CounterPid), on_complete_fun(self())]
        )
    ),

    %% then
    ?assertReceivedMatch({on_complete, {ok, DataSize}}, ?TIMEOUT),
    stop_counter(CounterPid),
    ?assertReceivedMatch({counter, 1024}, ?TIMEOUT).

%% TODO not working, got {ok, Size} instead of {error, cancel}
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
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt, get_binding(Worker1) | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt, get_binding(Worker2) | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    ?assertMatch({ok, _},
        remote_apply(Worker1, rtransfer, start_link, [RtransferOpts1])
    ),

    ?assertMatch({ok, _},
        remote_apply(Worker2, rtransfer, start_link, [RtransferOpts2])
    ),

    Ref1 = remote_apply(
        Worker1, rtransfer, prepare_request,
        [Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize]
    ),

    remote_apply(
        Worker1, rtransfer, fetch,
        [Ref1, notify_fun(), on_complete_fun(self())]
    ),
    remote_apply(Worker1, rtransfer, cancel, [Ref1]),

    timer:sleep(?TIMEOUT),
    %% then
    ?assertReceivedMatch({on_complete, {error, canceled}}, ?TIMEOUT).

many_requests_test(Config) ->
    %% test fetching many files
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = 32,
    RequestsNum = 1000,
    Data = generate_binary(DataSize),
    CounterPid = spawn(?MODULE, onCompleteCounter, [#{ok => 0, errors => 0}]),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt, get_binding(Worker1) | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt, get_binding(Worker2) | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    ?assertMatch({ok, _},
        remote_apply(Worker1, rtransfer, start_link, [RtransferOpts1])
    ),

    ?assertMatch({ok, _},
        remote_apply(Worker2, rtransfer, start_link, [RtransferOpts2])
    ),

    Refs = generate_requests_to_many_files(RequestsNum, Worker1, Worker2, DataSize),

    fetch_many(Refs, Worker1, notify_fun(), on_complete_fun(CounterPid)),

    timer:sleep(?TIMEOUT),
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
    CounterPid = spawn(?MODULE, onCompleteCounter, [#{ok => 0, errors => 0}]),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt, get_binding(Worker1) | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt, get_binding(Worker2) | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    ?assertMatch({ok, _},
        remote_apply(Worker1, rtransfer, start_link, [RtransferOpts1])
    ),
    ?assertMatch({ok, _},
        remote_apply(Worker2, rtransfer, start_link, [RtransferOpts2])
    ),

    Refs = generate_requests_to_one_file(RequestsNum, Worker1, Worker2, Chunk),

    fetch_many(Refs, Worker1, notify_fun(), on_complete_fun(CounterPid)),

    timer:sleep(?TIMEOUT),
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
    ?assertMatch({ok, _},
        remote_apply(Worker1, rtransfer, start_link, [[get_binding(Worker1) | RtransferOpts2]])
    ),
    ?assertMatch({ok, _},
        remote_apply(Worker2, rtransfer, start_link, [[get_binding(Worker2) | RtransferOpts2]])
    ),

    Ref1 = remote_apply(
        Worker1, rtransfer, prepare_request,
        [Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize]
    ),

    remote_apply(
        Worker1, rtransfer, fetch,
        [Ref1, notify_fun(), on_complete_fun(self())]
    ),

    %% then
    ?assertReceivedMatch({on_complete, {error,{other,{send_error,normal}}}}, ?TIMEOUT).

error_read_fun_test(Config) ->
    %% test with read_fun callback returning an error
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = ?TEST_BLOCK_SIZE,
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {error, ?FILE_HANDLE2, test_error}),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt, get_binding(Worker1) | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt, get_binding(Worker2) | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    ?assertMatch({ok, _},
        remote_apply(Worker1, rtransfer, start_link, [RtransferOpts1])
    ),
    ?assertMatch({ok, _},
        remote_apply(Worker2, rtransfer, start_link, [RtransferOpts2])
    ),

    Ref1 = remote_apply(
        Worker1, rtransfer, prepare_request,
        [Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize]
    ),

    remote_apply(
        Worker1, rtransfer, fetch,
        [Ref1, notify_fun(), on_complete_fun(self())]
    ),

    %% then
    ?assertReceivedMatch({on_complete, {error,{other,{send_error,normal}}}}, ?TIMEOUT).

%% TODO exited with no function clause matching
error_write_fun_test(Config) ->
    %% test with write_fun callback returning an error
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = ?TEST_BLOCK_SIZE,
    Data = generate_binary(DataSize),
    WriteFunOpt = make_opt_fun(write_fun, {error, ?FILE_HANDLE2, test_error}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt, get_binding(Worker1) | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt, get_binding(Worker2) | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    ?assertMatch({ok, _},
        remote_apply(Worker1, rtransfer, start_link, [RtransferOpts1])
    ),
    ?assertMatch({ok, _},
        remote_apply(Worker2, rtransfer, start_link, [RtransferOpts2])
    ),

    Ref1 = remote_apply(
        Worker1, rtransfer, prepare_request,
        [Worker2, ?TEST_FILE_UUID, ?TEST_OFFSET, DataSize]
    ),

    remote_apply(
        Worker1, rtransfer, fetch,
        [Ref1, notify_fun(), on_complete_fun(self())]
    ),

%%     timer:sleep(?TIMEOUT),
    %% then
    ?assertReceivedMatch({on_complete, {error,{other,{send_error,normal}}}}, ?TIMEOUT).

%% TODO this case crashes
bad_formatted_request_test(Config) ->
    %% test calling fetch with bad _request
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = ?TEST_BLOCK_SIZE,
    Data = generate_binary(DataSize),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt, get_binding(Worker1) | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt, get_binding(Worker2) | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    ?assertMatch({ok, _},
        remote_apply(Worker1, rtransfer, start_link, [RtransferOpts1])
    ),
    ?assertMatch({ok, _},
        remote_apply(Worker2, rtransfer, start_link, [RtransferOpts2])
    ),

    remote_apply(Worker1, rtransfer, fetch,
        [{bad_request}, notify_fun(), on_complete_fun(self())]),

    %% then
    %% TODO check what should error message look like
    ?assertReceivedMatch({on_complete, {error, _}}, ?TIMEOUT).

%% TODO this case crashes
bad_file_offset_test(Config) ->
    %% test calling fetch with file offset greater than file size
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    %% given
    DataSize = ?TEST_BLOCK_SIZE,
    %% offset is bigger than DataSize
    Offset = DataSize + 10,
    Data = generate_binary(DataSize),
    WriteFunOpt = make_opt_fun(write_fun, {ok, ?FILE_HANDLE2, DataSize}),
    ReadFunOpt = make_opt_fun(read_fun, {ok, ?FILE_HANDLE2, Data}),
    RtransferOpts1 = [ReadFunOpt, WriteFunOpt, get_binding(Worker1) | ?DEFAULT_RTRANSFER_OPTS],
    RtransferOpts2 = [ReadFunOpt, WriteFunOpt, get_binding(Worker2) | ?DEFAULT_RTRANSFER_OPTS],

    %% when
    ?assertMatch({ok, _},
        remote_apply(Worker1, rtransfer, start_link, [RtransferOpts1])
    ),
    ?assertMatch({ok, _},
        remote_apply(Worker2, rtransfer, start_link, [RtransferOpts2])
    ),

    ?assertMatch({error, _},
        remote_apply(Worker1, rtransfer, fetch,
            [{bad_request}, notify_fun(), on_complete_fun(self())]
        )
    ),

    Ref1 = remote_apply(
        Worker1, rtransfer, prepare_request,
        [Worker2, ?TEST_FILE_UUID, Offset, DataSize]
    ),

    ?assertMatch({error, _},
        remote_apply(Worker1, rtransfer, fetch,
            [Ref1, notify_fun(), on_complete_fun(self())]
        )
    ),

    %% then
    %% TODO check what should error msg look like
    ?assertReceivedMatch({on_complete, {error, _}}, ?TIMEOUT).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_, Config) ->
    NewConfig = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")),
    application:start(ssl2),
    hackney:start(),
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, NewConfig),
    start_applier(Worker1, ?REMOTE_APPLIER, ?COOKIE_OP2),
    start_applier(Worker2, ?REMOTE_APPLIER, ?COOKIE_OP1),
    NewConfig.

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun stop_applier/1, Workers),
    hackney:stop(),
    application:stop(ssl2),
    test_node_starter:clean_environment(Config).
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
    fun(_Handle) ->ok end.

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

get_binding(Node) ->
    Ip = get_node_ip(Node),
    {ok, InetIp} = inet:parse_address(Ip),
    {bind, [InetIp]}.

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
        increase ->
            counter(State + 1);
        {return, Pid} ->
            Pid ! {counter, State}
    end.

stop_counter(CounterPid) ->
    CounterPid ! {return, self()}.

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
        || I <- lists:seq(1, RequestsNum)
    ].

generate_requests_to_one_file(RequestsNum, Worker1, Worker2, Chunk) ->
    [
        remote_apply(Worker1, rtransfer, prepare_request,
            [Worker2, ?TEST_FILE_UUID, Offset, Chunk])
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