%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of db_sync and proxy
%%% @end
%%%-------------------------------------------------------------------
-module(massive_multi_provider_file_ops_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    db_sync_basic_opts_test/1, db_sync_many_ops_test/1, db_sync_distributed_modification_test/1,
    db_sync_many_ops_test_base/1, multi_space_test/1, rtransfer_test/1, rtransfer_test_base/1,
    rtransfer_multisource_test/1, rtransfer_blocking_test/1, traverse_test/1, external_traverse_test/1,
    traverse_cancel_test/1, external_traverse_cancel_test/1, traverse_external_cancel_test/1,
    queued_traverse_cancel_test/1, queued_traverse_external_cancel_test/1, traverse_restart_test/1,
    multiple_traverse_test/1, external_multiple_traverse_test/1, mixed_multiple_traverse_test/1
]).

%% Pool callbacks
-export([do_master_job/2, do_slave_job/2, update_job_progress/5, get_job/1, get_sync_info/1,
    on_cancel_init/1, task_canceled/1, task_finished/1]).

-define(TEST_CASES, [
    db_sync_basic_opts_test, db_sync_many_ops_test, db_sync_distributed_modification_test,
    multi_space_test, rtransfer_test, rtransfer_multisource_test, rtransfer_blocking_test,
    traverse_test, external_traverse_test, traverse_cancel_test, external_traverse_cancel_test,
    traverse_external_cancel_test, queued_traverse_cancel_test, queued_traverse_external_cancel_test,
    traverse_restart_test, multiple_traverse_test, external_multiple_traverse_test, mixed_multiple_traverse_test
]).

-define(PERFORMANCE_TEST_CASES, [
    db_sync_many_ops_test, rtransfer_test
]).

all() ->
    ?ALL(?TEST_CASES, ?PERFORMANCE_TEST_CASES).

-define(MASTER_POOL_NAME, binary_to_atom(<<(atom_to_binary(?MODULE, utf8))/binary, "_master">>, utf8)).
-define(SLAVE_POOL_NAME, binary_to_atom(<<(atom_to_binary(?MODULE, utf8))/binary, "_slave">>, utf8)).

%%%===================================================================
%%% Test functions
%%%===================================================================

-define(db_sync_performance_description(Desc),
    [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, dirs_num}, {value, 2}, {description, "Numbers of directories used during test."}],
            [{name, files_num}, {value, 5}, {description, "Numbers of files used during test."}]
        ]},
        {description, Desc},
        {config, [{name, large_config},
            {parameters, [
                [{name, dirs_num}, {value, 25}],
                [{name, files_num}, {value, 75}]
            ]},
            {description, ""}
        ]}
    ]).

-define(rtransfer_performance_description(Desc),
    [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, small_files_num}, {value, 3},
                {description, "Numbers of small files used during test."}],
            [{name, medium_files_num}, {value, 3},
                {description, "Numbers of medium files used during test."}],
            [{name, big_files_num}, {value, 1},
                {description, "Numbers of big files used during test."}],
            [{name, big_file_parts}, {value, 5},
                {description, "Numbers of parts of big file."}],
            [{name, transfers_num}, {value, 1},
                {description, "Numbers of transfers used during test."}],
            [{name, transfer_file_parts}, {value, 20},
                {description, "Numbers of parts of transfered file."}]
        ]},
        {description, Desc},
        {config, [{name, large_config},
            {parameters, [
                [{name, small_files_num}, {value, 30}],
                [{name, medium_files_num}, {value, 20}],
                [{name, big_files_num}, {value, 3}],
                [{name, big_file_parts}, {value, 10}],
                [{name, transfers_num}, {value, 2}],
                [{name, transfer_file_parts}, {value, 20}]
            ]},
            {description, ""}
        ]},
        {config, [{name, small_config},
            {parameters, [
                [{name, small_files_num}, {value, 50}],
                [{name, medium_files_num}, {value, 0}],
                [{name, big_files_num}, {value, 0}],
                [{name, transfers_num}, {value, 0}]
            ]},
            {description, ""}
        ]},
        {config, [{name, very_small_config},
            {parameters, [
                [{name, small_files_num}, {value, 3}],
                [{name, medium_files_num}, {value, 0}],
                [{name, big_files_num}, {value, 0}],
                [{name, transfers_num}, {value, 0}]
            ]},
            {description, ""}
        ]},
        {config, [{name, default_config},
            {parameters, [
            ]},
            {description, ""}
        ]}
    ]).

rtransfer_multisource_test(Config0) ->
    Config = multi_provider_file_ops_test_base:extend_config(Config0,
        <<"user1">>, {3, 0, 0, 1}, 60),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),
    Workers1 = ?config(workers1, Config),
    [Worker2] = Workers2 = ?config(workers2, Config),
    Workers = ?config(op_worker_nodes, Config),
    [Worker3] = (Workers -- Workers1) -- Workers2,

    lists:foreach(fun(Worker) ->
        test_utils:set_env(Worker, ?APP_NAME, minimal_sync_request, 1)
    end, ?config(op_worker_nodes, Config)),

    File = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,

    ?assertMatch({ok, _}, lfm_proxy:create(Worker2, SessId(Worker2), File, 8#755)),
    OpenAns = lfm_proxy:open(Worker2, SessId(Worker2), {path, File}, rdwr),
    ?assertMatch({ok, _}, OpenAns),
    {ok, Handle} = OpenAns,
    Bytes = <<"1234567890abcde">>,
    Size1 = size(Bytes),
    ?assertEqual({ok, Size1}, lfm_proxy:write(Worker2, Handle, 0, Bytes)),
    ?assertEqual(ok, lfm_proxy:close(Worker2, Handle)),

    ?assertMatch({ok, #file_attr{size = Size1}},
        lfm_proxy:stat(Worker3, SessId(Worker3), {path, File}), 60),
    OpenAns2 = lfm_proxy:open(Worker3, SessId(Worker3), {path, File}, rdwr),
    ?assertMatch({ok, _}, OpenAns2),
    {ok, Handle2} = OpenAns2,
    ?assertEqual({ok, 1}, lfm_proxy:write(Worker3, Handle2, 7, <<"z">>)),
    ?assertEqual({ok, Size1}, lfm_proxy:write(Worker3, Handle2, Size1, Bytes)),
    ?assertEqual(ok, lfm_proxy:close(Worker3, Handle2)),

    Size2 = 2 * size(Bytes),
    ?assertMatch({ok, #file_attr{size = Size2}},
        lfm_proxy:stat(Worker1, SessId(Worker1), {path, File}), 60),
    OpenAns3 = lfm_proxy:open(Worker1, SessId(Worker1), {path, File}, rdwr),
    ?assertMatch({ok, _}, OpenAns3),
    {ok, Handle3} = OpenAns3,

    ?assertMatch({ok, <<"2">>}, lfm_proxy:silent_read(Worker1, Handle3, 1, 1)),
    ?assertMatch({ok, <<"45">>}, lfm_proxy:silent_read(Worker1, Handle3, 3, 2)),
    ?assertMatch({ok, <<"123456">>}, lfm_proxy:silent_read(Worker1, Handle3, 0, 6)),

    ?assertMatch({ok, <<"e">>}, lfm_proxy:silent_read(Worker1, Handle3, 14, 1)),
    ?assertMatch({ok, <<"de1">>}, lfm_proxy:silent_read(Worker1, Handle3, 13, 3)),

    ?assertMatch({ok, <<"9">>}, lfm_proxy:silent_read(Worker1, Handle3, 8, 1)),
    ?assertMatch({ok, <<"7z9">>}, lfm_proxy:silent_read(Worker1, Handle3, 6, 3)),

    ?assertMatch({ok, #file_attr{size = Size2}},
        lfm_proxy:stat(Worker2, SessId(Worker2), {path, File}), 60),
    OpenAns4 = lfm_proxy:open(Worker2, SessId(Worker2), {path, File}, rdwr),
    ?assertMatch({ok, _}, OpenAns4),
    {ok, Handle4} = OpenAns4,
    ?assertMatch({ok, <<"4">>}, lfm_proxy:silent_read(Worker2, Handle4, 18, 1)),
    ?assertEqual(ok, lfm_proxy:close(Worker2, Handle4)),

    timer:sleep(15000),
    ?assertMatch({ok, <<"3">>}, lfm_proxy:silent_read(Worker1, Handle3, 17, 1)),
    ?assertMatch({ok, <<"2345">>}, lfm_proxy:silent_read(Worker1, Handle3, 16, 4)),

    ?assertEqual(ok, lfm_proxy:close(Worker1, Handle3)),
    ok.

db_sync_basic_opts_test(Config) ->
    multi_provider_file_ops_test_base:basic_opts_test_base(Config, <<"user1">>, {3,0,0}, 60).

rtransfer_test(Config) ->
    ?PERFORMANCE(Config, ?rtransfer_performance_description("Tests rtransfer")).
rtransfer_test_base(Config) ->
    SMN = ?config(small_files_num, Config),
    MFN = ?config(medium_files_num, Config),
    BFN = ?config(big_files_num, Config),
    BFP = ?config(big_file_parts, Config),
    TN = ?config(transfers_num, Config),
    TFP = ?config(transfer_file_parts, Config),
    multi_provider_file_ops_test_base:rtransfer_test_base(Config, <<"user1">>,
        {3,0,0}, 180, timer:minutes(5), SMN, MFN, BFN, BFP, TN, TFP).

rtransfer_blocking_test(Config) ->
    multi_provider_file_ops_test_base:rtransfer_blocking_test_base(Config, <<"user1">>,
        {3,0,0}, 180, timer:minutes(5), 30).

db_sync_many_ops_test(Config) ->
    ?PERFORMANCE(Config, ?db_sync_performance_description("Tests working on dirs and files with db_sync")).
db_sync_many_ops_test_base(Config) ->
    DirsNum = ?config(dirs_num, Config),
    FilesNum = ?config(files_num, Config),
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user1">>, {3,0,0}, 60, DirsNum, FilesNum).

db_sync_distributed_modification_test(Config) ->
    multi_provider_file_ops_test_base:distributed_modification_test_base(Config, <<"user1">>, {3,0,0}, 60).

multi_space_test(Config) ->
    User = <<"user1">>,
    Spaces = ?config({spaces, User}, Config),
    Attempts = 120,

    SpaceConfigs = lists:foldl(fun({_, SN}, Acc) ->
        {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider} = case SN of
            <<"space1">> ->
                {3,0,0,1};
            _ ->
                {0,3,1,1}
        end,
        EC = multi_provider_file_ops_test_base:extend_config(Config, User,
            {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts),
        [{SN, EC} | Acc]
    end, [], Spaces),

    multi_provider_file_ops_test_base:multi_space_test_base(Config, SpaceConfigs, User).

traverse_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    traverse_test_base(Config, Worker, <<"traverse_test">>).

external_traverse_test(Config) ->
    [_, _, Worker] = ?config(op_worker_nodes, Config),
    traverse_test_base(Config, Worker, <<"external_traverse_test">>).

traverse_test_base(Config, StartTaskWorker, DirName) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    {SessId, _} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(StartTaskWorker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    Dir1 = <<"/", SpaceName/binary, "/", DirName/binary>>,
    {ok, Guid1} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, Dir1)),
    build_traverse_tree(Worker, SessId, Dir1, 1),
    ?assertMatch({ok, _}, lfm_proxy:resolve_guid(StartTaskWorker, SessId2, Dir1), 30),

    ExecutorID = rpc:call(Worker, oneprovider, get_id_or_undefined, []),
    TestMap = #{<<"key">> => <<"value">>},
    RunOptions = #{
        target_provider_id => ExecutorID,
        batch_size => 1,
        traverse_info => self(),
        additional_data => TestMap
    },
    {ok, TaskID} = ?assertMatch({ok, _}, rpc:call(StartTaskWorker, tree_traverse, run,
        [?MODULE, file_ctx:new_by_guid(Guid1), RunOptions])),

    case StartTaskWorker of
        Worker ->
            ?assertMatch({ok, [TaskID], _},
                rpc:call(StartTaskWorker, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), ongoing]));
        _ ->
            ?assertMatch({ok, [TaskID], _},
                rpc:call(StartTaskWorker, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), scheduled]))
    end,

    Expected = get_expected_jobs(),
    Ans = get_slave_ans(),

    SJobsNum = length(Expected),
    MJobsNum = SJobsNum * 4 div 3 - 1,
    Description = #{
        slave_jobs_delegated => SJobsNum,
        slave_jobs_done => SJobsNum,
        slave_jobs_failed => 0,
        master_jobs_delegated => MJobsNum,
        master_jobs_done => MJobsNum
    },

    ?assertEqual(Expected, lists:sort(Ans)),

    lists:foreach(fun(W) ->
        ?assertMatch({ok, #document{value = #traverse_task{description = Description, enqueued = false,
            canceled = false, status = finished, additional_data = TestMap}}},
            rpc:call(W, tree_traverse, get_task, [?MODULE, TaskID]), 30),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), ongoing]), 30),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), scheduled]), 30),
        check_ended(Worker, [TaskID]),

        case W of
            Worker -> check_callbacks(W, 0, 0, 1);
            _ -> check_callbacks(W, 0, 0, 0)
        end
    end, Workers).

traverse_cancel_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    traverse_cancel_test_base(Config, Worker, Worker, <<"traverse_cancel_test">>).

external_traverse_cancel_test(Config) ->
    [CancelWorker, _, StartTaskWorker] = ?config(op_worker_nodes, Config),
    traverse_cancel_test_base(Config, StartTaskWorker, CancelWorker, <<"external_traverse_cancel_test">>).

traverse_external_cancel_test(Config) ->
    [StartTaskWorker, _, CancelWorker] = ?config(op_worker_nodes, Config),
    traverse_cancel_test_base(Config, StartTaskWorker, CancelWorker, <<"traverse_external_cancel_test">>).

traverse_cancel_test_base(Config, StartTaskWorker, CancelWorker, DirName) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    {SessId, _} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(StartTaskWorker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    Dir1 = <<"/", SpaceName/binary, "/", DirName/binary>>,
    {ok, Guid1} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, Dir1)),
    build_traverse_tree(Worker, SessId, Dir1, 1),
    ?assertMatch({ok, _}, lfm_proxy:resolve_guid(StartTaskWorker, SessId2, Dir1), 30),

    ExecutorID = rpc:call(Worker, oneprovider, get_id_or_undefined, []),
    RunOptions = #{
        target_provider_id => ExecutorID,
        batch_size => 1,
        traverse_info => {self(), cancel, DirName}
    },
    {ok, TaskID} = ?assertMatch({ok, _}, rpc:call(StartTaskWorker, tree_traverse, run,
        [?MODULE, file_ctx:new_by_guid(Guid1), RunOptions])),

    RecAns = receive
        {cancel, Check} when Check =:= DirName ->
            ?assertEqual(ok, rpc:call(CancelWorker, tree_traverse, cancel, [?MODULE, TaskID]), 15)
    after
        30000 ->
            timeout
    end,
    ?assertEqual(ok, RecAns),

    get_slave_ans(),

    lists:foreach(fun(W) ->
        ?assertMatch({ok, #document{value = #traverse_task{enqueued = false,
            canceled = true, status = canceled}}}, rpc:call(W, tree_traverse, get_task, [?MODULE, TaskID]), 30),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), ongoing]), 30),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), scheduled]), 30),
        check_ended(Worker, [TaskID]),

        case W of
            Worker -> check_callbacks(W, 1, 1, 0);
            _ -> check_callbacks(W, 0, 0, 0)
        end
    end, Workers).

queued_traverse_cancel_test(Config) ->
    [CancelWorker, _, _] = ?config(op_worker_nodes, Config),
    queued_traverse_cancel_test_base(Config, CancelWorker, <<"queued_traverse_cancel_test">>).

queued_traverse_external_cancel_test(Config) ->
    [_, _, CancelWorker] = ?config(op_worker_nodes, Config),
    queued_traverse_cancel_test_base(Config, CancelWorker, <<"queued_traverse_external_cancel_test">>).

queued_traverse_cancel_test_base(Config, CancelWorker, DirName) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    {SessId, _} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    Dir1 = <<"/", SpaceName/binary, "/", DirName/binary>>,
    {ok, Guid1} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, Dir1)),
    build_traverse_tree(Worker, SessId, Dir1, 1),
    ?assertMatch({ok, _}, lfm_proxy:resolve_guid(Worker, SessId, Dir1), 30),

    ExecutorID = rpc:call(Worker, oneprovider, get_id_or_undefined, []),
    RunOptions0 = #{
        target_provider_id => ExecutorID,
        batch_size => 1,
        traverse_info => self()
    },
    RunOptions = RunOptions0#{traverse_info => {self(), cancel, DirName}},
    {ok, TaskID0} = ?assertMatch({ok, _}, rpc:call(Worker, tree_traverse, run,
        [?MODULE, file_ctx:new_by_guid(Guid1), RunOptions0])),
    {ok, TaskID} = ?assertMatch({ok, _}, rpc:call(Worker, tree_traverse, run,
        [?MODULE, file_ctx:new_by_guid(Guid1), RunOptions])),

    RecAns = receive
        {cancel, Check} when Check =:= DirName ->
            ?assertEqual(ok, rpc:call(CancelWorker, tree_traverse, cancel, [?MODULE, TaskID]), 15)
    after
        30000 ->
            timeout
    end,
    ?assertEqual(ok, RecAns),

    get_slave_ans(),

    lists:foreach(fun(W) ->
        ?assertMatch({ok, #document{value = #traverse_task{enqueued = false,
            canceled = false, status = finished}}}, rpc:call(W, tree_traverse, get_task, [?MODULE, TaskID0]), 30),
        ?assertMatch({ok, #document{value = #traverse_task{enqueued = false,
            canceled = true, status = canceled}}}, rpc:call(W, tree_traverse, get_task, [?MODULE, TaskID]), 30),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), ongoing]), 30),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), scheduled]), 30),
        check_ended(Worker, [TaskID0, TaskID]),

        case W of
            Worker -> check_callbacks(W, 1, 1, 1);
            _ -> check_callbacks(W, 0, 0, 0)
        end
    end, Workers).

traverse_restart_test(Config) ->
    DirName = <<"traverse_restart_test">>,
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    {SessId, _} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    Dir1 = <<"/", SpaceName/binary, "/", DirName/binary>>,
    {ok, Guid1} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, Dir1)),
    build_traverse_tree(Worker, SessId, Dir1, 1),
    ?assertMatch({ok, _}, lfm_proxy:resolve_guid(Worker, SessId, Dir1), 30),

    ExecutorID = rpc:call(Worker, oneprovider, get_id_or_undefined, []),
    RunOptions = #{
        target_provider_id => ExecutorID,
        batch_size => 1,
        traverse_info => {self(), cancel, DirName}
    },
    RunOptions2 = RunOptions#{traverse_info => self()},
    {ok, TaskID} = ?assertMatch({ok, _}, rpc:call(Worker, tree_traverse, run,
        [?MODULE, file_ctx:new_by_guid(Guid1), RunOptions])),
    {ok, TaskID2} = ?assertMatch({ok, _}, rpc:call(Worker, tree_traverse, run,
        [?MODULE, file_ctx:new_by_guid(Guid1), RunOptions2])),

    RecAns = receive
        {cancel, Check} when Check =:= DirName ->
            ?assertEqual(ok, rpc:call(Worker, worker_pool, stop_sup_pool, [?MASTER_POOL_NAME])),
            ?assertEqual(ok, rpc:call(Worker, worker_pool, stop_sup_pool, [?SLAVE_POOL_NAME]))
    after
        5000 ->
            timeout
    end,
    ?assertEqual(ok, RecAns),
    ?assertEqual(ok, rpc:call(Worker, tree_traverse, init, [?MODULE, 3, 3, 10])),

    get_slave_ans(),

    lists:foreach(fun(W) ->
        ?assertMatch({ok, #document{value = #traverse_task{enqueued = false,
            canceled = false, status = finished}}}, rpc:call(W, tree_traverse, get_task, [?MODULE, TaskID]), 30),
        ?assertMatch({ok, #document{value = #traverse_task{enqueued = false,
            canceled = false, status = finished}}}, rpc:call(W, tree_traverse, get_task, [?MODULE, TaskID2]), 30),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), ongoing]), 30),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), scheduled]), 30),
        check_ended(Worker, [TaskID, TaskID2])
    end, Workers).

multiple_traverse_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    multiple_traverse_test_base(Config, lists:duplicate(10, Worker), <<"multiple_traverse_test">>).

external_multiple_traverse_test(Config) ->
    [_, _, Worker] = ?config(op_worker_nodes, Config),
    multiple_traverse_test_base(Config, lists:duplicate(10, Worker), <<"external_multiple_traverse_test">>).

mixed_multiple_traverse_test(Config) ->
    [Worker, _, Worker2] = ?config(op_worker_nodes, Config),
    Workers = lists:flatten(lists:duplicate(5, [Worker, Worker2])),
    multiple_traverse_test_base(Config, Workers, <<"mixed_multiple_traverse_test">>).

multiple_traverse_test_base(Config, StartTaskWorkers, DirName) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    {SessId, _} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
        ?config({user_id, <<"user1">>}, Config)},
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    Dir1 = <<"/", SpaceName/binary, "/", DirName/binary>>,
    {ok, Guid1} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, Dir1)),
    build_traverse_tree(Worker, SessId, Dir1, 1),

    lists:foreach(fun(StartTaskWorker) ->

        {SessId2, _} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(StartTaskWorker)}}, Config),
            ?config({user_id, <<"user1">>}, Config)},
        ?assertMatch({ok, _}, lfm_proxy:resolve_guid(StartTaskWorker, SessId2, Dir1), 30)
    end, StartTaskWorkers),

    ExecutorID = rpc:call(Worker, oneprovider, get_id_or_undefined, []),
    RunOptions = #{
        target_provider_id => ExecutorID,
        batch_size => 1,
        traverse_info => self()
    },
    TaskIDs = lists:map(fun(StartTaskWorker) ->
        {ok, TaskID} = ?assertMatch({ok, _}, rpc:call(StartTaskWorker, tree_traverse, run,
            [?MODULE, file_ctx:new_by_guid(Guid1), RunOptions])),
        TaskID
    end, StartTaskWorkers),

    Expected = get_expected_jobs(),
    Ans = get_slave_ans(),

    SJobsNum = length(Expected),
    MJobsNum = SJobsNum * 4 div 3 - 1,
    Description = #{
        slave_jobs_delegated => SJobsNum,
        slave_jobs_done => SJobsNum,
        slave_jobs_failed => 0,
        master_jobs_delegated => MJobsNum,
        master_jobs_done => MJobsNum
    },

    Check = lists:flatten(lists:duplicate(length(StartTaskWorkers), Expected)),
    ?assertEqual(lists:sort(Check), lists:sort(Ans)),

    lists:foreach(fun(W) ->
        lists:foreach(fun(TaskID) ->
            ?assertMatch({ok, #document{value = #traverse_task{description = Description, enqueued = false,
                canceled = false, status = finished}}}, rpc:call(W, tree_traverse, get_task, [?MODULE, TaskID]), 30)
        end, TaskIDs),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), ongoing]), 30),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), scheduled]), 30),
        check_ended(Worker, TaskIDs),

        case W of
            Worker -> check_callbacks(W, 0, 0, 10);
            _ -> check_callbacks(W, 0, 0, 0)
        end
    end, Workers).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> multi_provider_file_ops_test_base:init_env(NewConfig) end,
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config).

init_per_testcase(Case, Config) when
    Case =:= traverse_test ; Case =:= external_traverse_test ; Case =:= traverse_cancel_test ;
    Case =:= external_traverse_cancel_test ; Case =:= traverse_external_cancel_test ;
    Case =:= queued_traverse_cancel_test ; Case =:= queued_traverse_external_cancel_test ;
    Case =:= traverse_restart_test ; Case =:= multiple_traverse_test ;
    Case =:= external_multiple_traverse_test ; Case =:= mixed_multiple_traverse_test ->
    Workers = ?config(op_worker_nodes, Config),
    clear_callbacks(Workers),
    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, tree_traverse, init, [?MODULE, 3, 3, 10]))
    end, Workers),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 60}),
    lfm_proxy:init(Config).

end_per_testcase(Case, Config) when
    Case =:= traverse_test ; Case =:= external_traverse_test ; Case =:= traverse_cancel_test ;
    Case =:= external_traverse_cancel_test ; Case =:= traverse_external_cancel_test ;
    Case =:= queued_traverse_cancel_test ; Case =:= queued_traverse_external_cancel_test ;
    Case =:= traverse_restart_test ; Case =:= multiple_traverse_test ;
    Case =:= external_multiple_traverse_test ; Case =:= mixed_multiple_traverse_test ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, traverse, stop_pool, [atom_to_binary(?MODULE, utf8)]))
    end, Workers),
    lfm_proxy:teardown(Config);
end_per_testcase(rtransfer_blocking_test, Config) ->
    multi_provider_file_ops_test_base:rtransfer_blocking_test_cleanup(Config),
    lfm_proxy:teardown(Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_slave_ans() ->
    receive
        {slave, Num} ->
            [Num | get_slave_ans()]
    after
        10000 ->
            []
    end.

build_traverse_tree(Worker, SessId, Dir, Num) ->
    NumBin = integer_to_binary(Num),
    Dirs = case Num < 1000 of
        true -> [10 * Num, 10 * Num + 5];
        _ -> []
    end,
    Files = [Num + 1, Num + 2, Num + 3],

    DirsPaths = lists:map(fun(DirNum) ->
        DirNumBin = integer_to_binary(DirNum),
        NewDir = <<Dir/binary, "/", DirNumBin/binary>>,
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, NewDir)),
        {NewDir, DirNum}
    end, Dirs),

    lists:foreach(fun(FileNum) ->
        FileNumBin = integer_to_binary(FileNum),
        NewFile = <<Dir/binary, "/", FileNumBin/binary>>,
        ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId, NewFile, 8#600))
    end, Files),

    NumBin = integer_to_binary(Num),
    Files ++ lists:flatten(lists:map(fun({D, N}) ->
        build_traverse_tree(Worker, SessId, D, N) end, DirsPaths)).

%%%===================================================================
%%% Pool callbacks
%%%===================================================================

do_master_job(Job, TaskID) ->
    tree_traverse:do_master_job(Job, TaskID).

do_slave_job({#document{value = #file_meta{name = <<"2">>}} = Doc, {Pid, cancel, DirName}}, TaskID) ->
    timer:sleep(500),
    Pid ! {cancel, DirName},
    timer:sleep(15000),
    do_slave_job({Doc, Pid}, TaskID);
do_slave_job({Doc, {Pid, cancel, _DirName}}, TaskID) ->
    do_slave_job({Doc, Pid}, TaskID);
do_slave_job({#document{value = #file_meta{name = Name}}, TraverseInfo}, _TaskID) ->
    TraverseInfo ! {slave, binary_to_integer(Name)},
    ok.

update_job_progress(ID, Job, Pool, TaskID, Status) ->
    tree_traverse:update_job_progress(ID, Job, Pool, TaskID, Status, ?MODULE).

get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

get_sync_info(Job) ->
    tree_traverse:get_sync_info(Job).

on_cancel_init(TaskID) ->
    save_callback(on_cancel_init, TaskID).

task_canceled(TaskID) ->
    save_callback(task_canceled, TaskID),
    timer:sleep(2000),
    ok.

task_finished(TaskID) ->
    save_callback(task_finished, TaskID).

%%%===================================================================
%%% Internal functions
%%%===================================================================

check_ended(Worker, Tasks) ->
    {ok, Ans, _} = ?assertMatch({ok, _, _},
        rpc:call(Worker, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), ended]), 30),
    ?assertEqual([], Tasks -- Ans).

save_callback(Callback, TaskID) ->
    List = application:get_env(?APP_NAME, Callback, []),
    application:set_env(?APP_NAME, Callback, [{TaskID, os:timestamp()} | List]),
    ok.

clear_callbacks(Workers) ->
    lists:foreach(fun(Worker) ->
        lists:foreach(fun(Callback) ->
            rpc:call(Worker, application, set_env, [?APP_NAME, Callback, []])
        end, [on_cancel_init, task_canceled, task_finished])
    end, Workers).

check_callbacks(Worker, OnCancelNum, CancelNum, FinishNum) ->
    Check = lists:zip([OnCancelNum, CancelNum, FinishNum], [on_cancel_init, task_canceled, task_finished]),
    lists:foreach(fun({Num, Callback}) ->
        Ans = rpc:call(Worker, application, get_env, [?APP_NAME, Callback, []]),
        ?assertEqual(Num, length(Ans))
    end, Check),

    Init = rpc:call(Worker, application, get_env, [?APP_NAME, on_cancel_init, []]),
    Cancel = rpc:call(Worker, application, get_env, [?APP_NAME, task_canceled, []]),

    lists:foreach(fun({TaskID, Time}) ->
        Time2 = proplists:get_value(TaskID, Cancel, 0),
        ?assert(timer:now_diff(Time2, Time) >= 2000000)
    end, Init).

get_expected_jobs() ->
    [2,3,4,
        11,12,13,16,17,18,
        101,102,103,106,107,108,
        151,152,153,156,157,158,
        1001,1002,1003,1006,1007,1008,
        1051,1052,1053,1056,1057,1058,
        1501,1502, 1503,1506,1507,1508,
        1551,1552,1553,1556,1557, 1558].