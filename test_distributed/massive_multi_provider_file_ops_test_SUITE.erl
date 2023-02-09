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

-behaviour(traverse_behaviour).

-include("global_definitions.hrl").
-include("tree_traverse.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/dbsync/dbsync.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    db_sync_basic_opts_test/1, db_sync_many_ops_test/1, db_sync_distributed_modification_test/1,
    db_sync_many_ops_test_base/1, multi_space_test/1, rtransfer_test/1, rtransfer_test_base/1,
    rtransfer_multisource_test/1, rtransfer_blocking_test/1, traverse_test/1, external_traverse_test/1,
    traverse_cancel_test/1, external_traverse_cancel_test/1, traverse_external_cancel_test/1,
    queued_traverse_cancel_test/1, queued_traverse_external_cancel_test/1, traverse_restart_test/1,
    multiple_traverse_test/1, external_multiple_traverse_test/1, mixed_multiple_traverse_test/1,
    db_sync_basic_opts_with_errors_test/1, resynchronization_test/1, initial_sync_repeat_test/1,
    range_resynchronization_test/1
]).

%% Pool callbacks
-export([do_master_job/2, do_slave_job/2, update_job_progress/5, get_job/1, get_sync_info/1,
    on_cancel_init/2, task_canceled/2, task_finished/2]).

%% File_meta posthooks used during tests
-export([test_posthook/3, encode_file_meta_posthook_args/2, decode_file_meta_posthook_args/2]).

-define(TEST_CASES, [
    initial_sync_repeat_test, resynchronization_test, range_resynchronization_test,
    db_sync_basic_opts_test, db_sync_many_ops_test,
    db_sync_distributed_modification_test, multi_space_test, rtransfer_test, rtransfer_multisource_test,
    rtransfer_blocking_test, traverse_test, external_traverse_test, traverse_cancel_test, external_traverse_cancel_test,
    traverse_external_cancel_test, queued_traverse_cancel_test, queued_traverse_external_cancel_test,
    traverse_restart_test, multiple_traverse_test, external_multiple_traverse_test, mixed_multiple_traverse_test,
    db_sync_basic_opts_with_errors_test
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

    ?assertMatch({ok, _}, lfm_proxy:create(Worker2, SessId(Worker2), File)),
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

db_sync_basic_opts_with_errors_test(Config) ->
    multi_provider_file_ops_test_base:basic_opts_test_base(Config, <<"user1">>, {3,0,0}, 60, false).

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
        traverse_info => #{pid => self()},
        additional_data => TestMap
    },
    {ok, TaskId} = ?assertMatch({ok, _}, rpc:call(StartTaskWorker, tree_traverse, run,
        [?MODULE, file_ctx:new_by_guid(Guid1), RunOptions])),

    case StartTaskWorker of
        Worker ->
            ?assertMatch({ok, [TaskId], _},
                rpc:call(StartTaskWorker, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), ongoing]));
        _ ->
            ?assertMatch({ok, [TaskId], _},
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
            rpc:call(W, tree_traverse, get_task, [?MODULE, TaskId]), 30),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), ongoing]), 30),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), scheduled]), 30),
        check_ended(Worker, [TaskId]),

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
        traverse_info => #{
            pid => self(),
            cancel => true,
            dir_name => DirName
        }
    },
    {ok, TaskId} = ?assertMatch({ok, _}, rpc:call(StartTaskWorker, tree_traverse, run,
        [?MODULE, file_ctx:new_by_guid(Guid1), RunOptions])),

    RecAns = receive
        {cancel, Check} when Check =:= DirName ->
            ?assertEqual(ok, rpc:call(CancelWorker, tree_traverse, cancel, [?MODULE, TaskId]), 15)
    after
        30000 ->
            timeout
    end,
    ?assertEqual(ok, RecAns),

    get_slave_ans(),

    lists:foreach(fun(W) ->
        verify_finished_task(W, TaskId, true),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), ongoing]), 30),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), scheduled]), 30),
        check_ended(Worker, [TaskId]),

        case W of
            Worker ->
                ?assertEqual(1, length(
                    rpc:call(W, application, get_env, [?APP_NAME, task_canceled, []]) ++
                    rpc:call(W, application, get_env, [?APP_NAME, task_finished, []])
                ));
            _ ->
                check_callbacks(W, 0, 0, 0)
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
    TraverseInfo0 = #{pid => self()},
    RunOptions0 = #{
        target_provider_id => ExecutorID,
        batch_size => 1,
        traverse_info => TraverseInfo0
    },
    RunOptions = RunOptions0#{
        traverse_info => TraverseInfo0#{
            cancel => true,
            dir_name => DirName
    }},
    {ok, TaskId0} = ?assertMatch({ok, _}, rpc:call(Worker, tree_traverse, run,
        [?MODULE, file_ctx:new_by_guid(Guid1), RunOptions0])),
    {ok, TaskId} = ?assertMatch({ok, _}, rpc:call(Worker, tree_traverse, run,
        [?MODULE, file_ctx:new_by_guid(Guid1), RunOptions])),

    RecAns = receive
        {cancel, Check} when Check =:= DirName ->
            ?assertEqual(ok, rpc:call(CancelWorker, tree_traverse, cancel, [?MODULE, TaskId]), 15)
    after
        30000 ->
            timeout
    end,
    ?assertEqual(ok, RecAns),

    get_slave_ans(),

    lists:foreach(fun(W) ->
        ?assertMatch({ok, #document{value = #traverse_task{enqueued = false,
            canceled = false, status = finished}}}, rpc:call(W, tree_traverse, get_task, [?MODULE, TaskId0]), 30),
        verify_finished_task(W, TaskId, true),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), ongoing]), 30),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), scheduled]), 30),
        check_ended(Worker, [TaskId0, TaskId]),

        case W of
            Worker ->
                ?assertEqual(2, length(
                    rpc:call(W, application, get_env, [?APP_NAME, task_canceled, []]) ++
                    rpc:call(W, application, get_env, [?APP_NAME, task_finished, []])
                ));
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
    TraverseInfo = #{
        pid => self(),
        cancel => true,
        dir_name => DirName
    },
    RunOptions = #{
        target_provider_id => ExecutorID,
        batch_size => 1,
        traverse_info => TraverseInfo
    },
    RunOptions2 = RunOptions#{traverse_info => maps:without([cancel, dir_name], TraverseInfo)},
    {ok, TaskId} = ?assertMatch({ok, _}, rpc:call(Worker, tree_traverse, run,
        [?MODULE, file_ctx:new_by_guid(Guid1), RunOptions])),
    {ok, TaskId2} = ?assertMatch({ok, _}, rpc:call(Worker, tree_traverse, run,
        [?MODULE, file_ctx:new_by_guid(Guid1), RunOptions2])),

    % Stop pools
    RecAns = receive
        {cancel, Check} when Check =:= DirName ->
            ?assertEqual(ok, rpc:call(Worker, worker_pool, stop_sup_pool, [?MASTER_POOL_NAME])),
            ?assertEqual(ok, rpc:call(Worker, worker_pool, stop_sup_pool, [?SLAVE_POOL_NAME]))
    after
        5000 ->
            timeout
    end,
    ?assertEqual(ok, RecAns),

    % Restart pools
    PoolName = atom_to_binary(?MODULE, utf8),
    ?assertMatch({ok, _}, rpc:call(Worker, worker_pool, start_sup_pool, [?MASTER_POOL_NAME,
        [{workers, 3}, {queue_type, lifo}]])),
    ?assertMatch({ok, _}, rpc:call(Worker, worker_pool, start_sup_pool, [?SLAVE_POOL_NAME,
        [{workers, 3}, {queue_type, lifo}]])),
    ?assertEqual(ok, rpc:call(Worker, traverse, restart_tasks, [PoolName, #{executor => ExecutorID}, Worker])),

    get_slave_ans(),

    lists:foreach(fun(W) ->
        verify_finished_task(W, TaskId, false),
        verify_finished_task(W, TaskId2, false),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [PoolName, ongoing]), 30),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [PoolName, scheduled]), 30),
        check_ended(Worker, [TaskId, TaskId2])
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
        traverse_info => #{pid => self()}
    },
    TaskIds = lists:map(fun(StartTaskWorker) ->
        {ok, TaskId} = ?assertMatch({ok, _}, rpc:call(StartTaskWorker, tree_traverse, run,
            [?MODULE, file_ctx:new_by_guid(Guid1), RunOptions])),
        TaskId
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
        lists:foreach(fun(TaskId) ->
            ?assertMatch({ok, #document{value = #traverse_task{description = Description, enqueued = false,
                canceled = false, status = finished}}}, rpc:call(W, tree_traverse, get_task, [?MODULE, TaskId]), 30)
        end, TaskIds),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), ongoing]), 30),
        ?assertMatch({ok, [], _}, rpc:call(W, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), scheduled]), 30),
        check_ended(Worker, TaskIds),

        case W of
            Worker -> check_callbacks(W, 0, 0, 10);
            _ -> check_callbacks(W, 0, 0, 0)
        end
    end, Workers).


resynchronization_test(Config) ->
    [Worker1, Worker2, Worker3] = ?config(op_worker_nodes, Config),
    SessId1 = lfm_test_utils:get_user1_session_id(Config, Worker1),
    SessId2 = lfm_test_utils:get_user1_session_id(Config, Worker2),
    SessId3 = lfm_test_utils:get_user1_session_id(Config, Worker3),
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
    Structure = [{3, 3}, {3, 3}],

    test_utils:mock_expect(Worker1, dbsync_changes, apply, fun(_Doc) -> ok end),

    {ok, Worker2Root} = ?assertMatch({ok, _},
        lfm_proxy:mkdir(Worker2, SessId2, SpaceGuid, <<"resynchronization_test_dir2">>, 8#777)),
    {Worker2Dirs, Worker2Files} = lfm_test_utils:create_files_tree(Worker2, SessId2, Structure, Worker2Root),

    {ok, Worker3Root} = ?assertMatch({ok, _},
        lfm_proxy:mkdir(Worker3, SessId3, SpaceGuid, <<"resynchronization_test_dir3">>, 8#777)),
    {Worker3Dirs, Worker3Files} = lfm_test_utils:create_files_tree(Worker3, SessId3, Structure, Worker3Root),

    % Sleep to allow synchronization of documents with mocked apply function
    timer:sleep(timer:seconds(60)),
    ?assertEqual(ok, test_utils:mock_unload(Worker1, dbsync_changes)),

    % Create and allow sync file which parent is not synced
    [Worker2Dir1 | _] = Worker2Dirs,
    {ok, {FileToUnlinkGuid, FileToUnlinkHandle}} = lfm_proxy:create_and_open(
        Worker2, SessId2, Worker2Dir1, <<"file_to_unlink">>, ?DEFAULT_FILE_MODE),
    ok = lfm_proxy:close(Worker2, FileToUnlinkHandle),
    lfm_proxy:unlink(Worker2, SessId2, #file_ref{guid = FileToUnlinkGuid}),
    timer:sleep(timer:seconds(20)),

    lists:foreach(fun(Guid) ->
        ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(Worker1, SessId1, ?FILE_REF(Guid)))
    end, Worker2Dirs ++ Worker2Files ++ Worker3Dirs ++ Worker3Files),

    Provider2Id = rpc:call(Worker2, oneprovider, get_id_or_undefined, []),
    ?assertEqual(ok, rpc:call(Worker1, dbsync_worker, resynchronize_provider_metadata, [SpaceId, Provider2Id])),
    lists:foreach(fun(Guid) ->
        ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId1, ?FILE_REF(Guid)), 60)
    end, Worker2Dirs ++ Worker2Files),
    lists:foreach(fun(Guid) ->
        ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(Worker1, SessId1, ?FILE_REF(Guid)))
    end, Worker3Dirs ++ Worker3Files),

    ?assertEqual(ok, rpc:call(Worker1, dbsync_worker, resynchronize_all, [SpaceId, Provider2Id])),
    lists:foreach(fun(Guid) ->
        ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId1, ?FILE_REF(Guid)), 60)
    end, Worker2Dirs ++ Worker2Files ++ Worker3Dirs ++ Worker3Files),

    ?assertEqual(undefined, rpc:call(Worker1, dbsync_state, get_synchronization_params, [SpaceId, Provider2Id]), 5).


initial_sync_repeat_test(Config) ->
    [Worker1, Worker2, Worker3] = ?config(op_worker_nodes, Config),
    SessId1 = lfm_test_utils:get_user1_session_id(Config, Worker1),
    SessId2 = lfm_test_utils:get_user1_session_id(Config, Worker2),
    SessId3 = lfm_test_utils:get_user1_session_id(Config, Worker3),
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
    Structure = [{3, 3}, {3, 3}],

    Provider2Id = opw_test_rpc:get_provider_id(Worker2),
    test_utils:mock_expect(Worker1, dbsync_utils, is_supported, fun(SpaceId, ProviderIds) ->
        case lists:member(Provider2Id, ProviderIds) of
            true -> false;
            false -> meck:passthrough([SpaceId, ProviderIds])
        end
    end),
    ?assertEqual(continue, rpc:call(Worker1, dbsync_state, set_seq_and_timestamp, [SpaceId, Provider2Id, 1, 0])),

    {ok, Worker2Root} = ?assertMatch({ok, _},
        lfm_proxy:mkdir(Worker2, SessId2, SpaceGuid, <<"initial_sync_repeat_test_dir2">>, 8#777)),
    Worker2RootUuid = file_id:guid_to_uuid(Worker2Root),
    {Worker2Dirs, Worker2Files} = lfm_test_utils:create_files_tree(Worker2, SessId2, Structure, Worker2Root),

    {ok, Worker3Root} = ?assertMatch({ok, _},
        lfm_proxy:mkdir(Worker3, SessId3, SpaceGuid, <<"initial_sync_repeat_test_dir3">>, 8#777)),
    {Worker3Dirs, Worker3Files} = lfm_test_utils:create_files_tree(Worker3, SessId3, Structure, Worker3Root),

    % Sleep to allow synchronization of documents with mocked is_supported function
    timer:sleep(timer:seconds(30)),

    Master = self(),
    test_utils:mock_expect(Worker1, dbsync_changes, apply, fun
        (#document{key = Key, value = #file_meta{}} = Doc) when Key =:= Worker2RootUuid ->
            Master ! {file_synced, Key, self()},
            receive
                proceed -> meck:passthrough([Doc])
            end;
        (Doc) ->
            meck:passthrough([Doc])
    end),
    ?assertEqual(ok, test_utils:mock_unload(Worker1, dbsync_utils)),

    add_posthooks_on_file_sync(Worker1, SpaceId, Provider2Id, initial_sync, [<<"1">>, <<"2">>]),
    add_posthooks_on_file_sync(Worker1, SpaceId, Provider2Id, resynchronization, [<<"3">>]),
    verify_posthooks_called([<<"1">>, <<"3">>]),

    lists:foreach(fun(Guid) ->
        ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId1, ?FILE_REF(Guid)), 60)
    end, Worker2Dirs ++ Worker2Files ++ Worker3Dirs ++ Worker3Files),

    ?assertEqual(undefined, rpc:call(Worker1, dbsync_state, get_synchronization_params, [SpaceId, Provider2Id]), 5).


range_resynchronization_test(Config) ->
    [Worker1, Worker2, _] = ?config(op_worker_nodes, Config),
    SessId1 = lfm_test_utils:get_user1_session_id(Config, Worker1),
    SessId2 = lfm_test_utils:get_user1_session_id(Config, Worker2),
    Provider2Id = rpc:call(Worker2, oneprovider, get_id_or_undefined, []),
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
    Structure = [{3, 3}, {3, 3}],

    test_utils:mock_expect(Worker1, dbsync_changes, apply, fun(_Doc) -> ok end),
    InitialSeq = rpc:call(Worker1, dbsync_state, get_seq, [SpaceId, Provider2Id]),

    {ok, Worker2Root} = ?assertMatch({ok, _},
        lfm_proxy:mkdir(Worker2, SessId2, SpaceGuid, <<"range_resynchronization_test_dir">>, 8#777)),
    {Worker2Dirs, Worker2Files} = lfm_test_utils:create_files_tree(Worker2, SessId2, Structure, Worker2Root),

    % Sleep to allow synchronization of documents with mocked apply function
    timer:sleep(timer:seconds(60)),
    ?assertEqual(ok, test_utils:mock_unload(Worker1, dbsync_changes)),

    lists:foreach(fun(Guid) ->
        ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(Worker1, SessId1, ?FILE_REF(Guid)))
    end, Worker2Dirs ++ Worker2Files),

    FinalSeq = rpc:call(Worker1, dbsync_state, get_seq, [SpaceId, Provider2Id]),
    SeqCount = FinalSeq - InitialSeq,
    ResyncRanges = [
        {InitialSeq, InitialSeq + SeqCount div 3},
        {InitialSeq + SeqCount div 3, InitialSeq + 2 * SeqCount div 3},
        {InitialSeq + 2 * SeqCount div 3, FinalSeq}
    ],
    lists:foreach(fun({StartSeq, TargetSeq}) ->
        ?assertEqual(ok, rpc:call(Worker1, dbsync_worker, resynchronize,
            [SpaceId, Provider2Id, ?ALL_MUTATORS_EXCEPT_SENDER, StartSeq, TargetSeq])),
        ?assertEqual(FinalSeq, rpc:call(Worker1, dbsync_state, get_seq, [SpaceId, Provider2Id]), 60)
    end, ResyncRanges),

    lists:foreach(fun(Guid) ->
        ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId1, ?FILE_REF(Guid)), 60)
    end, Worker2Dirs ++ Worker2Files),

    ?assertEqual(undefined, rpc:call(Worker1, dbsync_state, get_synchronization_params, [SpaceId, Provider2Id]), 5).


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
init_per_testcase(db_sync_basic_opts_with_errors_test = Case, Config) ->
    MockedConfig = multi_provider_file_ops_test_base:mock_sync_and_rtransfer_errors(Config),
    init_per_testcase(?DEFAULT_CASE(Case), MockedConfig);
init_per_testcase(Case, Config) when
    Case =:= resynchronization_test ; Case =:= range_resynchronization_test
->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [dbsync_changes]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(initial_sync_repeat_test = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, [dbsync_changes, dbsync_utils, qos_logic]),
    % Prevent changing of dbsync synchronization mode by QoS hooks
    test_utils:mock_expect(Worker, qos_logic, handle_qos_entry_change, fun(_, _) ->
        ok
    end),
    test_utils:mock_expect(Worker, qos_logic, invalidate_cache_and_reconcile, fun(_) ->
        ok
    end),
    test_utils:mock_expect(Worker, qos_logic, missing_file_meta_posthook, fun(_, _) ->
        ok
    end),
    test_utils:mock_expect(Worker, qos_logic, missing_link_posthook, fun(_, _, _) ->
        ok
    end),
    test_utils:mock_expect(Worker, qos_logic, reconcile_qos, fun(_) ->
        ok
    end),
    test_utils:set_env(Worker, ?APP_NAME, max_file_meta_posthooks, 1),
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
        ?assertEqual(ok, rpc:call(Worker, tree_traverse, stop, [?MODULE]))
    end, Workers),
    lfm_proxy:teardown(Config);
end_per_testcase(rtransfer_blocking_test, Config) ->
    multi_provider_file_ops_test_base:rtransfer_blocking_test_cleanup(Config),
    lfm_proxy:teardown(Config);
end_per_testcase(db_sync_basic_opts_with_errors_test = Case, Config) ->
    multi_provider_file_ops_test_base:unmock_sync_and_rtransfer_errors(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(Case, Config) when
    Case =:= resynchronization_test ; Case =:= range_resynchronization_test
->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker, [dbsync_changes]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(initial_sync_repeat_test = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker, [dbsync_changes, dbsync_utils, qos_logic]),
    ?assertEqual(ok, rpc:call(Worker, application, unset_env, [?APP_NAME, max_file_meta_posthooks])),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
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
        ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId, NewFile))
    end, Files),

    NumBin = integer_to_binary(Num),
    Files ++ lists:flatten(lists:map(fun({D, N}) ->
        build_traverse_tree(Worker, SessId, D, N) end, DirsPaths)).

%%%===================================================================
%%% Pool callbacks
%%%===================================================================

do_master_job(Job, TaskId) ->
    tree_traverse:do_master_job(Job, TaskId).

do_slave_job(Job = #tree_traverse_slave{
    traverse_info = #{
        pid := Pid,
        cancel := true,
        dir_name := DirName
    },
    file_ctx = FileCtx
}, TaskId) ->
    {#document{value = #file_meta{name = Name}}, _} = file_ctx:get_file_doc(FileCtx),
    case Name of
        <<"2">> ->
            timer:sleep(500),
            Pid ! {cancel, DirName},
            timer:sleep(30000);
        _ -> ok
    end,
    do_slave_job(Job#tree_traverse_slave{traverse_info = #{pid => Pid}}, TaskId);
do_slave_job(#tree_traverse_slave{
    traverse_info = #{pid := Pid},
    file_ctx = FileCtx
}, _TaskId) ->
    {#document{value = #file_meta{name = Name}}, _} = file_ctx:get_file_doc(FileCtx),
    Pid ! {slave, binary_to_integer(Name)},
    ok.

update_job_progress(ID, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(ID, Job, Pool, TaskId, Status, ?MODULE).

get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

get_sync_info(Job) ->
    tree_traverse:get_sync_info(Job).

on_cancel_init(TaskId, _PoolName) ->
    save_callback(on_cancel_init, TaskId).

task_canceled(TaskId, _PoolName) ->
    save_callback(task_canceled, TaskId),
    timer:sleep(2000),
    ok.

task_finished(TaskId, _PoolName) ->
    save_callback(task_finished, TaskId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

check_ended(Worker, Tasks) ->
    {ok, Ans, _} = ?assertMatch({ok, _, _},
        rpc:call(Worker, traverse_task_list, list, [atom_to_binary(?MODULE, utf8), ended]), 30),
    ?assertEqual([], Tasks -- Ans).

save_callback(Callback, TaskId) ->
    critical_section:run(save_callback, fun() ->
        List = op_worker:get_env(Callback, []),
        op_worker:set_env(Callback, [{TaskId, stopwatch:start()} | List])
    end),
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

    % the stopwatches are started and saved when a callback is executed
    lists:foreach(fun({TaskId, InitStopwatch}) ->
        CancelStopwatch = proplists:get_value(TaskId, Cancel),
        ?assert(stopwatch:read_millis(InitStopwatch) - stopwatch:read_millis(CancelStopwatch) >= timer:seconds(2))
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


add_posthooks_on_file_sync(Worker, SpaceId, ProviderId, ExpectedSyncMode, Posthooks) ->
    Master = self(),
    CheckAns = receive
        {file_synced, Uuid, DbsyncProc} ->
            ?assertMatch(#synchronization_params{
                mode = ExpectedSyncMode,
                included_mutators = ?ALL_MUTATORS_EXCEPT_SENDER
            }, rpc:call(Worker, dbsync_state, get_synchronization_params, [SpaceId, ProviderId])),
            lists:foreach(fun(PosthookName) ->
                ?assertEqual(ok, rpc:call(Worker, file_meta_posthooks, add_hook,
                    [{file_meta_missing, Uuid}, PosthookName, ?MODULE, test_posthook, [Master, Uuid, PosthookName]]
                ))
            end, Posthooks),    
            DbsyncProc ! proceed,
            ok
    after
        timer:seconds(30) ->
            timeout
    end,
    ?assertEqual(ok, CheckAns).


test_posthook(Master, Uuid, PosthookName) ->
    case file_meta:exists(Uuid) of
        true ->
            Master ! {posthook_executed, PosthookName},
            ok;
        false ->
            {error, not_found}
    end.


encode_file_meta_posthook_args(_, Args) ->
    term_to_binary(Args).


decode_file_meta_posthook_args(_, EncodedArgs) ->
    binary_to_term(EncodedArgs).


verify_posthooks_called(ExpectedPosthooks) ->
    receive
        {posthook_executed, PosthookName} ->
            ?assert(lists:member(PosthookName, ExpectedPosthooks)),
            verify_posthooks_called(ExpectedPosthooks -- [PosthookName])
    after
        timer:seconds(5) ->
            ?assertEqual([], ExpectedPosthooks)
    end.


verify_finished_task(Worker, TaskId, IsCanceled) ->
    CheckTask = fun() ->
        try
            {ok, #document{value = #traverse_task{enqueued = Enqueued, canceled = Canceled, status = Status}}} =
                rpc:call(Worker, tree_traverse, get_task, [?MODULE, TaskId]),
            % Status can be finished for canceled task if task was finished before cancel was processed
            % (possible for remote cancellations).
            % Status can be canceled even if job was not expected to be canceled in case of restart.
            IsFinished = case (Status =:= canceled) orelse (Status =:= finished) of
                true -> true;
                false -> {false, Status}
            end,
            {Enqueued, Canceled, IsFinished}
        catch
            Error:Reason ->
                {Error, Reason}
        end
    end,
    ?assertEqual({false, IsCanceled, true}, CheckTask(), 30).