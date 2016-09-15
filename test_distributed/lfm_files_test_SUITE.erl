%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of logical_file_manager API.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_files_test_SUITE).
-author("Rafal Slota").

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    fslogic_new_file_test/1,
    lfm_create_and_unlink_test/1,
    lfm_create_and_access_test/1,
    lfm_write_test/1,
    lfm_stat_test/1,
    lfm_synch_stat_test/1,
    lfm_truncate_test/1,
    lfm_acl_test/1,
    rm_recursive_test/1,
    file_gap_test/1,
    ls_test/1, ls_test_base/1,
    ls_with_stats_test/1, ls_with_stats_test_base/1
]).

-define(TEST_CASES, [
    fslogic_new_file_test,
    lfm_create_and_unlink_test,
    lfm_create_and_access_test,
    lfm_write_test,
    lfm_stat_test,
    lfm_synch_stat_test,
    lfm_truncate_test,
    lfm_acl_test,
    rm_recursive_test,
    file_gap_test,
    ls_test,
    ls_with_stats_test
]).

-define(PERFORMANCE_TEST_CASES, [
    ls_test, ls_with_stats_test
]).

all() ->
    ?ALL(?TEST_CASES, ?PERFORMANCE_TEST_CASES).

-define(TIMEOUT, timer:seconds(10)).
-define(REPEATS, 5).
-define(SUCCESS_RATE, 100).

-define(req(W, SessId, FuseRequest), rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}])).

-define(file_req(W, SessId, ContextGuid, FileRequest), ?req(W, SessId,
    #file_request{context_guid = ContextGuid, file_request = FileRequest})).

-define(lfm_req(W, Method, Args), rpc:call(W, file_manager, Method, Args, ?TIMEOUT)).

%%%====================================================================
%%% Test function
%%%====================================================================

ls_with_stats_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, ?SUCCESS_RATE},
        {parameters, [
            [{name, proc_num}, {value, 1}, {description, "Number of threads used during the test."}],
            [{name, dir_level}, {value, 10}, {description, "Level of test directory."}],
            [{name, dirs_num_per_proc}, {value, 10}, {description, "Number of dirs tested by single thread."}]
        ]},
        {description, "Tests performance of ls with gettin stats operation"},
        {config, [{name, low_level_single_thread_small_dir},
            {parameters, [
                [{name, dir_level}, {value, 1}],
                [{name, dirs_num_per_proc}, {value, 5}]
            ]},
            {description, ""}
        ]},
        {config, [{name, low_level_single_thread_large_dir},
            {parameters, [
                [{name, dir_level}, {value, 1}],
                [{name, dirs_num_per_proc}, {value, 100}]
            ]},
            {description, ""}
        ]},
        {config, [{name, low_level_10_threads_large_dir},
            {parameters, [
                [{name, proc_num}, {value, 10}],
                [{name, dir_level}, {value, 1}],
                [{name, dirs_num_per_proc}, {value, 10}]
            ]},
            {description, ""}
        ]},
%%        {config, [{name, low_level_many_threads_large_dir},
%%            {parameters, [
%%                [{name, proc_num}, {value, 100}],
%%                [{name, dir_level}, {value, 1}],
%%                [{name, dirs_num_per_proc}, {value, 1}]
%%            ]},
%%            {description, ""}
%%        ]},
        {config, [{name, high_level_single_thread_small_dir},
            {parameters, [
                [{name, dir_level}, {value, 100}],
                [{name, dirs_num_per_proc}, {value, 5}]
            ]},
            {description, ""}
        ]},
        {config, [{name, high_level_single_thread_large_dir},
            {parameters, [
                [{name, dir_level}, {value, 100}],
                [{name, dirs_num_per_proc}, {value, 100}]
            ]},
            {description, ""}
        ]},
        {config, [{name, high_level_10_threads_large_dir},
            {parameters, [
                [{name, proc_num}, {value, 10}],
                [{name, dir_level}, {value, 100}],
                [{name, dirs_num_per_proc}, {value, 10}]
            ]},
            {description, ""}
        ]}
%%        {config, [{name, high_level_many_threads_large_dir},
%%            {parameters, [
%%                [{name, proc_num}, {value, 100}],
%%                [{name, dir_level}, {value, 100}],
%%                [{name, dirs_num_per_proc}, {value, 1}]
%%            ]},
%%            {description, ""}
%%        ]}
    ]).
ls_with_stats_test_base(Config) ->
    % Get test and environment description
    DirLevel = ?config(dir_level, Config),
    ProcNum = ?config(proc_num, Config),
    DirsNumPerProc = ?config(dirs_num_per_proc, Config),

    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    Master = self(),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    % Generate names of dirs in test directory tree
    [LastTreeDir | _] = TreeDirsReversed = lists:foldl(fun(_, [H | _] = Acc) ->
        NewDir = <<H/binary, "/", (generator:gen_name())/binary>>,
        [NewDir | Acc]
    end, [<<"/space_name1">>], lists:seq(1,DirLevel)),
    [_ | TreeDirs] = lists:reverse(TreeDirsReversed),

    % Create dirs tree
    {CreateTreeTime, _} = measure_execution_time(fun() ->
        lists:foreach(fun(D) ->
            ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId1, D, 8#755))
        end, TreeDirs)
    end),

    % Create dirs at last level of tree (to be listed)
    {CreateDirsTime, _} = measure_execution_time(fun() ->
        Fun = fun() ->
            lists:foreach(fun(_) ->
                D = <<LastTreeDir/binary, "/", (generator:gen_name())/binary>>,
                ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId1, D, 8#755))
            end, lists:seq(1,DirsNumPerProc))
        end,
        case ProcNum of
            1 ->
                [Fun()];
            _ ->
                lists:foreach(fun(_) ->
                    spawn(fun() ->
                        Fun(),
                        report_success(Master)
                    end)
                end, lists:seq(1,ProcNum)),
                check_run_parallel_ans(ProcNum)
        end
    end),

    % List directory
    {LsTime, LSDirs} = measure_execution_time(fun() ->
        LSAns = lfm_proxy:ls(Worker, SessId1, {path, LastTreeDir}, 0, DirsNumPerProc*ProcNum),
        ?assertMatch({ok, _}, LSAns),
        {ok, ListedDirs} = LSAns,
        ?assertEqual(DirsNumPerProc*ProcNum, length(ListedDirs)),
        ListedDirs
    end),

    % Stat listed directories
    {StatTime, _} = measure_execution_time(fun() ->
        Fun = fun(Dirs) ->
            lists:foreach(fun({D, _}) ->
                StatAns = lfm_proxy:stat(Worker, SessId1,  {guid, D}),
                ?assertMatch({ok, #file_attr{}}, StatAns)
            end, Dirs)
        end,
        case ProcNum of
            1 ->
                Fun(LSDirs);
            _ ->
                {Dirs, _} = lists:foldl(fun(D, {[H | T] = Acc, Count}) ->
                    case Count =< DirsNumPerProc of
                        true ->
                            {[[D | H] | T], Count + 1};
                        _ ->
                            {[[D] | Acc], 1}
                    end
                end, {[[]], 0}, LSDirs),

                lists:foreach(fun(ProcDirs) ->
                    spawn(fun() ->
                        Fun(ProcDirs),
                        Master ! run_parallel_ok
                          end)
                end, Dirs),
                check_run_parallel_ans(ProcNum)
        end
    end),

    LsWithStatTime = LsTime + StatTime,

    [
        #parameter{name = create_tree_time, value = CreateTreeTime, unit = "us",
            description = "Time of test tree creation"},
        #parameter{name = create_dirs_time, value = CreateDirsTime, unit = "us",
            description = "Time of test dirs creation"},
        #parameter{name = ls_time, value = LsTime, unit = "us",
            description = "Time of ls operation"},
        #parameter{name = stat_time, value = StatTime, unit = "us",
            description = "Time of all stat operations"},
        #parameter{name = ls_stat_time, value = LsWithStatTime, unit = "us",
            description = "Total time of ls and all stat operations"}
    ].

ls_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, ?SUCCESS_RATE},
        {parameters, [
            [{name, dir_size_multiplier}, {value, 1}, {description, "Parametr for dir size tunning."}]
        ]},
        {description, "Tests ls operation"},
        {config, [{name, medium_dir},
            {parameters, [
                [{name, dir_size_multiplier}, {value, 1}]
            ]},
            {description, ""}
        ]},
        {config, [{name, large_dir},
            {parameters, [
                [{name, dir_size_multiplier}, {value, 10}]
            ]},
            {description, ""}
        ]}
    ]).
ls_test_base(Config) ->
    DSM = ?config(dir_size_multiplier, Config),

    [Worker | _] = ?config(op_worker_nodes, Config),
    {SessId1, _UserId1} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    MainDir = generator:gen_name(),
    MainDirPath = <<"/space_name1/", MainDir/binary, "/">>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId1, MainDirPath, 8#755)),

    VerifyLS = fun(Offset0, Limit0, ElementsList) ->
        Offset = Offset0 * DSM,
        Limit = Limit0 * DSM,
        LSAns = lfm_proxy:ls(Worker, SessId1, {path, MainDirPath}, Offset, Limit),
        LSAns2 = lfm_proxy:ls(Worker, SessId1, {path, MainDirPath}, 0, Offset),
        LSAns3 = lfm_proxy:ls(Worker, SessId1, {path, MainDirPath}, Offset + Limit, length(ElementsList)),
        ?assertMatch({ok, _}, LSAns),
        ?assertMatch({ok, _}, LSAns2),
        ?assertMatch({ok, _}, LSAns3),
        {_, ListedElements} = LSAns,
        {_, ListedElements2} = LSAns2,
        {_, ListedElements3} = LSAns3,

        ?assertEqual({min(Limit, max(length(ElementsList) - Offset, 0)), min(Offset, length(ElementsList)),
            max(length(ElementsList) - Offset - Limit, 0)},
            {length(ListedElements), length(ListedElements2), length(ListedElements3)}),
        ?assertEqual(ElementsList,
            lists:sort(lists:map(fun({_, Name}) -> Name  end, ListedElements ++ ListedElements2 ++ ListedElements3)))
    end,

    Files = lists:sort(lists:map(fun(_) ->
        generator:gen_name() end, lists:seq(1, 30*DSM))),
    lists:foreach(fun(F) ->
        ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId1, <<MainDirPath/binary, F/binary>>, 8#755))
    end, Files),

    VerifyLS(0,30, Files),
    VerifyLS(0,4, Files),
    VerifyLS(0,15, Files),
    VerifyLS(0,23, Files),
    VerifyLS(12,11, Files),
    VerifyLS(20,3, Files),
    VerifyLS(22,8, Files),
    VerifyLS(0,40, Files),
    VerifyLS(30,10, Files),
    VerifyLS(35,5, Files),

    Dirs = lists:map(fun(_) ->
        generator:gen_name() end, lists:seq(1, 30*DSM)),
    lists:foreach(fun(D) ->
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId1, <<MainDirPath/binary, D/binary>>, 8#755))
    end, Dirs),
    FandD = lists:sort(Files ++ Dirs),

    VerifyLS(0,60, FandD),
    VerifyLS(0,23, FandD),
    VerifyLS(12,11, FandD),
    VerifyLS(20,3, FandD),
    VerifyLS(22,8, FandD),
    VerifyLS(22,23, FandD),
    VerifyLS(45,5, FandD),
    VerifyLS(45,15, FandD),
    VerifyLS(10,35, FandD),

    {FinalLSTime, _} = measure_execution_time(fun() ->
        VerifyLS(0,80, FandD)
    end),

    #parameter{name = final_ls_time, value = FinalLSTime, unit = "us",
        description = "Time of last full dir listing"}.


fslogic_new_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, <<"user2">>}, Config)},

    RootUUID1 = get_uuid_privileged(Worker, SessId1, <<"/space_name1">>),
    RootUUID2 = get_uuid_privileged(Worker, SessId2, <<"/space_name2">>),

    Resp11 = ?file_req(Worker, SessId1, RootUUID1, #get_new_file_location{name = <<"test">>, create_handle = false}),
    Resp21 = ?file_req(Worker, SessId2, RootUUID2, #get_new_file_location{name = <<"test">>, create_handle = false}),

    ?assertMatch(#fuse_response{status = #status{code = ?OK}, fuse_response = #file_location{}}, Resp11),
    ?assertMatch(#fuse_response{status = #status{code = ?OK}, fuse_response = #file_location{}}, Resp21),

    #fuse_response{fuse_response = #file_location{
        file_id = FileId11,
        storage_id = StorageId11,
        provider_id = ProviderId11}} = Resp11,

    #fuse_response{fuse_response = #file_location{
        file_id = FileId21,
        storage_id = StorageId21,
        provider_id = ProviderId21}} = Resp21,

    ?assertNotMatch(undefined, FileId11),
    ?assertNotMatch(undefined, FileId21),

    TestStorageId = ?config({storage_id, ?GET_DOMAIN(Worker)}, Config),
    ?assertMatch(TestStorageId, StorageId11),
    ?assertMatch(TestStorageId, StorageId21),

    TestProviderId = rpc:call(Worker, oneprovider, get_provider_id, []),
    ?assertMatch(TestProviderId, ProviderId11),
    ?assertMatch(TestProviderId, ProviderId21).

lfm_create_and_access_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user2">>}, Config)},

    FilePath1 = <<"/space_name3/", (generator:gen_name())/binary>>,
    FilePath2 = <<"/space_name3/", (generator:gen_name())/binary>>,
    FilePath3 = <<"/space_name3/", (generator:gen_name())/binary>>,
    FilePath4 = <<"/space_name3/", (generator:gen_name())/binary>>,

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath1, 8#240)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath2, 8#640)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath3, 8#670)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath4, 8#540)),

    %% File #1
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath1}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath1}, read)),
    ?assertMatch(ok,      lfm_proxy:truncate(W, SessId1, {path, FilePath1}, 10)),

    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId1, {path, FilePath1}, read)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath1}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId1, {path, FilePath1}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath1}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(W, SessId2, {path, FilePath1}, 10)),

    %% File #2
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath2}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath2}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath2}, rdwr)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath2}, read)),
    ?assertMatch(ok,      lfm_proxy:truncate(W, SessId1, {path, FilePath2}, 10)),

    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath2}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath2}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(W, SessId2, {path, FilePath2}, 10)),

    %% File #3
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath3}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath3}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath3}, rdwr)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath3}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath3}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath3}, rdwr)),
    ?assertMatch(ok,      lfm_proxy:truncate(W, SessId1, {path, FilePath3}, 10)),
    ?assertMatch(ok,      lfm_proxy:truncate(W, SessId1, {path, FilePath3}, 10)),

    %% File #4
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {path, FilePath4}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {path, FilePath4}, read)),

    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId1, {path, FilePath4}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId1, {path, FilePath4}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath4}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId2, {path, FilePath4}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(W, SessId1, {path, FilePath4}, 10)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(W, SessId2, {path, FilePath4}, 10)).

lfm_create_and_unlink_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user2">>}, Config)},

    FilePath11 = <<"/space_name2/", (generator:gen_name())/binary>>,
    FilePath12 = <<"/space_name2/", (generator:gen_name())/binary>>,
    FilePath21 = <<"/space_name2/", (generator:gen_name())/binary>>,
    FilePath22 = <<"/space_name2/", (generator:gen_name())/binary>>,

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath11, 8#755)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath12, 8#755)),
    ?assertMatch({error, ?EEXIST}, lfm_proxy:create(W, SessId1, FilePath11, 8#755)),

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, FilePath21, 8#755)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, FilePath22, 8#755)),
    ?assertMatch({error, ?EEXIST}, lfm_proxy:create(W, SessId2, FilePath21, 8#755)),

    ?assertMatch(ok, lfm_proxy:unlink(W, SessId1, {path, FilePath11})),
    ?assertMatch(ok, lfm_proxy:unlink(W, SessId2, {path, FilePath21})),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:unlink(W, SessId1, {path, FilePath11})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:unlink(W, SessId2, {path, FilePath21})),

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, FilePath11, 8#755)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, FilePath21, 8#755)).

lfm_write_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},
    {SessId2, _UserId2} = {?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user2">>}, Config)},

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/space_name1/test3">>, 8#755)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/space_name1/test4">>, 8#755)),

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/space_name2/test3">>, 8#755)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/space_name2/test4">>, 8#755)),

    O11 = lfm_proxy:open(W, SessId1, {path, <<"/space_name1/test3">>}, rdwr),
    O12 = lfm_proxy:open(W, SessId1, {path, <<"/space_name1/test4">>}, rdwr),

    ?assertMatch({ok, _}, O11),
    ?assertMatch({ok, _}, O12),

    {ok, Handle11} = O11,
    {ok, Handle12} = O12,

    WriteAndTest =
        fun(Worker, Handle, Offset, Bytes) ->
            Size = size(Bytes),
            ?assertMatch({ok, Size}, lfm_proxy:write(Worker, Handle, Offset, Bytes)),
            for(Offset, Offset + Size - 1,
                fun(I) ->
                    for(1, Offset + Size - I,
                        fun(J) ->
                            SubBytes = binary:part(Bytes, I - Offset, J),
                            ?assertMatch({ok, SubBytes}, lfm_proxy:read(Worker, Handle, I, J))
                        end)
                end)
        end,

    WriteAndTest(W, Handle11, 0, <<"abc">>),
    WriteAndTest(W, Handle12, 0, <<"abc">>),

    WriteAndTest(W, Handle11, 3, <<"def">>),
    WriteAndTest(W, Handle12, 3, <<"def">>),

    WriteAndTest(W, Handle11, 2, <<"qwerty">>),
    WriteAndTest(W, Handle12, 2, <<"qwerty">>),

    WriteAndTest(W, Handle11, 8, <<"zxcvbnm">>),
    WriteAndTest(W, Handle12, 8, <<"zxcvbnm">>),

    WriteAndTest(W, Handle11, 6, <<"qwerty">>),
    WriteAndTest(W, Handle12, 6, <<"qwerty">>),

    WriteAndTest(W, Handle11, 10, crypto:rand_bytes(40)).

lfm_stat_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/space_name2/test5">>, 8#755)),

    O11 = lfm_proxy:open(W, SessId1, {path, <<"/space_name2/test5">>}, rdwr),

    ?assertMatch({ok, _}, O11),
    {ok, Handle11} = O11,

    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test5">>})),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 0, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 3}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test5">>}), 10),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 3, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 6}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test5">>}), 10),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 2, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 6}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test5">>}), 10),

    ?assertMatch({ok, 9}, lfm_proxy:write(W, Handle11, 1, <<"123456789">>)),
    ?assertMatch({ok, #file_attr{size = 10}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test5">>}), 10).

lfm_synch_stat_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/space_name2/test6">>, 8#755)),

    O11 = lfm_proxy:open(W, SessId1, {path, <<"/space_name2/test6">>}, rdwr),

    ?assertMatch({ok, _}, O11),
    {ok, Handle11} = O11,

    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test6">>})),

    ?assertMatch({ok, 3, {ok, #file_attr{size = 3}}}, lfm_proxy:write_and_check(W, Handle11, 0, <<"abc">>)),

    ?assertMatch({ok, 3, {ok, #file_attr{size = 6}}}, lfm_proxy:write_and_check(W, Handle11, 3, <<"abc">>)),

    ?assertMatch({ok, 3, {ok, #file_attr{size = 6}}}, lfm_proxy:write_and_check(W, Handle11, 2, <<"abc">>)),

    ?assertMatch({ok, 9, {ok, #file_attr{size = 10}}}, lfm_proxy:write_and_check(W, Handle11, 1, <<"123456789">>)).

lfm_truncate_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    {SessId1, _UserId1} = {?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config), ?config({user_id, <<"user1">>}, Config)},

    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/space_name2/test7">>, 8#755)),

    O11 = lfm_proxy:open(W, SessId1, {path, <<"/space_name2/test7">>}, rdwr),

    ?assertMatch({ok, _}, O11),
    {ok, Handle11} = O11,

    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test7">>})),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 0, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 3}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test7">>}), 10),

    ?assertMatch(ok, lfm_proxy:truncate(W, SessId1, {path, <<"/space_name2/test7">>}, 1)),
    ?assertMatch({ok, #file_attr{size = 1}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test7">>}), 10),
    ?assertMatch({ok, <<"a">>}, lfm_proxy:read(W, Handle11, 0, 10)),

    ?assertMatch(ok, lfm_proxy:truncate(W, SessId1, {path, <<"/space_name2/test7">>}, 10)),
    ?assertMatch({ok, #file_attr{size = 10}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test7">>}), 10),
    ?assertMatch({ok, <<"a">>}, lfm_proxy:read(W, Handle11, 0, 1)),

    ?assertMatch({ok, 3}, lfm_proxy:write(W, Handle11, 1, <<"abc">>)),
    ?assertMatch({ok, #file_attr{size = 10}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test7">>}), 10),

    ?assertMatch(ok, lfm_proxy:truncate(W, SessId1, {path, <<"/space_name2/test7">>}, 5)),
    ?assertMatch({ok, #file_attr{size = 5}}, lfm_proxy:stat(W, SessId1, {path, <<"/space_name2/test7">>}), 10),
    ?assertMatch({ok, <<"aabc">>}, lfm_proxy:read(W, Handle11, 0, 4)).

lfm_acl_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    UserId1 = ?config({user_id, <<"user1">>}, Config),
    [{GroupId1, _GroupName1} | _] = ?config({groups, <<"user1">>}, Config),
    FileName = <<"/space_name2/test_file_acl">>,
    DirName = <<"/space_name2/test_dir_acl">>,

    {ok, FileGUID} = lfm_proxy:create(W, SessId1, FileName, 8#755),
    {ok, _} = lfm_proxy:mkdir(W, SessId1, DirName),

    % test setting and getting acl
    Acl = [
        #accesscontrolentity{acetype = ?allow_mask, identifier = UserId1, aceflags = ?no_flags_mask, acemask = ?read_mask bor ?write_mask},
        #accesscontrolentity{acetype = ?deny_mask, identifier = GroupId1, aceflags = ?identifier_group_mask, acemask = ?write_mask}
    ],
    Ans1 = lfm_proxy:set_acl(W, SessId1, {guid, FileGUID}, Acl),
    ?assertEqual(ok, Ans1),
    Ans2 = lfm_proxy:get_acl(W, SessId1, {guid, FileGUID}),
    ?assertEqual({ok, Acl}, Ans2).

rm_recursive_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    DirA =  <<"/space_name1/a">>,
    DirB =    <<"/space_name1/a/b">>,
    DirC =    <<"/space_name1/a/c">>,
    FileG =     <<"/space_name1/a/c/g">>,
    FileH =     <<"/space_name1/a/c/h">>,
    DirD =    <<"/space_name1/a/d">>,
    FileI =     <<"/space_name1/a/d/i">>,
    DirE =      <<"/space_name1/a/d/e">>,
    FileF =   <<"/space_name1/a/f">>,
    DirX =    <<"/space_name1/a/x">>,
    FileJ =     <<"/space_name1/a/x/j">>,
    {ok, DirAGuid} = lfm_proxy:mkdir(W, SessId, DirA, 8#700),
    {ok, DirBGuid} = lfm_proxy:mkdir(W, SessId, DirB, 8#300),
    {ok, DirCGuid} = lfm_proxy:mkdir(W, SessId, DirC, 8#700),
    {ok, DirDGuid} = lfm_proxy:mkdir(W, SessId, DirD, 8#700),
    {ok, DirEGuid} = lfm_proxy:mkdir(W, SessId, DirE, 8#000),
    {ok, DirXGuid} = lfm_proxy:mkdir(W, SessId, DirX, 8#700),
    {ok, FileFGuid} = lfm_proxy:create(W, SessId, FileF, 8#000),
    {ok, FileGGuid} = lfm_proxy:create(W, SessId, FileG, 8#000),
    {ok, FileHGuid} = lfm_proxy:create(W, SessId, FileH, 8#000),
    {ok, FileIGuid} = lfm_proxy:create(W, SessId, FileI, 8#000),
    {ok, FileJGuid} = lfm_proxy:create(W, SessId, FileJ, 8#000),
    ok = lfm_proxy:set_perms(W, SessId, {guid, DirXGuid}, 8#500),

    % when
    ?assertEqual({error, ?EACCES}, lfm_proxy:rm_recursive(W, SessId, {guid, DirAGuid})),

    % then
    ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId, {guid, DirAGuid})),
    ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId, {guid, DirBGuid})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W, SessId, {guid, DirCGuid})),
    ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId, {guid, DirDGuid})),
    ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId, {guid, DirEGuid})),
    ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId, {guid, DirXGuid})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W, SessId, {guid, FileFGuid})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W, SessId, {guid, FileGGuid})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W, SessId, {guid, FileHGuid})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W, SessId, {guid, FileIGuid})),
    ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId, {guid, FileJGuid})).


file_gap_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    {ok, Guid} = lfm_proxy:create(W, SessId, <<"/space_name2/f">>, 8#777),
    {ok, Handle} = lfm_proxy:open(W, SessId, {guid, Guid}, rdwr),

    % when
    {ok, 3} = lfm_proxy:write(W, Handle, 3, <<"abc">>),
    ok = lfm_proxy:fsync(W, Handle),

    % then
    ?assertEqual({ok, <<0, 0, 0, $a, $b, $c>>},
        lfm_proxy:read(W, Handle, 0, 6)),
    ?assertEqual({ok, <<0, 0, 0, $a, $b, $c>>},
        lfm_proxy:read(W, Handle, 0, 100)),

    % when
    {ok, 4} = lfm_proxy:write(W, Handle, 8, <<"defg">>),
    ok = lfm_proxy:fsync(W, Handle),

    % then
    ?assertEqual({ok, <<0, 0, 0, $a, $b, $c, 0, 0, $d, $e, $f, $g>>},
        lfm_proxy:read(W, Handle, 0, 12)).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    initializer:setup_storage(ConfigWithNodes).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    ?TEST_STOP(Config).

init_per_testcase(Case, Config) ->
    ?CASE_START(Case),
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(Case, Config) ->
    ?CASE_STOP(Case),
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Get uuid of given by path file. Possible as root to bypass permissions checks.
get_uuid_privileged(Worker, SessId, Path) ->
    get_uuid(Worker, SessId, Path).

get_uuid(Worker, SessId, Path) ->
    #fuse_response{fuse_response = #file_attr{uuid = UUID}} = ?assertMatch(
        #fuse_response{status = #status{code = ?OK}},
        ?req(Worker, SessId, #resolve_guid{path = Path}),
        30
    ),
    UUID.

for(From, To, Fun) ->
    for(From, To, 1, Fun).
for(From, To, Step, Fun) ->
    [Fun(I) || I <- lists:seq(From, To, Step)].

measure_execution_time(Fun) ->
    StartTime = os:timestamp(),
    Ans = Fun(),
    Now = os:timestamp(),
    {timer:now_diff(Now, StartTime), Ans}.

check_run_parallel_ans(0) ->
    ok;
check_run_parallel_ans(Num) ->
    RStatus = receive
        run_parallel_ok ->
            ok
    after
        100000 ->
            timeout
    end,
    ?assertEqual(ok, RStatus),
    check_run_parallel_ans(Num - 1).

report_success(Master) ->
    Master ! run_parallel_ok.