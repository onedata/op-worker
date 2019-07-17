%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%--------------------------------------------------------------------
-module(qos_test_SUITE).
-author("Michal Cwiertnia").

-include_lib("ctool/include/test/test_utils.hrl").
-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    empty_qos/1,
    add_qos_for_file_1/1,
    add_qos_for_file_2/1,
    add_qos_for_file_3/1,
    add_multiple_qos_for_file/1,
    add_qos_for_multiple_files_1/1,
    add_qos_for_multiple_files_2/1,
    add_qos_for_dir/1,
    create_new_file_in_dir_with_qos/1,
    create_new_file_in_dir_structure_with_qos/1,
    add_qos/7,
    wait_for_qos_fulfilment/6
]).

all() -> [
    empty_qos,
    add_qos_for_file_1,
    add_qos_for_file_2,
    add_qos_for_file_3,
    add_multiple_qos_for_file,
    add_qos_for_multiple_files_1,
    add_qos_for_multiple_files_2,
    add_qos_for_dir,
    create_new_file_in_dir_with_qos,
    create_new_file_in_dir_structure_with_qos
].

-define(req(W, SessId, FuseRequest), element(2, rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}]))).
-define(TEST_POOL, test_pool).
-define(TEST_CACHE, test_cache).

-define(SPACE_ID, <<"space1">>).

-define(FILE_PATH(FileName), filename:join(["/", ?SPACE_ID, FileName])).

-define(WORKER_POOL, worker_pool).
-define(ATTEMPTS, 60).

% qos for test providers
-define(P1_TEST_QOS, #{
    <<"country">> => <<"PL">>,
    <<"type">> => <<"disk">>,
    <<"tier">> => <<"t3">>
}).

-define(P2_TEST_QOS, #{
    <<"country">> => <<"FR">>,
    <<"type">> => <<"tape">>,
    <<"tier">> => <<"t2">>
}).

-define(P3_TEST_QOS, #{
    <<"country">> => <<"PT">>,
    <<"type">> => <<"disk">>,
    <<"tier">> => <<"t2">>
}).

-define(TEST_PROVIDERS_QOS, #{
    <<"p1">> => ?P1_TEST_QOS,
    <<"p2">> => ?P2_TEST_QOS,
    <<"p3">> => ?P3_TEST_QOS
}).

-define(Q1, <<"q1">>).
-define(Q2, <<"q2">>).

%%%====================================================================
%%% Test function
%%%====================================================================




empty_qos(Config) ->
    [_WorkerP1, WorkerP2, _WorkerP3] = get_op_nodes(Config),
    {SessId1, _UserId1} = {
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
        ?config({user_id, <<"user1">>}, Config)
    },

    % create file
    FileGuid = create_test_file(WorkerP2, SessId1, ?FILE_PATH(<<"file1">>), <<"test_data">>),

    % check that file has empty qos
    ?assertMatch({ok, undefined}, lfm_proxy:get_file_qos(WorkerP2, SessId1, {guid, FileGuid})).


add_qos_for_file_1(Config) ->
    [WorkerP1, WorkerP2, _WorkerP3] = get_op_nodes(Config),
    P1Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP1)),
    P2Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP2)),

    Spec = #{
        source_provider => WorkerP2,
        directory_structure_before =>
        {?SPACE_ID, [
                {<<"file1">>, <<"test_data">>, [P2Id]}
        ]},
        qos => [
            #{
                name => ?Q1,
                path => ?FILE_PATH(<<"file1">>),
                expression => [<<"country=PL">>]
            }
        ],
        files_qos => [
            #{
                paths => [?FILE_PATH(<<"file1">>)],
                qos_list => [?Q1],
                target_providers => #{
                    P1Id => [?Q1]
                }
            }
        ],
        directory_structure_after =>
        {?SPACE_ID, [
            {<<"file1">>, <<"test_data">>, [P1Id, P2Id]}
        ]}
    },

    add_qos_base(Config, Spec).

add_qos_for_file_2(Config) ->
    [WorkerP1, WorkerP2, WorkerP3] = get_op_nodes(Config),
    P1Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP1)),
    P2Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP2)),
    P3Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP3)),

    Spec = #{
        source_provider => WorkerP2,
        directory_structure_before =>
        {?SPACE_ID, [
            {<<"file1">>, <<"test_data">>, [P2Id]}
        ]},
        qos => [
            #{
                name => ?Q1,
                path => ?FILE_PATH(<<"file1">>),
                expression => [<<"type=disk">>],
                replicas_num => 2
            }
        ],
        files_qos => [
            #{
                paths => [?FILE_PATH(<<"file1">>)],
                qos_list => [?Q1],
                target_providers => #{
                    P1Id => [?Q1],
                    P3Id => [?Q1]
                }
            }
        ],
        directory_structure_after =>
        {?SPACE_ID, [
            {<<"file1">>, <<"test_data">>, [P1Id, P2Id, P3Id]}
        ]}
    },

    add_qos_base(Config, Spec).

add_qos_for_file_3(Config) ->
    [_WorkerP1, WorkerP2, _WorkerP3] = get_op_nodes(Config),
    P2Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP2)),

    Spec = #{
        source_provider => WorkerP2,
        directory_structure_before =>
        {?SPACE_ID, [
            {<<"file1">>, <<"test_data">>, [P2Id]}
        ]},
        qos => [
            #{
                name => ?Q1,
                path => ?FILE_PATH(<<"file1">>),
                expression => [<<"country=FR">>]
            }
        ],
        files_qos => [
            #{
                paths => [?FILE_PATH(<<"file1">>)],
                qos_list => [?Q1],
                target_providers => #{
                    P2Id => [?Q1]
                }
            }
        ],
        directory_structure_after =>
        {?SPACE_ID, [
            {<<"file1">>, <<"test_data">>, [P2Id]}
        ]}
    },

    add_qos_base(Config, Spec).

add_multiple_qos_for_file(Config) ->
    [WorkerP1, WorkerP2, WorkerP3] = get_op_nodes(Config),
    P1Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP1)),
    P2Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP2)),
    P3Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP3)),

    Spec = #{
        source_provider => WorkerP2,
        directory_structure_before =>
        {?SPACE_ID, [
            {<<"file1">>, <<"test_data">>, [P2Id]}
        ]},
        qos => [
            #{
                name => ?Q1,
                path => ?FILE_PATH(<<"file1">>),
                expression => [<<"country=PL">>]
            },
            #{
                name => ?Q2,
                path => ?FILE_PATH(<<"file1">>),
                expression => [<<"country=PT">>]
            }
        ],
        files_qos => [
            #{
                paths => [?FILE_PATH(<<"file1">>)],
                qos_list => [?Q1, ?Q2],
                target_providers => #{
                    P1Id => [?Q1],
                    P3Id => [?Q2]
                }
            }
        ],
        directory_structure_after =>
        {?SPACE_ID, [
            {<<"file1">>, <<"test_data">>, [P1Id, P2Id, P3Id]}
        ]}
    },

    add_qos_base(Config, Spec).

add_qos_for_multiple_files_1(Config) ->
    [WorkerP1, WorkerP2, WorkerP3] = get_op_nodes(Config),
    P1Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP1)),
    P2Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP2)),
    P3Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP3)),

    Spec = #{
        source_provider => WorkerP2,
        directory_structure_before =>
        {?SPACE_ID, [
            {<<"file1">>, <<"test_data">>, [P2Id]},
            {<<"file2">>, <<"test_data2">>, [P2Id]}
        ]},
        qos => [
            #{
                name => ?Q1,
                path => ?FILE_PATH(<<"file1">>),
                expression => [<<"country=PL">>]
            },
            #{
                name => ?Q2,
                path => ?FILE_PATH(<<"file2">>),
                expression => [<<"country=PT">>]
            }
        ],
        files_qos => [
            #{
                paths => [?FILE_PATH(<<"file1">>)],
                qos_list => [?Q1],
                target_providers => #{
                    P1Id => [?Q1]
                }
            },
            #{
                paths => [?FILE_PATH(<<"file2">>)],
                qos_list => [?Q2],
                target_providers => #{
                    P3Id => [?Q2]
                }
            }
        ],
        directory_structure_after =>
        {?SPACE_ID, [
            {<<"file1">>, <<"test_data">>, [P1Id, P2Id]},
            {<<"file2">>, <<"test_data2">>, [P2Id, P3Id]}
        ]}
    },

    add_qos_base(Config, Spec).

add_qos_for_multiple_files_2(Config) ->
    [WorkerP1, WorkerP2, WorkerP3] = get_op_nodes(Config),
    P1Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP1)),
    P2Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP2)),
    P3Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP3)),

    Spec = #{
        source_provider => WorkerP2,
        directory_structure_before =>
        {?SPACE_ID, [
            {<<"file1">>, <<"test_data">>, [P2Id]},
            {<<"file2">>, <<"test_data2">>, [P2Id]}
        ]},
        qos => [
            #{
                name => ?Q1,
                path => ?FILE_PATH(<<"file1">>),
                expression => [<<"country=PL">>]
            },
            #{
                name => ?Q2,
                path => ?FILE_PATH(<<"file2">>),
                expression => [<<"type=disk">>],
                replicas_num => 2
            }
        ],
        files_qos => [
            #{
                paths => [?FILE_PATH(<<"file1">>)],
                qos_list => [?Q1],
                target_providers => #{
                    P1Id => [?Q1]
                }
            },
            #{
                paths => [?FILE_PATH(<<"file2">>)],
                qos_list => [?Q2],
                target_providers => #{
                    P1Id => [?Q2],
                    P3Id => [?Q2]
                }
            }
        ],
        directory_structure_after =>
        {?SPACE_ID, [
            {<<"file1">>, <<"test_data">>, [P1Id, P2Id]},
            {<<"file2">>, <<"test_data2">>, [P1Id, P2Id, P3Id]}
        ]}
    },

    add_qos_base(Config, Spec).

add_qos_for_dir(Config) ->
    [WorkerP1, WorkerP2, _WorkerP3] = get_op_nodes(Config),
    P1Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP1)),
    P2Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP2)),
    File1Path = filename:join(?FILE_PATH(<<"dir1">>), <<"file1">>),
    File2Path = filename:join(?FILE_PATH(<<"dir1">>), <<"file2">>),
    File3Path = filename:join(?FILE_PATH(<<"dir1">>), <<"file3">>),


    Spec = #{
        source_provider => WorkerP2,
        directory_structure_before =>
        {?SPACE_ID, [
            {<<"dir1">>, [
                {<<"file1">>, <<"test_data">>, [P2Id]},
                {<<"file2">>, <<"test_data2">>, [P2Id]},
                {<<"file3">>, <<"test_data3">>, [P2Id]}
            ]}
        ]},
        qos => [
            #{
                name => ?Q1,
                path => ?FILE_PATH(<<"dir1">>),
                expression => [<<"country=PL">>]
            }
        ],
        files_qos => [
            #{
                paths => [File1Path, File2Path, File3Path],
                qos_list => [],
                target_providers => #{
                    P1Id => [?Q1]
                }
            }
        ],
        directory_structure_after =>
        {?SPACE_ID, [
            {<<"dir1">>, [
                {<<"file1">>, <<"test_data">>, [P1Id, P2Id]},
                {<<"file2">>, <<"test_data2">>, [P1Id, P2Id]},
                {<<"file3">>, <<"test_data3">>, [P1Id, P2Id]}
            ]}
        ]}
    },

    add_qos_base(Config, Spec).

create_new_file_in_dir_with_qos(Config) ->
    [WorkerP1, WorkerP2, _WorkerP3] = get_op_nodes(Config),
    P1Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP1)),
    P2Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP2)),
    File1Path = filename:join(?FILE_PATH(<<"dir1">>), <<"file1">>),
    File2Path = filename:join(?FILE_PATH(<<"dir1">>), <<"file2">>),
    File3Path = filename:join(?FILE_PATH(<<"dir1">>), <<"file3">>),

    Spec = #{
        source_provider => WorkerP2,
        directory_structure_before =>
        {?SPACE_ID, [
            {<<"dir1">>, [
                {<<"file1">>, <<"test_data">>, [P2Id]},
                {<<"file2">>, <<"test_data2">>, [P2Id]},
                {<<"file3">>, <<"test_data3">>, [P2Id]}
            ]}
        ]},
        qos => [
            #{
                name => ?Q1,
                path => ?FILE_PATH(<<"dir1">>),
                expression => [<<"country=PL">>]
            }
        ],
        files_qos => [
            #{
                paths => [File1Path, File2Path, File3Path],
                qos_list => [],
                target_providers => #{
                    P1Id => [?Q1]
                }
            }
        ],
        directory_structure_after =>
        {?SPACE_ID, [
            {<<"dir1">>, [
                {<<"file1">>, <<"test_data">>, [P1Id, P2Id]},
                {<<"file2">>, <<"test_data2">>, [P1Id, P2Id]},
                {<<"file3">>, <<"test_data3">>, [P1Id, P2Id]}
            ]}
        ]},
        add_files_after_qos_fulfilled =>
        {?SPACE_ID, [
            {<<"dir1">>, [
                {<<"file4">>, <<"test_data4">>, [P2Id]}
            ]}
        ]},
        qos_invalidated => [
            #{
                name => ?Q1,
                path => ?FILE_PATH(<<"dir1">>)
            }
        ],
        dir_structure_with_new_files =>
        {?SPACE_ID, [
            {<<"dir1">>, [
                {<<"file1">>, <<"test_data">>, [P1Id, P2Id]},
                {<<"file2">>, <<"test_data2">>, [P1Id, P2Id]},
                {<<"file3">>, <<"test_data3">>, [P1Id, P2Id]},
                {<<"file4">>, <<"test_data4">>, [P1Id, P2Id]}
            ]}
        ]}
    },

    add_qos_base(Config, Spec).

create_new_file_in_dir_structure_with_qos(Config) ->
    [WorkerP1, WorkerP2, WorkerP3] = get_op_nodes(Config),
    P1Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP1)),
    P2Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP2)),
    P3Id = initializer:domain_to_provider_id(?GET_DOMAIN(WorkerP3)),
    File1Path = filename:join(?FILE_PATH(<<"dir1">>), <<"dir2/dir3/file1">>),

    Spec = #{
        source_provider => WorkerP2,
        directory_structure_before =>
        {?SPACE_ID, [
            {<<"dir1">>, [
                {<<"dir2">>, [
                    {<<"dir3">>, [
                        {<<"file1">>, <<"test_data">>, [P2Id]}
                    ]}
                ]}
            ]}
        ]},
        qos => [
            #{
                name => ?Q1,
                path => ?FILE_PATH(<<"dir1">>),
                expression => [<<"country=PL">>]
            },
            #{
                name => ?Q2,
                path => filename:join(?FILE_PATH(<<"dir1">>), <<"dir2">>),
                expression => [<<"country=PT">>]
            }
        ],
        files_qos => [
            #{
                paths => [File1Path],
                qos_list => [],
                target_providers => #{
                    P1Id => [?Q1],
                    P3Id => [?Q2]
                }
            }
        ],
        directory_structure_after =>
        {?SPACE_ID, [
            {<<"dir1">>, [
                {<<"dir2">>, [
                    {<<"dir3">>, [
                        {<<"file1">>, <<"test_data">>, [P1Id, P2Id, P3Id]}
                    ]}
                ]}
            ]}
        ]},
        add_files_after_qos_fulfilled =>
        {?SPACE_ID, [
            {<<"dir1">>, [
                {<<"file4">>, <<"test_data4">>, [P2Id]},
                {<<"dir2">>, [
                    {<<"dir3">>, [
                        {<<"file5">>, <<"test_data5">>, [P2Id]}
                    ]}
                ]}
            ]}
        ]},
        qos_invalidated => [
            #{
                name => ?Q1,
                path => ?FILE_PATH(<<"dir1">>)
            },
            #{
                name => ?Q2,
                path => filename:join(?FILE_PATH(<<"dir1">>), <<"dir2">>)
            }
        ],
        dir_structure_with_new_files =>
        {?SPACE_ID, [
            {<<"dir1">>, [
                {<<"file4">>, <<"test_data4">>, [P1Id, P2Id]},
                {<<"dir2">>, [
                    {<<"dir3">>, [
                        {<<"file1">>, <<"test_data">>, [P1Id, P2Id, P3Id]},
                        {<<"file5">>, <<"test_data5">>, [P1Id, P2Id, P3Id]}
                    ]}
                ]}
            ]}
        ]}
    },

    add_qos_base(Config, Spec).

add_qos_base(Config, Spec) ->
    #{
        source_provider := SourceProvider,
        directory_structure_before := DirStructure,
        qos := QosToAddList,
        files_qos := ExpectedFilesQos,
        directory_structure_after := DirStructureAfter
    } = Spec,

    {SessId1, _UserId1} = {
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(SourceProvider)}}, Config),
        ?config({user_id, <<"user1">>}, Config)
    },

    % create initial dir structure, check initial distribution
    create_dir_structure(SourceProvider, SessId1, DirStructure, <<"/">>),
    ?assertMatch(true, assert_distribution_in_dir_structure(SourceProvider,
        SessId1, DirStructure, <<"/">>, ?ATTEMPTS)),

    % add qos specified in test spec
    PathToFileQos = add_qos_in_parallel(SourceProvider, SessId1, QosToAddList),
    wait_for_qos_fulfilment_in_parallel(SourceProvider, SessId1, PathToFileQos),

    % check distribution after qos is fulfilled, check file_qos
    ?assertMatch(true, assert_file_qos(SourceProvider, SessId1, ExpectedFilesQos, PathToFileQos)),
    ?assertMatch(true, assert_distribution_in_dir_structure(SourceProvider,
        SessId1, DirStructureAfter, <<"/">>, ?ATTEMPTS)),

    case maps:is_key(add_files_after_qos_fulfilled, Spec) of
        true ->
            #{
                add_files_after_qos_fulfilled := NewFiles,
                qos_invalidated := QosInvalidated,
                dir_structure_with_new_files := NewDirStructure
            } = Spec,

            create_dir_structure(SourceProvider, SessId1, NewFiles, <<"/">>),

            % check file distribution and that qos is not fulfilled
            ?assertMatch(true, assert_distribution_in_dir_structure(SourceProvider, SessId1,
                NewFiles, <<"/">>, ?ATTEMPTS)),

            % check that qos have been invalidated
            InvalidatedQos = assert_qos_invalidated(SourceProvider, SessId1, QosInvalidated, PathToFileQos),

            % get qos record
            wait_for_qos_fulfilment_in_parallel(SourceProvider, SessId1, InvalidatedQos),

            ?assertMatch(true, assert_distribution_in_dir_structure(SourceProvider,
                SessId1, NewDirStructure, <<"/">>, ?ATTEMPTS));
        false ->
            ok
    end.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off)
                      end, ?config(op_worker_nodes, NewConfig2)),

        application:start(ssl),
        hackney:start(),
        NewConfig2
               end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, transfers_test_utils, transfers_test_mechanism, ?MODULE]}
        | Config
    ].

end_per_suite(Config) ->
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).

init_per_testcase(_, Config) ->
    ct:timetrap(timer:minutes(5)),
    NewConfig = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    ensure_verification_pool_started(),
    NewConfig2 = replication_transfers_test_base:init_per_testcase(default, NewConfig),
    mock_providers_qos(NewConfig2),
    NewConfig2.

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    transfers_test_utils:unmock_replication_worker(Workers),
    transfers_test_utils:unmock_replica_synchronizer_failure(Workers),
    transfers_test_utils:remove_transfers(Config),
    transfers_test_utils:remove_all_indexes(Workers, ?SPACE_ID),
    transfers_test_utils:ensure_transfers_removed(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_op_nodes(Config) ->
    Workers = ?config(op_worker_nodes, Config),

    % return list of workers sorted using provider ID
    SortingFun = fun(Worker1, Worker2) ->
        ProviderId1 = initializer:domain_to_provider_id(?GET_DOMAIN(Worker1)),
        ProviderId2 = initializer:domain_to_provider_id(?GET_DOMAIN(Worker2)),
        ProviderId1 =< ProviderId2
    end,
    lists:sort(SortingFun, Workers).

create_dir_structure(Worker, SessionId, {DirName, DirContent}, Path) when DirName =:= ?SPACE_ID ->
    DirPath = filename:join(Path, DirName),
    lists:foreach(fun(Child) ->
        create_dir_structure(Worker, SessionId, Child, DirPath)
    end, DirContent);
create_dir_structure(Worker, SessionId, {DirName, DirContent}, Path) ->
    try
        lfm_proxy:mkdir(Worker, SessionId, filename:join(Path, DirName))
    catch
        error:eexist ->
            ok
    end,
    DirPath = filename:join(Path, DirName),
    lists:foreach(fun(Child) ->
        create_dir_structure(Worker, SessionId, Child, DirPath)
    end, DirContent);
create_dir_structure(Worker, SessionId, {FileName, FileContent, _FileDistribution}, Path) ->
    create_test_file(Worker, SessionId, filename:join(Path, FileName), FileContent).

create_test_file(Worker, SessionId, File, TestData) ->
    {ok, FileGuid} = lfm_proxy:create(Worker, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(Worker, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(Worker, Handle, 0, TestData),
    lfm_proxy:fsync(Worker, Handle),
    lfm_proxy:close(Worker, Handle),
    FileGuid.

assert_distribution_in_dir_structure(_Worker, _SessionId, _DirStructure, _Path, Attempts) when Attempts == 0 ->
    false;
assert_distribution_in_dir_structure(Worker, SessionId, DirStructure, Path, Attempts) when Attempts > 0 ->
    PrintError = Attempts == 1,
    case assert_file_distribution(Worker, SessionId, DirStructure, Path, PrintError) of
        true ->
            true;
        false ->
            timer:sleep(timer:seconds(1)),
            assert_distribution_in_dir_structure(Worker, SessionId, DirStructure, Path, Attempts - 1)
    end.

assert_file_distribution(Worker, SessionId, {DirName, DirContent}, Path, PrintError) ->
    lists:foldl(fun(Child, Matched) ->
        DirPath = filename:join(Path, DirName),
        case assert_file_distribution(Worker, SessionId, Child, DirPath, PrintError) of
            true ->
                Matched;
            false ->
                false
        end
    end, true, DirContent);
assert_file_distribution(Worker, SessId, {FileName, FileContent, ExpectedFileDistribution}, Path, PrintError) ->
    FilePath = filename:join(Path, FileName),
    FileGuid = get_guid(Worker, SessId, FilePath),
    {ok, FileLocations} = lfm_proxy:get_file_distribution(Worker, SessId, {guid, FileGuid}),
    ExpectedDistributionSorted = lists:sort(
        fill_in_expected_distribution(ExpectedFileDistribution, FileContent)
    ),
    FileLocationsSorted = lists:sort(FileLocations),

    case FileLocationsSorted == ExpectedDistributionSorted of
        true ->
            true;
        false ->
            case PrintError of
                true ->
                    ct:pal("Wrong file distribution for ~p. ~n"
                           "    Expected: ~p~n"
                           "    Got: ~p~n", [FilePath, ExpectedDistributionSorted, FileLocationsSorted]),
                    false;
                false ->
                    false
            end
    end.

fill_in_expected_distribution(ExpectedDistribution, FileContent) ->
    lists:map(fun(ProviderDistributionOrId) ->
        case is_map(ProviderDistributionOrId) of
            true ->
                ProviderDistribution = ProviderDistributionOrId,
                case maps:is_key(<<"blocks">>, ProviderDistribution) of
                    true ->
                        ProviderDistribution;
                    _ ->
                        ProviderDistribution#{
                            <<"blocks">> => [[0, size(FileContent)]],
                            <<"totalBlocksSize">> => size(FileContent)
                        }
                end;
            false ->
                ProviderId = ProviderDistributionOrId,
                #{
                    <<"providerId">> => ProviderId,
                    <<"blocks">> => [[0, size(FileContent)]],
                    <<"totalBlocksSize">> => size(FileContent)
                }
        end
    end, ExpectedDistribution).

assert_qos_fulfilled(Worker, SessId, QosId) ->
    case rpc:call(Worker, lfm_qos, check_qos_fulfilled, [SessId, QosId]) of
        true ->
            true;
        false ->
            {ok, Qos} = lfm_proxy:get_qos_details(Worker, SessId, QosId),
            ct:pal("Qos not fulfilled while it should be: ~n"
                   "  ActiveTransfers:     ~p~n"
                   "  GeneratingTransfers: ~p~n"
                   "  TraverseJob:         ~p~n",
                [Qos#qos_entry.active_transfers, Qos#qos_entry.active_tasks,
                    Qos#qos_entry.traverse_job]),
            false
    end.

assert_file_qos(Worker, SessId, ExpectedFileQosList, QosDescList) ->
    lists:all(fun(ExpectedFileQos) ->
        #{
            paths := PathsList,
            qos_list := ExpectedQosListWithNames,
            target_providers := ExpectedTargetProvidersWithNames
        } = ExpectedFileQos,

        lists:foldl(fun(Path, Matched) ->
            % get actual file qos
            {ok, FileQos} = lfm_proxy:get_file_qos(Worker, SessId, {path, Path}),

            % in test spec we pass qos name, now we have to change it to qos id
            ExpectedQosList = lists:map(fun(QosName) ->
                QosDesc = lists:keyfind(QosName, 1, QosDescList),
                element(2, QosDesc)
            end, ExpectedQosListWithNames),

            % sort both expected and actual qos_list and check if they match
            ExpectedQosListSorted = lists:sort(ExpectedQosList),
            FileQosSorted = lists:sort(FileQos#file_qos.qos_list),
            QosListMatched = ExpectedQosListSorted == FileQosSorted,
            case QosListMatched of
                true ->
                    ok;
                false ->
                    ct:pal("Wrong qos_list for: ~p~n"
                           "    Expected: ~p~n"
                           "    Got: ~p~n", [Path, ExpectedQosList, FileQosSorted])
            end,

            % again in test spec we have qos names, need to change them to qos ids
            % in the same time all qos id lists are sorted
            ExpectedTargetProvidersSorted = maps:map(fun(_ProvId, QosNamesList) ->
                lists:sort(
                    lists:map(fun(QosName) ->
                        QosDesc = lists:keyfind(QosName, 1, QosDescList),
                        element(2, QosDesc)
                    end, QosNamesList)
                )
            end, ExpectedTargetProvidersWithNames),

            % sort qos id lists in actual target providers
            TargetProvidersSorted = maps:map(fun(_ProvId, QosIdList) ->
                lists:sort(QosIdList)
            end, FileQos#file_qos.target_storages),

            TargetProvidersMatched = TargetProvidersSorted == ExpectedTargetProvidersSorted,
            case TargetProvidersMatched of
                true ->
                    ok;
                false ->
                    ct:pal("Wrong target providers for: ~p~n"
                            "    Expected: ~p~n"
                            "    Got: ~p~n", [Path, ExpectedTargetProvidersSorted,
                        TargetProvidersSorted])
            end,
            case QosListMatched andalso TargetProvidersMatched of
                true ->
                    Matched;
                false ->
                    false
            end
        end, true, PathsList)
    end, ExpectedFileQosList).

assert_qos_invalidated(Worker, SessId, QosToCheckList, QosDescList) ->
    lists:foldl(fun(InvalidatedQosMap, InvalidatedQosPartial) ->
        #{
            name := QosName,
            path := Path
        } = InvalidatedQosMap,

        FileGuid = get_guid(Worker, SessId, Path),
        {ok, #file_qos{qos_list = FileQos}} = ?assertMatch(
            {ok, #file_qos{qos_list = _FileQos}},
            lfm_proxy:get_file_qos(Worker, SessId, {guid, FileGuid})
        ),

        QosId = element(2, lists:keyfind(QosName, 1, QosDescList)),
        ?assertMatch(false, rpc:call(Worker, lfm_qos, check_qos_fulfilled, [SessId, FileQos])),
        ?assertMatch(false, rpc:call(Worker, lfm_qos, check_qos_fulfilled, [SessId, QosId])),

        [{QosName, QosId, Path} | InvalidatedQosPartial]
    end, [], QosToCheckList).

add_qos_in_parallel(Worker, SessId, QosToAddList) ->
    Children = lists:foldl(
        fun (QosToAdd, PartialChildren) ->
                #{
                    name := QosName,
                    expression := QosExpression,
                    path := Path
                } = QosToAdd,
                ReplicasNum = maps:get(replicas_num, QosToAdd, 1),
                [spawn_link(?MODULE, add_qos, [self(), Worker, SessId, Path,
                    QosExpression, ReplicasNum, QosName]) | PartialChildren]
        end, [], QosToAddList
    ),

    lists:foldl(fun (_Child, PathQosIdPartial) ->
        receive
            {QosName, QosId, Path} ->
                [{QosName, QosId, Path}|PathQosIdPartial]
        after timer:seconds(30) ->
            throw(timeout)
        end
    end, [], Children).

add_qos(Master, Worker, SessId, Path, QosExpression, ReplicasNum, QosName) ->
    ct:pal("Adding qos with expression: ~p for file: ~p~n", [QosExpression, Path]),
    Guid = get_guid(Worker, SessId, Path),
    {ok, QosId} = ?assertMatch(
        {ok, _QosId},
        lfm_proxy:add_qos(Worker, SessId, {guid, Guid},
            qos_expression:transform_to_rpn(QosExpression), ReplicasNum)
    ),

    % check that file qos has been set and that it is not fulfilled
    {ok, #file_qos{qos_list = FileQos}} = ?assertMatch(
        {ok, #file_qos{qos_list = _FileQos}},
        lfm_proxy:get_file_qos(Worker, SessId, {guid, Guid})
    ),
    ?assertMatch(false, rpc:call(Worker, lfm_qos, check_qos_fulfilled, [SessId, FileQos])),

    % send reply to master
    Master ! {QosName, QosId, Path}.

wait_for_qos_fulfilment_in_parallel(Worker, SessId, QosPathToId) ->
    Children = lists:foldl(
        fun({QosName, QosId, Path}, Children) ->
                [spawn_link(?MODULE, wait_for_qos_fulfilment,
                    [self(), Worker, SessId, QosId, QosName, Path]) | Children]
        end, [], QosPathToId),

    lists:foreach(fun (_Child) ->
        receive
            {Path, QosId} ->
                ?assertMatch(true, assert_qos_fulfilled(Worker, SessId, QosId));
            timeout ->
                throw(timeout)
        after timer:seconds(2 * ?ATTEMPTS) ->
            throw(timeout)
        end end, Children).

wait_for_qos_fulfilment(Master, Worker, SessId, QosId, QosName, Path) ->
    {ok, QosRecord} = ?assertMatch({ok, _}, lfm_proxy:get_qos_details(Worker, SessId, QosId)),
    ct:pal("Waiting for fulfilment of qos ~p: ~n"
           "    Expression:          ~p~n"
           "    ActiveTransfers:     ~p~n"
           "    TraverseJob:         ~p~n"
           "    ActiveTasks:         ~p~n", [QosName, QosRecord#qos_entry.expression,
        QosRecord#qos_entry.active_transfers, QosRecord#qos_entry.traverse_job, QosRecord#qos_entry.active_tasks]
    ),
    wait_for_qos_fulfilment(Master, Worker, SessId, QosId, QosName, Path, ?ATTEMPTS).

wait_for_qos_fulfilment(Master, Worker, SessId, QosId, QosName, Path, Attempts) ->
    {ok, QosRecord} = ?assertMatch({ok, _}, lfm_proxy:get_qos_details(Worker, SessId, QosId)),
    case {QosRecord#qos_entry.active_tasks == [], QosRecord#qos_entry.traverse_job == undefined} of
        {true, true} ->
            case wait_for_transfers(Worker, QosRecord#qos_entry.active_transfers, Path, QosId) of
                true ->
                    Master ! {Path, QosId};
                false ->
                    print_qos_fulfilment_timeout_msg(QosRecord, QosName),
                    Master ! timeout
            end;
        {_, _} ->
            case Attempts == 0 of
                true ->
                    print_qos_fulfilment_timeout_msg(QosRecord, QosName),
                    Master ! timeout;
                _ ->
                    timer:sleep(1000),
                    wait_for_qos_fulfilment(Master, Worker, SessId, QosId, QosName,
                        Path, Attempts - 1)
            end
    end.

print_qos_fulfilment_timeout_msg(Qos, QosName) ->
    ct:pal("Timeout while waiting for fulfilment of qos ~p: ~n"
    "    Expression:          ~p~n"
    "    ActiveTransfers:     ~p~n"
    "    TraverseJob:         ~p~n"
    "    ActiveTasks:         ~p~n",
        [QosName, Qos#qos_entry.expression, Qos#qos_entry.active_transfers, Qos#qos_entry.traverse_job,
            Qos#qos_entry.active_tasks]
    ).

wait_for_transfers(Worker, Transfers, Path, QosId) ->
    lists:foreach(fun(TransferId) ->
        {ok, #document{value = TransferRecord}} = rpc:call(Worker, transfer, get, [TransferId]),
        ct:pal("Transfer for ~p in progress: ~n"
               "  from: ~p~n"
               "  to    ~p~n",
            [TransferRecord#transfer.path, TransferRecord#transfer.scheduling_provider,
                TransferRecord#transfer.replicating_provider])
    end, Transfers),
    wait_for_transfers(Worker, Transfers, Path, QosId, ?ATTEMPTS).

wait_for_transfers(Worker, Transfers, Path, QosId, Attempts) ->
    ActiveTransferList = lists:foldl(fun(TransferId, Acc) ->
        case rpc:call(Worker, transfer, get, [TransferId]) of
            {ok, #document{value = TransferRecord}} ->
                case rpc:call(Worker, transfer, is_ended, [TransferRecord]) of
                    true ->
                        Acc;
                    false ->
                        [TransferId | Acc]
                end;
            {error, not_found} ->
                Acc
        end
    end, [], Transfers),

    case length(ActiveTransferList) of
        0 ->
            true;
        _ ->
            case Attempts == 0 of
                true ->
                    false;
                false ->
                    timer:sleep(1000),
                    wait_for_transfers(Worker, Transfers, Path, QosId, Attempts - 1)
            end
    end.

mock_providers_qos(Config) ->
    Workers = ?config(op_worker_nodes, Config),

    % needed as we do not have oz in tests
    test_utils:mock_new(Workers, oz_spaces, [passthrough]),
    test_utils:mock_expect(Workers, oz_spaces, get_providers,
        fun(_SessId, _SpaceId) ->
            {ok, lists:foldl(fun (Worker, Acc) ->
                [initializer:domain_to_provider_id(?GET_DOMAIN(Worker)) | Acc]
                             end, [], Workers)}
        end),

    test_utils:mock_new(Workers, providers_qos),
    test_utils:mock_expect(Workers, providers_qos, get_provider_qos,
        fun (ProviderId) ->
            % names of test providers starts with p1, p2 etc.
            {ok, ProvName} = provider_logic:get_name(ProviderId),
            maps:get(binary:part(ProvName, 0, 2), ?TEST_PROVIDERS_QOS)
        end).

ensure_verification_pool_started() ->
    {ok, _} = application:ensure_all_started(worker_pool),
    worker_pool:start_sup_pool(?WORKER_POOL, [{workers, 8}]).

get_guid(Worker, SessId, Path) ->
    #fuse_response{fuse_response = #guid{guid = Guid}} =
        ?assertMatch(
            #fuse_response{status = #status{code = ?OK}},
            ?req(Worker, SessId, #resolve_guid{path = Path}),
            30
        ),
    Guid.