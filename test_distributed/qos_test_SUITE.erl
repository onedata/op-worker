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

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    empty_qos/1,
    add_single_qos_for_file/1,
    add_single_qos_for_file_with_multiple_replicas/1,
    add_qos_for_file_3/1,
    add_multiple_qos_for_file/1,
    add_qos_for_multiple_files_1/1,
    add_qos_for_multiple_files_2/1,
    add_qos_for_dir/1,
    create_new_file_in_dir_with_qos/1,
    create_new_file_in_dir_structure_with_qos/1
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




%%empty_qos(Config) ->
%%    [_WorkerP1, WorkerP2, _WorkerP3] = get_op_nodes(Config),
%%    {SessId1, _UserId1} = {
%%        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
%%        ?config({user_id, <<"user1">>}, Config)
%%    },
%%
%%    % create file
%%    FileGuid = create_test_file(WorkerP2, SessId1, ?FILE_PATH(<<"file1">>), <<"test_data">>),
%%
%%    % check that file has empty qos
%%    ?assertMatch({ok, undefined}, lfm_proxy:get_file_qos(WorkerP2, SessId1, {guid, FileGuid})).


add_single_qos_for_file(Config) ->
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

    qos_test_utils:add_qos_test_base(Config, Spec).

add_single_qos_for_file_with_multiple_replicas(Config) ->
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

    qos_test_utils:add_qos_test_base(Config, Spec).

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

    qos_test_utils:add_qos_test_base(Config, Spec).

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

    qos_test_utils:add_qos_test_base(Config, Spec).

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

    qos_test_utils:add_qos_test_base(Config, Spec).

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

    qos_test_utils:add_qos_test_base(Config, Spec).

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

    qos_test_utils:add_qos_test_base(Config, Spec).

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

    qos_test_utils:add_qos_test_base(Config, Spec).

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

    qos_test_utils:add_qos_test_base(Config, Spec).

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
    test_utils:mock_unload(Workers, providers_qos),
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

create_test_file(Worker, SessionId, File, TestData) ->
    {ok, FileGuid} = lfm_proxy:create(Worker, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(Worker, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(Worker, Handle, 0, TestData),
    lfm_proxy:fsync(Worker, Handle),
    lfm_proxy:close(Worker, Handle),
    FileGuid.

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
