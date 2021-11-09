%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests for QoS management on multiple providers.
%%% @end
%%%--------------------------------------------------------------------
-module(multi_provider_qos_test_SUITE).
-author("Michal Cwiertnia").
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("qos_tests_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, init_per_testcase/2,
    end_per_suite/1, end_per_testcase/2
]).

%% tests
-export([
    effective_qos_for_file_in_directory/1,
    effective_qos_for_file_in_nested_directories/1,
    effective_qos_for_files_in_different_directories_of_tree_structure/1,

    qos_reconciliation_file_test/1,
    qos_reconciliation_dir_test/1,
    reconcile_qos_using_file_meta_posthooks_test/1,
    reconcile_with_links_race_test/1,
    reconcile_with_links_race_nested_file_test/1,
    reconcile_with_links_and_file_meta_race_nested_file_test/1,

    reevaluate_impossible_qos_test/1,
    reevaluate_impossible_qos_race_test/1,
    reevaluate_impossible_qos_conflict_test/1,

    qos_traverse_cancellation_test/1,
    
    qos_on_hardlink_test/1,
    effective_qos_with_hardlinks_test/1,
    qos_with_inode_deletion_test/1,
    qos_with_hardlink_deletion_test/1,
    qos_with_mixed_deletion_test/1,
    qos_on_symlink_test/1,
    effective_qos_with_symlink_test/1,
    create_hardlink_in_dir_with_qos/1,
    
    qos_transfer_stats_test/1
]).

all() -> [
    effective_qos_for_file_in_directory,
    effective_qos_for_file_in_nested_directories,
    effective_qos_for_files_in_different_directories_of_tree_structure,

    qos_reconciliation_file_test,
    qos_reconciliation_dir_test,
    reconcile_qos_using_file_meta_posthooks_test,
    reconcile_with_links_race_test,
    reconcile_with_links_race_nested_file_test,
    reconcile_with_links_and_file_meta_race_nested_file_test,

    reevaluate_impossible_qos_test,
    reevaluate_impossible_qos_race_test,
    reevaluate_impossible_qos_conflict_test,

    qos_traverse_cancellation_test,

    qos_on_hardlink_test,
    effective_qos_with_hardlinks_test,
    qos_with_inode_deletion_test,
    qos_with_hardlink_deletion_test,
    qos_with_mixed_deletion_test,
    qos_on_symlink_test,
    effective_qos_with_symlink_test,
    create_hardlink_in_dir_with_qos,
    
    qos_transfer_stats_test
].


-define(FILE_PATH(FileName), filename:join([?SPACE_PATH1, FileName])).

-define(SPACE_ID, <<"space1">>).
-define(SPACE_PATH1, <<"/space1">>).

-define(PROVIDER_ID(Worker), ?GET_DOMAIN_BIN(Worker)).

-define(ATTEMPTS, 60).

%%%====================================================================
%%% Test function
%%%====================================================================

%%%===================================================================
%%% Group of tests that creates directory structure, adds QoS on different
%%% levels of created structure and checks effective QoS and QoS docs.
%%%===================================================================

effective_qos_for_file_in_directory(Config) ->
    Workers = [WorkerP1, WorkerP2, Worker1P3, Worker2P3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    WorkersConfigurations = [
        [WorkerP1],
        [Worker1P3],
        [Worker2P3]
    ],

    lists:foreach(fun([Worker1] = WorkerConf) ->
        ct:pal("Starting for workers: ~p~n", [WorkerConf]),
        DirName = generator:gen_name(),
        DirPath = filename:join(?SPACE_PATH1, DirName),
        FilePath = filename:join(DirPath, <<"file1">>),

        QosSpec = qos_test_base:effective_qos_for_file_in_directory_spec(DirPath,
            FilePath, Worker1, Workers, get_provider_to_storage_mappings(Config)),
        add_qos_for_dir_and_check_effective_qos(Config,
            #effective_qos_test_spec{
                initial_dir_structure = #test_dir_structure{
                    worker = WorkerP1,
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"file1">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1)]}
                        ]}
                    ]}
                },
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_effective_qos = QosSpec#qos_spec.expected_effective_qos,
                expected_dir_structure = #test_dir_structure{
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"file1">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)]}
                        ]}
                    ]}
                }
            }
        )
    end, WorkersConfigurations).


effective_qos_for_file_in_nested_directories(Config) ->
    Workers = [WorkerP1, WorkerP2, Worker1P3, Worker2P3, Worker3P3] = qos_tests_utils:get_op_nodes_sorted(Config),
    WorkersConfigurations = [
        [WorkerP1, WorkerP2, Worker1P3],
        [Worker1P3, Worker2P3, Worker3P3],
        [WorkerP1, Worker2P3, Worker3P3]
    ],

    lists:foreach(fun(WorkerConf) ->
        ct:pal("Starting for workers: ~p~n", [WorkerConf]),
        DirName = generator:gen_name(),
        Dir1Path = filename:join(?SPACE_PATH1, DirName),
        Dir2Path = filename:join(Dir1Path, <<"dir2">>),
        Dir3Path = filename:join(Dir2Path, <<"dir3">>),
        File21Path = filename:join(Dir2Path, <<"file21">>),
        File31Path = filename:join(Dir3Path, <<"file31">>),

        QosSpec = qos_test_base:effective_qos_for_file_in_nested_directories_spec(
            [Dir1Path, Dir2Path, Dir3Path], [File21Path, File31Path], WorkerConf, Workers, get_provider_to_storage_mappings(Config)
        ),
        add_qos_for_dir_and_check_effective_qos(Config,
            #effective_qos_test_spec{
                initial_dir_structure = #test_dir_structure{
                    worker = WorkerP1,
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"dir2">>, [
                                {<<"file21">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1)]},
                                {<<"dir3">>, [
                                    {<<"file31">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1)]}
                                ]}
                            ]}
                        ]}
                    ]}
                },
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_effective_qos = QosSpec#qos_spec.expected_effective_qos,
                expected_dir_structure = #test_dir_structure{
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"dir2">>, [
                                {<<"file21">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)]},
                                {<<"dir3">>, [
                                    {<<"file31">>, ?TEST_DATA, [
                                        ?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2), ?PROVIDER_ID(Worker1P3)
                                    ]}
                                ]}
                            ]}
                        ]}
                    ]}
                }
            }
        )
    end, WorkersConfigurations).


effective_qos_for_files_in_different_directories_of_tree_structure(Config) ->
    Workers = [WorkerP1, WorkerP2, Worker1P3, Worker2P3, Worker3P3] = qos_tests_utils:get_op_nodes_sorted(Config),
    WorkersConfigurations = [
        [WorkerP1, WorkerP2, Worker1P3],
        [Worker1P3, Worker2P3, Worker3P3],
        [WorkerP1, Worker2P3, Worker3P3]
    ],

    lists:foreach(fun(WorkerConf) ->
        ct:pal("Starting for workers: ~p~n", [WorkerConf]),
        DirName = generator:gen_name(),
        Dir1Path = filename:join(?SPACE_PATH1, DirName),
        Dir2Path = filename:join(Dir1Path, <<"dir2">>),
        Dir3Path = filename:join(Dir1Path, <<"dir3">>),
        File21Path = filename:join(Dir2Path, <<"file21">>),
        File31Path = filename:join(Dir3Path, <<"file31">>),
        QosSpec = qos_test_base:effective_qos_for_files_in_different_directories_of_tree_structure_spec(
            [Dir1Path, Dir2Path, Dir3Path], [File21Path, File31Path], WorkerConf, Workers, get_provider_to_storage_mappings(Config)
        ),

        add_qos_for_dir_and_check_effective_qos(Config,
            #effective_qos_test_spec{
                initial_dir_structure = #test_dir_structure{
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"dir2">>, [{<<"file21">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1)]}]},
                            {<<"dir3">>, [{<<"file31">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1)]}]}
                        ]}
                    ]}
                },
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_effective_qos = QosSpec#qos_spec.expected_effective_qos,
                expected_dir_structure = #test_dir_structure{
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"dir2">>, [{<<"file21">>, ?TEST_DATA, [
                                ?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)]}
                            ]},
                            {<<"dir3">>, [{<<"file31">>, ?TEST_DATA, [
                                ?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(Worker1P3)]}
                            ]}
                        ]}
                    ]}
                }
            }
        )
    end, WorkersConfigurations).


%%%===================================================================
%%% QoS reconciliation tests
%%%===================================================================

qos_reconciliation_file_test(Config) ->
    basic_qos_restoration_test_base(Config, simple).

qos_reconciliation_dir_test(Config) ->
    basic_qos_restoration_test_base(Config, nested).


reconcile_qos_using_file_meta_posthooks_test(Config) ->
    [Worker1, Worker2 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    DirName = <<"dir1">>,
    DirPath = filename:join(?SPACE_PATH1, DirName),
    FilePath = filename:join(DirPath, <<"file1">>),

    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            worker = Worker1,
            dir_structure = {?SPACE_PATH1, [
                {DirName, []}
            ]}
        },
        qos_to_add = [
            #qos_to_add{
                worker = Worker1,
                qos_name = ?QOS1,
                path = DirPath,
                expression = <<"providerId=", (?GET_DOMAIN_BIN(Worker2))/binary>>
            }
        ]
    },

    {_, _} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),

    mock_dbsync_changes(Worker2, ?FUNCTION_NAME),
    mock_file_meta_posthooks(Config),

    Guid = qos_tests_utils:create_file(Worker1, SessId, FilePath, <<"test_data">>),

    DirStructureBefore = get_expected_structure_for_single_dir([?PROVIDER_ID(Worker1)]),
    DirStructureBefore2 = DirStructureBefore#test_dir_structure{assertion_workers = [Worker1]},
    ?assert(qos_tests_utils:assert_distribution_in_dir_structure(
        Config, DirStructureBefore2, #{files => [{Guid, FilePath}], dirs => []}
    )),
    
    Filters = [{file_meta, file_id:guid_to_uuid(Guid)}],
    ensure_docs_received(?FUNCTION_NAME, Filters),
    unmock_dbsync_changes(Worker2),
    save_not_matching_docs(Worker2, ?FUNCTION_NAME, Filters),
    receive
        post_hook_created ->
            unmock_file_meta_posthooks(Config)
    end,
    save_matching_docs(Worker2, ?FUNCTION_NAME, Filters),

    DirStructureAfter = get_expected_structure_for_single_dir([?PROVIDER_ID(Worker1), ?PROVIDER_ID(Worker2)]),
    ?assert(qos_tests_utils:assert_distribution_in_dir_structure(Config, DirStructureAfter,  #{files => [{Guid, FilePath}], dirs => []})).


reconcile_with_links_race_test(Config) ->
    reconcile_with_links_race_test_base(Config, 0, [qos_entry, links_forest]).


reconcile_with_links_race_nested_file_test(Config) ->
    reconcile_with_links_race_test_base(Config, 8, [qos_entry, links_forest]).


reconcile_with_links_and_file_meta_race_nested_file_test(Config) ->
    reconcile_with_links_race_test_base(Config, 8, [qos_entry, file_meta, links_forest]).


reconcile_with_links_race_test_base(Config, Depth, RecordsToBlock) ->
    [Worker1, Worker2 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_ID),
    
    Name = generator:gen_name(),
    
    {ok, DirGuid} = lfm_proxy:mkdir(Worker1, SessId(Worker1), SpaceGuid, ?filename(Name, 0), ?DEFAULT_DIR_PERMS),
    % ensure that link in space is synchronized - in env_up tests space uuid is a legacy key and therefore file_meta posthooks are NOT executed for it
    ?assertMatch({ok, _}, rpc:call(Worker2, file_meta_forest, get, [file_id:guid_to_uuid(SpaceGuid), all, ?filename(Name, 0)]), ?ATTEMPTS),
    
    mock_dbsync_changes(Worker2, ?FUNCTION_NAME),
    {ok, QosEntryId} = lfm_proxy:add_qos_entry(Worker1, SessId(Worker1), ?FILE_REF(DirGuid), <<"providerId=", (?GET_DOMAIN_BIN(Worker2))/binary>>, 1),
    
    
    ParentGuid = lists:foldl(fun(_, TmpParentGuid) ->
        {ok, G} = lfm_proxy:mkdir(Worker1, SessId(Worker1), TmpParentGuid, ?filename(Name, 0), ?DEFAULT_DIR_PERMS),
        G
    end, DirGuid, lists:seq(1, Depth)),
    Guid = create_file_with_content(Worker1, SessId(Worker1), ParentGuid, ?filename(Name, 0)),
    
    ?assertMatch({ok, {Map, _}} when map_size(Map) =/= 0,
        lfm_proxy:get_effective_file_qos(Worker1, SessId(Worker1), ?FILE_REF(DirGuid)),
        ?ATTEMPTS),
    
    Size = size(?TEST_DATA),
    ExpectedDistributionFun = fun(List) -> 
        Distribution = lists:map(fun({W, TotalBlocksSize}) ->
            #{
                <<"providerId">> => ?GET_DOMAIN_BIN(W),
                <<"totalBlocksSize">> => TotalBlocksSize,
                <<"blocks">> => [[0, TotalBlocksSize]]
            }
        end, List),
        lists:sort(fun(#{<<"providerId">> := ProviderIdA}, #{<<"providerId">> := ProviderIdB}) ->
            ProviderIdA =< ProviderIdB
        end, Distribution)
    end,
    
    CheckDistributionFun = fun(ExpectedWorkersDistribution) ->
        lists:foreach(fun(Worker) ->
            ?assertEqual({ok, ExpectedDistributionFun(ExpectedWorkersDistribution)},
                lfm_proxy:get_file_distribution(Worker, SessId(Worker), ?FILE_REF(Guid)), ?ATTEMPTS
            )
        end, Workers)
    end,
    
    LinksKey = datastore_model:get_unique_key(file_meta, file_id:guid_to_uuid(DirGuid)),
    Filters = lists:map(fun
        (qos_entry) -> {qos_entry, QosEntryId};
        (file_meta) -> {file_meta, file_id:guid_to_uuid(ParentGuid)};
        (links_forest) -> {links, links_forest, LinksKey}
    end, RecordsToBlock),
    
    ensure_docs_received(?FUNCTION_NAME, Filters),
    unmock_dbsync_changes(Worker2),
    % mock fslogic_authz:ensure_authorized/3 to bypass its fail on non existing 
    % ancestor file meta document when checking file distribution
    mock_fslogic_authz_ensure_authorized(Worker2),
    save_not_matching_docs(Worker2, ?FUNCTION_NAME, Filters),
    
    CheckDistributionFun([{Worker1, Size}]),
    
    save_matching_docs(Worker2, ?FUNCTION_NAME, [{qos_entry, QosEntryId}]),
    lists:foreach(fun(Worker) ->
        ?assertEqual({ok, ?FULFILLED_QOS_STATUS}, lfm_proxy:check_qos_status(Worker, SessId(Worker), QosEntryId), ?ATTEMPTS)
    end, Workers),
    CheckDistributionFun([{Worker1, Size}]),
    
    save_matching_docs(Worker2, ?FUNCTION_NAME, [{links, links_forest, LinksKey}]),
    case lists:member(file_meta, RecordsToBlock) of
        false ->
            CheckDistributionFun([{Worker1, Size}, {Worker2, Size}]);
        true ->
            CheckDistributionFun([{Worker1, Size}]),
            save_matching_docs(Worker2, ?FUNCTION_NAME, [{file_meta, file_id:guid_to_uuid(ParentGuid)}]),
            CheckDistributionFun([{Worker1, Size}, {Worker2, Size}])
    end.
    

%%%===================================================================
%%% QoS reevaluate
%%%===================================================================

reevaluate_impossible_qos_test(Config) ->
    [Worker1, Worker2, _Worker3 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),

    DirName = <<"dir1">>,
    DirPath = filename:join(?SPACE_PATH1, DirName),
    FileName = <<"file1">>,

    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            worker = Worker2,
            dir_structure = {?SPACE_PATH1, [
                {DirName, [{FileName, ?TEST_DATA, [?PROVIDER_ID(Worker2)]}]}
            ]}
        },
        qos_to_add = [
            #qos_to_add{
                worker = Worker1,
                qos_name = ?QOS1,
                path = DirPath,
                expression = <<"country=PL">>,
                replicas_num = 2
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = Workers,
                qos_name = ?QOS1,
                file_key = {path, DirPath},
                replicas_num = 2,
                qos_expression = [<<"country=PL">>],
                possibility_check = {impossible, ?PROVIDER_ID(Worker1)}
            }
        ],
        wait_for_qos_fulfillment = []
    },

    {_GuidsAndPaths, QosNameIdMapping} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),

    ok = qos_tests_utils:set_qos_parameters(Worker2, #{<<"country">> => <<"PL">>}),
    % Impossible qos reevaluation is called after successful set_qos_parameters

    ExpectedQosEntriesAfter = [
        #expected_qos_entry{
            workers = Workers,
            qos_name = ?QOS1,
            file_key = {path, DirPath},
            replicas_num = 2,
            qos_expression = [<<"country=PL">>],
            possibility_check = {possible, ?PROVIDER_ID(Worker2)}
        }
    ],
    qos_tests_utils:wait_for_qos_fulfillment_in_parallel(Config, undefined, QosNameIdMapping, ExpectedQosEntriesAfter),

    ExpectedFileQos = [
        #expected_file_qos{
            workers = Workers,
            path = DirPath,
            qos_entries = [?QOS1],
            assigned_entries = #{
                initializer:get_storage_id(Worker1) => [?QOS1],
                initializer:get_storage_id(Worker2) => [?QOS1]
            }
        }
    ],
    qos_tests_utils:assert_file_qos_documents(Config, ExpectedFileQos, QosNameIdMapping, true, 10).


reevaluate_impossible_qos_race_test(Config) ->
    % this test checks appropriate handling of situation, when provider, on which entry was created,
    % do not have full knowledge of remote QoS parameters
    [Worker1, Worker2, _Worker3 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),

    DirName = <<"dir1">>,
    DirPath = filename:join(?SPACE_PATH1, DirName),
    FileName = <<"file1">>,

    ok = qos_tests_utils:set_qos_parameters(Worker2, #{<<"country">> => <<"other">>}),

    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            worker = Worker2,
            dir_structure = {?SPACE_PATH1, [
                {DirName, [{FileName, ?TEST_DATA, [?PROVIDER_ID(Worker2)]}]}
            ]}
        },
        qos_to_add = [
            #qos_to_add{
                worker = Worker1,
                qos_name = ?QOS1,
                path = DirPath,
                expression = <<"country=other">>
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = Workers,
                qos_name = ?QOS1,
                file_key = {path, DirPath},
                replicas_num = 1,
                qos_expression = [<<"country=other">>],
                possibility_check = {possible, ?PROVIDER_ID(Worker2)}
            }
        ],
        wait_for_qos_fulfillment = []
    },

    {_GuidsAndPaths, QosNameIdMapping} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),

    ExpectedFileQos = [
        #expected_file_qos{
            workers = Workers,
            path = DirPath,
            qos_entries = [?QOS1],
            assigned_entries = #{initializer:get_storage_id(Worker2) => [?QOS1]}
        }
    ],
    qos_tests_utils:assert_file_qos_documents(Config, ExpectedFileQos, QosNameIdMapping, true, 10).


reevaluate_impossible_qos_conflict_test(Config) ->
    % this test checks conflict resolution, when multiple providers mark entry as possible
    [Worker1, Worker2, _Worker3 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),

    DirName = <<"dir1">>,
    DirPath = filename:join(?SPACE_PATH1, DirName),
    FileName = <<"file1">>,

    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            worker = Worker2,
            dir_structure = {?SPACE_PATH1, [
                {DirName, [{FileName, ?TEST_DATA, [?PROVIDER_ID(Worker2)]}]}
            ]}
        },
        qos_to_add = [
            #qos_to_add{
                worker = Worker1,
                qos_name = ?QOS1,
                path = DirPath,
                expression = <<"country=other">>
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = Workers,
                qos_name = ?QOS1,
                file_key = {path, DirPath},
                replicas_num = 1,
                qos_expression = [<<"country=other">>],
                possibility_check = {impossible, ?PROVIDER_ID(Worker1)}
            }
        ],
        wait_for_qos_fulfillment = []
    },

    {_GuidsAndPaths, QosNameIdMapping} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),

    lists_utils:pforeach(fun(Worker) ->
        ok = qos_tests_utils:set_qos_parameters(Worker, #{<<"country">> => <<"other">>})
        % Impossible qos reevaluation is called after successful set_qos_parameters
    end, Workers),

    ExpectedQosEntriesAfter = [
        #expected_qos_entry{
            workers = Workers,
            qos_name = ?QOS1,
            file_key = {path, DirPath},
            replicas_num = 1,
            qos_expression = [<<"country=other">>],
            possibility_check = {possible, ?PROVIDER_ID(Worker1)}
        }
    ],
    qos_tests_utils:wait_for_qos_fulfillment_in_parallel(Config, undefined, QosNameIdMapping, ExpectedQosEntriesAfter),

    ExpectedFileQos = [
        #expected_file_qos{
            workers = Workers,
            path = DirPath,
            qos_entries = [?QOS1],
            assigned_entries = #{initializer:get_storage_id(Worker1) => [?QOS1]}
        }
    ],
    qos_tests_utils:assert_file_qos_documents(Config, ExpectedFileQos, QosNameIdMapping, true, 40).


%%%===================================================================
%%% QoS traverse tests
%%%===================================================================

qos_traverse_cancellation_test(Config) ->
    [Worker1, Worker2 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    Name = generator:gen_name(),
    QosRootFilePath = filename:join([<<"/">>, ?SPACE_ID, Name]),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    
    DirStructure =
        {?SPACE_ID, [
            {Name, % Dir1
                [
                    {?filename(Name, 0), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]},
                    {?filename(Name, 1), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]},
                    {?filename(Name, 2), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]},
                    {?filename(Name, 3), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]}
                ]
            }
        ]},
    
    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = DirStructure
        },
        qos_to_add = [
            #qos_to_add{
                worker = Worker1,
                qos_name = ?QOS1,
                path = QosRootFilePath,
                expression = <<"country=PL">>
            }
        ],
        % do not wait for QoS fulfillment
        wait_for_qos_fulfillment = [],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = Workers,
                qos_name = ?QOS1,
                file_key = {path, QosRootFilePath},
                qos_expression = [<<"country=PL">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(Worker1)}
            }
        ]
    },
    
    {GuidsAndPaths, QosNameIdMapping} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),
    [QosEntryId] = maps:values(QosNameIdMapping),
    
    DirGuid = qos_tests_utils:get_guid(QosRootFilePath, GuidsAndPaths),
    
    % create file and write to it on remote provider to trigger reconcile transfer
    {ok, {FileGuid, FileHandle}} = lfm_proxy:create_and_open(Worker2, SessId(Worker2), DirGuid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, _} = lfm_proxy:write(Worker2, FileHandle, 0, <<"new_data">>),
    ok = lfm_proxy:close(Worker2, FileHandle),
    
    % wait for reconciliation transfer to start
    receive {qos_slave_job, _Pid, FileGuid} = Msg ->
        self() ! Msg
    end,
    
    % remove entry to trigger transfer cancellation
    lfm_proxy:remove_qos_entry(Worker1, SessId(Worker1), QosEntryId),
    
    % check that 5 transfers were cancelled (4 from traverse and 1 reconciliation)
    test_utils:mock_assert_num_calls(Worker1, replica_synchronizer, cancel, 1, 5, ?ATTEMPTS),
    
    % check that qos_entry document is deleted
    lists:foreach(fun(Worker) ->
        ?assertEqual(?ERROR_NOT_FOUND, rpc:call(Worker, qos_entry, get, [QosEntryId]), ?ATTEMPTS)
    end, Workers),
    
    % finish transfers to unlock waiting slave job processes
    ok = qos_tests_utils:finish_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)] ++ [FileGuid]).


%%%===================================================================
%%% QoS with hardlinks tests
%%%===================================================================

qos_on_hardlink_test(Config) ->
    qos_test_base:qos_with_hardlink_test_base(Config, ?SPACE_ID, direct).

effective_qos_with_hardlinks_test(Config) ->
    qos_test_base:qos_with_hardlink_test_base(Config, ?SPACE_ID, effective).

qos_with_inode_deletion_test(Config) ->
    qos_test_base:qos_with_hardlink_deletion_test_base(Config, ?SPACE_ID, inode).

qos_with_hardlink_deletion_test(Config) ->
    qos_test_base:qos_with_hardlink_deletion_test_base(Config, ?SPACE_ID, hardlink).

qos_with_mixed_deletion_test(Config) ->
    qos_test_base:qos_with_hardlink_deletion_test_base(Config, ?SPACE_ID, mixed).

qos_on_symlink_test(Config) ->
    qos_test_base:qos_on_symlink_test_base(Config, ?SPACE_ID).

effective_qos_with_symlink_test(Config) ->
    qos_test_base:effective_qos_with_symlink_test_base(Config, ?SPACE_ID).

create_hardlink_in_dir_with_qos(Config) ->
    qos_test_base:create_hardlink_in_dir_with_qos(Config, ?SPACE_ID).


%%%===================================================================
%%% QoS transfer stats tests
%%%===================================================================
    
qos_transfer_stats_test(Config) ->
    [Worker1, Worker2, Worker3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    Name = generator:gen_name(),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    Guid = create_file_with_content(Worker1, SessId(Worker1), fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_ID), Name),
    ProviderId2 = ?GET_DOMAIN_BIN(Worker2),
    {ok, QosEntryId} = lfm_proxy:add_qos_entry(Worker1, SessId(Worker1), ?FILE_REF(Guid), <<"providerId=", ProviderId2/binary>>, 1),
    % wait for qos entries to be dbsynced to other provider
    ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(Worker2, SessId(Worker2), QosEntryId), ?ATTEMPTS),
    ?assertEqual({ok, ?FULFILLED_QOS_STATUS}, lfm_proxy:check_qos_status(Worker1, SessId(Worker1), QosEntryId), ?ATTEMPTS),
    
    check_transfer_stats(Worker1, QosEntryId, bytes, [<<"total">>], empty),
    check_transfer_stats(Worker2, QosEntryId, bytes, [<<"total">>, <<"mntst1">>], {1, byte_size(?TEST_DATA)}),
    check_transfer_stats(Worker1, QosEntryId, files, [<<"total">>], empty),
    check_transfer_stats(Worker2, QosEntryId, files, [<<"total">>, <<"mntst2">>], {1, 1}),
    
    {ok, HW3} = lfm_proxy:open(Worker3, SessId(Worker3), #file_ref{guid = Guid}, write),
    NewData = crypto:strong_rand_bytes(8),
    {ok, _} = lfm_proxy:write(Worker3, HW3, 0, NewData),
    ok = lfm_proxy:close(Worker3, HW3),
    
    {ok, HW2} = lfm_proxy:open(Worker2, SessId(Worker2), #file_ref{guid = Guid}, read),
    ?assertEqual({ok, NewData}, lfm_proxy:read(Worker2, HW2, 0, byte_size(NewData)), ?ATTEMPTS),
    ok = lfm_proxy:close(Worker2, HW2),
    ?assertEqual({ok, ?FULFILLED_QOS_STATUS}, lfm_proxy:check_qos_status(Worker2, SessId(Worker2), QosEntryId), ?ATTEMPTS),
    
    check_transfer_stats(Worker1, QosEntryId, bytes, [<<"total">>], empty),
    check_transfer_stats(Worker2, QosEntryId, bytes, [<<"mntst1">>], {1, byte_size(?TEST_DATA)}),
    check_transfer_stats(Worker2, QosEntryId, bytes, [<<"mntst3">>], {1, byte_size(NewData)}),
    check_transfer_stats(Worker2, QosEntryId, bytes, [<<"total">>], {2, byte_size(NewData) + byte_size(?TEST_DATA)}),
    check_transfer_stats(Worker1, QosEntryId, files, [<<"total">>], empty),
    check_transfer_stats(Worker2, QosEntryId, files, [<<"total">>, <<"mntst2">>], {2, 2}).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    qos_test_base:init_per_suite(Config).


end_per_suite(Config) ->
    qos_test_base:end_per_suite(Config).


init_per_testcase(qos_traverse_cancellation_test , Config) ->
    Workers = ?config(op_worker_nodes, Config),
    qos_tests_utils:mock_transfers(Workers),
    init_per_testcase(default, Config);
init_per_testcase(_, Config) ->
    qos_test_base:init_per_testcase(Config).


end_per_testcase(_, Config) ->
    qos_test_base:end_per_testcase(Config).

%%%===================================================================
%%% Test bases
%%%===================================================================

add_qos_for_dir_and_check_effective_qos(Config, TestSpec) ->
    #effective_qos_test_spec{
        initial_dir_structure = InitialDirStructure,
        qos_to_add = QosToAddList,
        wait_for_qos_fulfillment = WaitForQosFulfillment,
        expected_qos_entries = ExpectedQosEntries,
        expected_effective_qos = ExpectedEffectiveQos
    } = TestSpec,

    % create initial dir structure
    GuidsAndPaths = qos_tests_utils:create_dir_structure(Config, InitialDirStructure),
    ?assertMatch(true, qos_tests_utils:assert_distribution_in_dir_structure(Config, InitialDirStructure, GuidsAndPaths)),

    % add QoS and wait for fulfillment
    QosNameIdMapping = qos_tests_utils:add_multiple_qos(Config, QosToAddList),
    qos_tests_utils:wait_for_qos_fulfillment_in_parallel(Config, WaitForQosFulfillment, QosNameIdMapping, ExpectedQosEntries),

    % check documents
    qos_tests_utils:assert_qos_entry_documents(Config, ExpectedQosEntries, QosNameIdMapping, ?ATTEMPTS),
    qos_tests_utils:assert_effective_qos(Config, ExpectedEffectiveQos, QosNameIdMapping, true).


basic_qos_restoration_test_base(Config, DirStructureType) ->
    [Worker1, _Worker2, Worker3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    SessionId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),

    Filename = generator:gen_name(),
    QosSpec = create_basic_qos_test_spec(Config, DirStructureType, Filename, undefined),
    {GuidsAndPaths, _} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),
    NewData = <<"new_test_data">>,
    StoragePaths = lists:map(fun({Guid, Path}) ->
        SpaceId = file_id:guid_to_space_id(Guid),
        % remove leading slash and space id
        [_, _ | PathTokens] = binary:split(Path, <<"/">>, [global]),
        StoragePath = storage_file_path(Worker3, SpaceId, filename:join(PathTokens)),
        ?assertEqual({ok, ?TEST_DATA}, read_file(Worker3, StoragePath)),
        {ok, FileHandle} = lfm_proxy:open(Worker1, SessionId1, ?FILE_REF(Guid), write),
        {ok, _} = lfm_proxy:write(Worker1, FileHandle, 0, NewData),
        ok = lfm_proxy:close(Worker1, FileHandle),
        StoragePath
    end, maps:get(files, GuidsAndPaths)),
    lists:foreach(fun(StoragePath) ->
        ?assertEqual({ok, NewData}, read_file(Worker3, StoragePath), ?ATTEMPTS)
    end, StoragePaths).


create_basic_qos_test_spec(Config, DirStructureType, QosFilename, WaitForQos) ->
    [Worker1, _Worker2, Worker3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    {DirStructure, DirStructureAfter} = case DirStructureType of
        simple ->
            {?simple_dir_structure(?SPACE_ID, QosFilename, [?GET_DOMAIN_BIN(Worker1)]),
                ?simple_dir_structure(?SPACE_ID, QosFilename, [?GET_DOMAIN_BIN(Worker1), ?GET_DOMAIN_BIN(Worker3)])};
        nested ->
            {?nested_dir_structure(?SPACE_ID, QosFilename, [?GET_DOMAIN_BIN(Worker1)]),
                ?nested_dir_structure(?SPACE_ID, QosFilename, [?GET_DOMAIN_BIN(Worker1), ?GET_DOMAIN_BIN(Worker3)])}
    end,

    #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = DirStructure
        },
        qos_to_add = [
            #qos_to_add{
                worker = Worker1,
                qos_name = ?QOS1,
                path = ?FILE_PATH(QosFilename),
                expression = <<"country=PT">>
            }
        ],
        wait_for_qos_fulfillment = WaitForQos,
        expected_qos_entries = [
            #expected_qos_entry{
                qos_name = ?QOS1,
                file_key = {path, ?FILE_PATH(QosFilename)},
                qos_expression = [<<"country=PT">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(Worker1)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                path = ?FILE_PATH(QosFilename),
                qos_entries = [?QOS1],
                assigned_entries = #{
                    initializer:get_storage_id(Worker3) => [?QOS1]
                }
            }
        ],
        expected_dir_structure = #test_dir_structure{
            dir_structure = DirStructureAfter
        }
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================

get_expected_structure_for_single_dir(ProviderIdList) ->
    #test_dir_structure{
        dir_structure = {?SPACE_ID, [
            {<<"dir1">>, [
                {<<"file1">>, <<"test_data">>, ProviderIdList}
            ]}
        ]}
    }.


storage_file_path(Worker, SpaceId, FilePath) ->
    SpaceMnt = get_space_mount_point(Worker, SpaceId),
    filename:join([SpaceMnt, SpaceId, FilePath]).


get_space_mount_point(Worker, SpaceId) ->
    StorageId = initializer:get_supporting_storage_id(Worker, SpaceId),
    storage_mount_point(Worker, StorageId).


storage_mount_point(Worker, StorageId) ->
    Helper = rpc:call(Worker, storage, get_helper, [StorageId]),
    HelperArgs = helper:get_args(Helper),
    maps:get(<<"mountPoint">>, HelperArgs).


read_file(Worker, FilePath) ->
    rpc:call(Worker, file, read_file, [FilePath]).


get_provider_to_storage_mappings(Config) ->
    [WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    #{
        ?P1 => initializer:get_storage_id(WorkerP1),
        ?P2 => initializer:get_storage_id(WorkerP2),
        ?P3 => initializer:get_storage_id(WorkerP3)
    }.


mock_file_meta_posthooks(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, file_meta_posthooks, [passthrough]),
    TestPid = self(),
    ok = test_utils:mock_expect(Workers, file_meta_posthooks, add_hook,
        fun(FileUuid, Identifier, Module, Function, Args) ->
            Res = meck:passthrough([FileUuid, Identifier, Module, Function, Args]),
            TestPid ! post_hook_created,
            Res
        end).


unmock_file_meta_posthooks(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, file_meta_posthooks).


create_file_with_content(Worker, SessId, ParentGuid, Name) ->
    {ok, {G, H1}} = lfm_proxy:create_and_open(Worker, SessId, ParentGuid, Name, ?DEFAULT_FILE_PERMS),
    {ok, _} = lfm_proxy:write(Worker, H1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(Worker, H1),
    G.


mock_fslogic_authz_ensure_authorized(Worker) ->
    test_utils:mock_new(Worker, fslogic_authz, [passthrough]),
    ok = test_utils:mock_expect(Worker, fslogic_authz, ensure_authorized,
        fun(_, FileCtx, _) ->
            FileCtx
        end).


check_transfer_stats(Worker, QosEntryId, Type, ExpectedSeries, ExpectedValue) ->
    {ok, Stats} = rpc:call(Worker, qos_transfer_stats, get, [QosEntryId, Type]),
    lists:foreach(fun(Series) ->
        lists:foreach(fun(Metric) ->
            ?assert(maps:is_key({Series, Metric}, Stats)),
            Value = maps:get({Series, Metric}, Stats),
            case ExpectedValue of
                empty ->
                    ?assertEqual([], Value);
                _ ->
                    [{_Timestamp, Value1}] = Value,
                    ?assertEqual(ExpectedValue, Value1)
            end
        end, [<<"minute">>, <<"hour">>, <<"day">>, <<"month">>])
    end, ExpectedSeries).

%%%===================================================================
%%% DBSync mocks
%%%===================================================================

mock_dbsync_changes(Worker, MsgIdentifier) ->
    test_utils:mock_new(Worker, dbsync_changes, [passthrough]),
    TestPid = self(),

    ok = test_utils:mock_expect(Worker, dbsync_changes, apply,
        fun(#document{value = Value} = Doc) ->
            RecordType = element(1, Value),
            TestPid ! {MsgIdentifier, RecordType, Doc},
            ok
        end).


unmock_dbsync_changes(Worker) ->
    test_utils:mock_unload(Worker, dbsync_changes).


save_matching_docs(Worker, MsgIdentifier, Filters) ->
    save_docs(Worker, MsgIdentifier, Filters, matching).


save_not_matching_docs(Worker, MsgIdentifier, Filters) ->
    save_docs(Worker, MsgIdentifier, Filters, not_matching).


save_docs(Worker, MsgIdentifier, Filters, Strategy) ->
    LeftOutDocs = save_docs(Worker, MsgIdentifier, Filters, [], Strategy),
    lists:foreach(fun(#document{value = Value} = Doc) ->
        RecordType = element(1, Value),
        self() ! {MsgIdentifier, RecordType, Doc}
    end, LeftOutDocs).


save_docs(Worker, MsgIdentifier, Filters, LeftOutDocs, Strategy) ->
    ExpectedMatch = Strategy == matching,
    receive
        {MsgIdentifier, _, Doc} ->
            case matches_doc(Doc, Filters) of
                ExpectedMatch ->
                    ?assertMatch(ok, rpc:call(Worker, dbsync_changes, apply, [Doc])),
                    save_docs(Worker, MsgIdentifier, Filters, LeftOutDocs, Strategy);
                _ ->
                    save_docs(Worker, MsgIdentifier, Filters, [Doc | LeftOutDocs], Strategy)
            end
    after 0 ->
        LeftOutDocs
    end.


matches_doc(#document{key = Key, value = Value}, Filters) ->
    RecordType = element(1, Value),
    lists:any(fun
        (R) when R =:= RecordType ->
            true;
        ({R, K}) when R =:= RecordType andalso K =:= Key ->
            true;
        ({links, R, LinksKey}) when R =:= RecordType -> 
            matches_link_doc(Value, LinksKey);
        (_) ->
            false
    end, Filters).


matches_link_doc(#links_node{key = Key}, Key) -> true;
matches_link_doc(#links_forest{key = Key}, Key) -> true;
matches_link_doc(#links_mask{key = Key}, Key) -> true;
matches_link_doc(_, _) -> false.


ensure_docs_received(_MsgIdentifier, []) -> ok;
ensure_docs_received(MsgIdentifier, [{RecordType, _} | Tail]) ->
    wait_for_doc(MsgIdentifier, RecordType),
    ensure_docs_received(MsgIdentifier, Tail);
ensure_docs_received(MsgIdentifier, [{links, RecordType, _} | Tail]) ->
    wait_for_doc(MsgIdentifier, RecordType),
    ensure_docs_received(MsgIdentifier, Tail).


wait_for_doc(MsgIdentifier, RecordType) ->
    receive {MsgIdentifier, RecordType, _} = Msg ->
        self() ! Msg
    end.
