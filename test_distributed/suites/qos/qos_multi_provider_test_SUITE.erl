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
-module(qos_multi_provider_test_SUITE).
-author("Michal Cwiertnia").
-author("Michal Stanisz").

-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("qos_tests_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

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
    create_hardlink_in_dir_with_qos/1
]).

all() -> [
    effective_qos_for_file_in_directory,
    effective_qos_for_file_in_nested_directories,
    effective_qos_for_files_in_different_directories_of_tree_structure,

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
    create_hardlink_in_dir_with_qos
].


-define(FILE_PATH(FileName), filename:join([?SPACE_PATH1, FileName])).

-define(SPACE_PLACEHOLDER, space1).
-define(SPACE_NAME, <<"space1">>).
-define(SPACE_PATH1, <<"/space1">>).

-define(USER_PLACEHOLDER, user2).
-define(SESS_ID(ProviderPlaceholder), oct_background:get_user_session_id(?USER_PLACEHOLDER, ProviderPlaceholder)).

-define(ATTEMPTS, 60).

%%%====================================================================
%%% Test functions
%%%====================================================================

%%%===================================================================
%%% Group of tests that creates directory structure, adds QoS on different
%%% levels of created structure and checks effective QoS and QoS docs.
%%%===================================================================

effective_qos_for_file_in_directory(_Config) ->
    [Provider1, Provider2 | _] = Providers = oct_background:get_provider_ids(),

    lists:foreach(fun(ProviderAddingQos) ->
        ct:pal("Starting for provider: ~p~n", [ProviderAddingQos]),
        DirName = generator:gen_name(),
        DirPath = filename:join(?SPACE_PATH1, DirName),
        FilePath = filename:join(DirPath, <<"file1">>),

        QosSpec = qos_test_base:effective_qos_for_file_in_directory_spec(DirPath, FilePath, ProviderAddingQos, Providers),
        add_qos_for_dir_and_check_effective_qos(
            #effective_qos_test_spec{
                initial_dir_structure = #test_dir_structure{
                    provider = Provider1,
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"file1">>, ?TEST_DATA, [Provider1]}
                        ]}
                    ]}
                },
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_effective_qos = QosSpec#qos_spec.expected_effective_qos,
                expected_dir_structure = #test_dir_structure{
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"file1">>, ?TEST_DATA, [Provider1, Provider2]}
                        ]}
                    ]}
                }
            }
        )
    end, Providers).


effective_qos_for_file_in_nested_directories(_Config) ->
    [Provider1, Provider2 | _] = Providers = oct_background:get_provider_ids(),
    Configurations = [
        [Provider1, Provider2, Provider2],
        [Provider2, Provider1, Provider1],
        [Provider2, Provider2, Provider2]
    ],

    lists:foreach(fun(ProvidersAddingQos) ->
        ct:pal("Starting for providers: ~p~n", [ProvidersAddingQos]),
        DirName = generator:gen_name(),
        Dir1Path = filename:join(?SPACE_PATH1, DirName),
        Dir2Path = filename:join(Dir1Path, <<"dir2">>),
        Dir3Path = filename:join(Dir2Path, <<"dir3">>),
        File21Path = filename:join(Dir2Path, <<"file21">>),
        File31Path = filename:join(Dir3Path, <<"file31">>),

        QosSpec = qos_test_base:effective_qos_for_file_in_nested_directories_spec(
            [Dir1Path, Dir2Path, Dir3Path], [File21Path, File31Path], ProvidersAddingQos, Providers
        ),
        add_qos_for_dir_and_check_effective_qos(
            #effective_qos_test_spec{
                initial_dir_structure = #test_dir_structure{
                    provider = Provider1,
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"dir2">>, [
                                {<<"file21">>, ?TEST_DATA, [Provider1]},
                                {<<"dir3">>, [
                                    {<<"file31">>, ?TEST_DATA, [Provider1]}
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
                                {<<"file21">>, ?TEST_DATA, [Provider1]},
                                {<<"dir3">>, [
                                    {<<"file31">>, ?TEST_DATA, [Provider1, Provider2]}
                                ]}
                            ]}
                        ]}
                    ]}
                }
            }
        )
    end, Configurations).


effective_qos_for_files_in_different_directories_of_tree_structure(_Config) ->
    [Provider1, Provider2 | _] = Providers = oct_background:get_provider_ids(),
    Configuration = [
        [Provider1, Provider2, Provider2],
        [Provider2, Provider1, Provider1],
        [Provider2, Provider2, Provider2]
    ],

    lists:foreach(fun(WorkerConf) ->
        ct:pal("Starting for providers: ~p~n", [WorkerConf]),
        DirName = generator:gen_name(),
        Dir1Path = filename:join(?SPACE_PATH1, DirName),
        Dir2Path = filename:join(Dir1Path, <<"dir2">>),
        Dir3Path = filename:join(Dir1Path, <<"dir3">>),
        File21Path = filename:join(Dir2Path, <<"file21">>),
        File31Path = filename:join(Dir3Path, <<"file31">>),
        QosSpec = qos_test_base:effective_qos_for_files_in_different_directories_of_tree_structure_spec(
            [Dir1Path, Dir2Path, Dir3Path], [File21Path, File31Path], WorkerConf, Providers
        ),

        add_qos_for_dir_and_check_effective_qos(
            #effective_qos_test_spec{
                initial_dir_structure = #test_dir_structure{
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"dir2">>, [{<<"file21">>, ?TEST_DATA, [Provider1]}]},
                            {<<"dir3">>, [{<<"file31">>, ?TEST_DATA, [Provider1]}]}
                        ]}
                    ]}
                },
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_effective_qos = QosSpec#qos_spec.expected_effective_qos,
                expected_dir_structure = #test_dir_structure{
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"dir2">>, [{<<"file21">>, ?TEST_DATA, [Provider1]}]},
                            {<<"dir3">>, [{<<"file31">>, ?TEST_DATA, [Provider1, Provider2]}]}
                        ]}
                    ]}
                }
            }
        )
    end, Configuration).


%%%===================================================================
%%% QoS reconciliation tests
%%%===================================================================


reconcile_qos_using_file_meta_posthooks_test(_Config) ->
    [Provider1, Provider2 | _] = oct_background:get_provider_ids(),
    SessId = ?SESS_ID(Provider1),
    DirName = <<"dir1">>,
    DirPath = filename:join(?SPACE_PATH1, DirName),
    FilePath = filename:join(DirPath, <<"file1">>),

    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            provider = Provider1,
            dir_structure = {?SPACE_PATH1, [
                {DirName, []}
            ]}
        },
        qos_to_add = [
            #qos_to_add{
                provider_selector = Provider1,
                qos_name = ?QOS1,
                path = DirPath,
                expression = <<"providerId=", Provider2/binary>>
            }
        ]
    },

    {_, _} = qos_tests_utils:fulfill_qos_test_base(QosSpec),

    mock_dbsync_changes(oct_background:get_provider_nodes(Provider2), ?FUNCTION_NAME),
    mock_file_meta_posthooks(),

    Guid = qos_tests_utils:create_file(Provider1, SessId, FilePath, <<"test_data">>),

    DirStructureBefore = get_expected_structure_for_single_dir([Provider1]),
    DirStructureBefore2 = DirStructureBefore#test_dir_structure{assertion_providers = [Provider1]},
    ?assert(qos_tests_utils:assert_distribution_in_dir_structure(
        DirStructureBefore2, #{files => [{Guid, FilePath}], dirs => []}
    )),
    
    Filters = [{file_meta, file_id:guid_to_uuid(Guid)}],
    ensure_docs_received(?FUNCTION_NAME, Filters),
    unmock_dbsync_changes(oct_background:get_provider_nodes(Provider2)),
    save_not_matching_docs(oct_background:get_random_provider_node(Provider2), ?FUNCTION_NAME, Filters),
    receive
        post_hook_created ->
            unmock_file_meta_posthooks()
    end,
    save_matching_docs(oct_background:get_random_provider_node(Provider2), ?FUNCTION_NAME, Filters),

    DirStructureAfter = get_expected_structure_for_single_dir([Provider1, Provider2]),
    ?assert(qos_tests_utils:assert_distribution_in_dir_structure(DirStructureAfter,  
        #{files => [{Guid, FilePath}], dirs => []})).


reconcile_with_links_race_test(_Config) ->
    reconcile_with_links_race_test_base(0, [qos_entry, links_forest]).


reconcile_with_links_race_nested_file_test(_Config) ->
    reconcile_with_links_race_test_base(8, [qos_entry, links_forest]).


reconcile_with_links_and_file_meta_race_nested_file_test(_Config) ->
    reconcile_with_links_race_test_base(8, [qos_entry, file_meta, links_forest]).


reconcile_with_links_race_test_base(Depth, RecordsToBlock) ->
    [Provider1, Provider2 | _] = Providers = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    P1Node = oct_background:get_random_provider_node(Provider1),
    P2Node = oct_background:get_random_provider_node(Provider2),
    
    Name = generator:gen_name(),
    
    {ok, DirGuid} = lfm_proxy:mkdir(P1Node, ?SESS_ID(Provider1), SpaceGuid, ?filename(Name, 0), ?DEFAULT_DIR_PERMS),
    % ensure that link in space is synchronized - in env_up tests space uuid is a legacy key and therefore file_meta posthooks are NOT executed for it
    ?assertMatch({ok, _}, test_rpc:call(op_worker, Provider2, file_meta_forest, get, [file_id:guid_to_uuid(SpaceGuid), all, ?filename(Name, 0)]), ?ATTEMPTS),
    
    mock_dbsync_changes(oct_background:get_provider_nodes(Provider2), ?FUNCTION_NAME),
    {ok, QosEntryId} = lfm_proxy:add_qos_entry(P1Node, ?SESS_ID(Provider1), ?FILE_REF(DirGuid), <<"providerId=", Provider2/binary>>, 1),
    
    
    ParentGuid = lists:foldl(fun(_, TmpParentGuid) ->
        {ok, G} = lfm_proxy:mkdir(P1Node, ?SESS_ID(Provider1), TmpParentGuid, ?filename(Name, 0), ?DEFAULT_DIR_PERMS),
        G
    end, DirGuid, lists:seq(1, Depth)),
    Guid = create_file_with_content(P1Node, ?SESS_ID(Provider1), ParentGuid, ?filename(Name, 0)),
    
    ?assertMatch({ok, {Map, _}} when map_size(Map) =/= 0,
        lfm_proxy:get_effective_file_qos(P1Node, ?SESS_ID(Provider1), ?FILE_REF(DirGuid)),
        ?ATTEMPTS),
    
    Size = size(?TEST_DATA),
    ExpectedDistributionFun = fun(List) -> 
        Distribution = lists:map(fun({P, TotalBlocksSize}) ->
            #{
                <<"providerId">> => P,
                <<"totalBlocksSize">> => TotalBlocksSize,
                <<"blocks">> => [[0, TotalBlocksSize]]
            }
        end, List),
        lists:sort(fun(#{<<"providerId">> := ProviderIdA}, #{<<"providerId">> := ProviderIdB}) ->
            ProviderIdA =< ProviderIdB
        end, Distribution)
    end,
    
    CheckDistributionFun = fun(ExpectedWorkersDistribution) ->
        lists:foreach(fun(Provider) ->
            ?assertEqual({ok, ExpectedDistributionFun(ExpectedWorkersDistribution)},
                lfm_proxy:get_file_distribution(oct_background:get_random_provider_node(Provider), 
                    ?SESS_ID(Provider), ?FILE_REF(Guid)), ?ATTEMPTS
            )
        end, Providers)
    end,
    
    LinksKey = datastore_model:get_unique_key(file_meta, file_id:guid_to_uuid(DirGuid)),
    Filters = lists:map(fun
        (qos_entry) -> {qos_entry, QosEntryId};
        (file_meta) -> {file_meta, file_id:guid_to_uuid(ParentGuid)};
        (links_forest) -> {links, links_forest, LinksKey}
    end, RecordsToBlock),
    
    ensure_docs_received(?FUNCTION_NAME, Filters),
    unmock_dbsync_changes(oct_background:get_provider_nodes(Provider2)),
    % mock fslogic_authz:ensure_authorized/3 to bypass its fail on non existing 
    % ancestor file meta document when checking file distribution
    mock_fslogic_authz_ensure_authorized(oct_background:get_provider_nodes(Provider2)),
    save_not_matching_docs(P2Node, ?FUNCTION_NAME, Filters),
    
    CheckDistributionFun([{Provider1, Size}]),
    
    save_matching_docs(P2Node, ?FUNCTION_NAME, [{qos_entry, QosEntryId}]),
    lists:foreach(fun(Provider) ->
        ?assertEqual({ok, ?FULFILLED_QOS_STATUS}, lfm_proxy:check_qos_status(
            oct_background:get_random_provider_node(Provider), ?SESS_ID(Provider), QosEntryId), ?ATTEMPTS)
    end, Providers),
    CheckDistributionFun([{Provider1, Size}]),
    
    save_matching_docs(P2Node, ?FUNCTION_NAME, [{links, links_forest, LinksKey}]),
    case lists:member(file_meta, RecordsToBlock) of
        false ->
            CheckDistributionFun([{Provider1, Size}, {Provider2, Size}]);
        true ->
            CheckDistributionFun([{Provider1, Size}]),
            save_matching_docs(P2Node, ?FUNCTION_NAME, [{file_meta, file_id:guid_to_uuid(ParentGuid)}]),
            CheckDistributionFun([{Provider1, Size}, {Provider2, Size}])
    end.
    

%%%===================================================================
%%% QoS reevaluate
%%%===================================================================

reevaluate_impossible_qos_test(_Config) ->
    [_Provider1, Provider2 | _] = Providers = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    P2StorageId = opt_spaces:get_storage_id(Provider2, SpaceId),
    
    RandomQosParam = str_utils:rand_hex(5),
    {QosNameIdMapping, DirPath} = setup_reevaluate_test(RandomQosParam, impossible),
    
    % Impossible qos reevaluation is called after successful set_qos_parameters
    ok = qos_tests_utils:set_qos_parameters(Provider2, P2StorageId, #{<<"param">> => RandomQosParam}),
    
    ExpectedQosEntriesAfter = [
        #expected_qos_entry{
            providers = Providers,
            qos_name = ?QOS1,
            file_key = {path, DirPath},
            replicas_num = 1,
            qos_expression = [<<"param=", RandomQosParam/binary>>],
            possibility_check = {possible, Provider2}
        }
    ],
    qos_tests_utils:wait_for_qos_fulfillment_in_parallel(QosNameIdMapping, ExpectedQosEntriesAfter),
    qos_reevaluate_assert_file_qos(P2StorageId, DirPath, QosNameIdMapping).

reevaluate_impossible_qos_race_test(_Config) ->
    % this test checks appropriate handling of situation, when provider, on which entry was created,
    % do not have full knowledge of remote QoS parameters
    
    [_Provider1, Provider2 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    P2StorageId = opt_spaces:get_storage_id(Provider2, SpaceId),
    
    RandomQosParam = str_utils:rand_hex(5),
    ok = qos_tests_utils:set_qos_parameters(Provider2, P2StorageId, #{<<"param">> => RandomQosParam}),
    
    {QosNameIdMapping, DirPath} = setup_reevaluate_test(RandomQosParam, possible),
    
    qos_reevaluate_assert_file_qos(P2StorageId, DirPath, QosNameIdMapping).


reevaluate_impossible_qos_conflict_test(_Config) ->
    % this test checks conflict resolution, when multiple providers mark entry as possible
    
    Providers = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    
    RandomQosParam = str_utils:rand_hex(5),
    {QosNameIdMapping, DirPath} = setup_reevaluate_test(RandomQosParam, impossible),

    lists_utils:pforeach(fun(Provider) ->
        StorageId = opt_spaces:get_storage_id(Provider, SpaceId),
        % Impossible qos reevaluation is called after successful set_qos_parameters
        ok = qos_tests_utils:set_qos_parameters(Provider, StorageId, #{<<"param">> => RandomQosParam})
    end, Providers),
    
    % final result should be calculated by provider with lowest id lexicographically
    FinalProvider = lists:min(Providers),

    ExpectedQosEntriesAfter = [
        #expected_qos_entry{
            providers = Providers,
            qos_name = ?QOS1,
            file_key = {path, DirPath},
            replicas_num = 1,
            qos_expression = [<<"param=", RandomQosParam/binary>>],
            possibility_check = {possible, FinalProvider}
        }
    ],
    qos_tests_utils:wait_for_qos_fulfillment_in_parallel(QosNameIdMapping, ExpectedQosEntriesAfter).


setup_reevaluate_test(QosParam, Status) ->
    [Provider1, _Provider2 | _] = Providers = oct_background:get_provider_ids(),
    
    DirName = generator:gen_name(),
    DirPath = filename:join(?SPACE_PATH1, DirName),
    FileName = generator:gen_name(),
    
    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            provider = Provider1,
            dir_structure = {?SPACE_PATH1, [
                {DirName, [{FileName, ?TEST_DATA, [Provider1]}]}
            ]}
        },
        qos_to_add = [
            #qos_to_add{
                provider_selector = Provider1,
                qos_name = ?QOS1,
                path = DirPath,
                expression = <<"param=", QosParam/binary>>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = Providers,
                qos_name = ?QOS1,
                file_key = {path, DirPath},
                replicas_num = 1,
                qos_expression = [<<"param=", QosParam/binary>>],
                possibility_check = {Status, Provider1}
            }
        ],
        wait_for_qos_fulfillment = false
    },
    {_GuidsAndPaths, QosNameIdMapping} = qos_tests_utils:fulfill_qos_test_base(QosSpec),
    {QosNameIdMapping, DirPath}.

qos_reevaluate_assert_file_qos(StorageId, DirPath, QosNameIdMapping) ->
    Providers = oct_background:get_provider_ids(),
    ExpectedFileQos = [
        #expected_file_qos{
            providers = Providers,
            path = DirPath,
            qos_entries = [?QOS1],
            assigned_entries = #{
                StorageId => [?QOS1]
            }
        }
    ],
    qos_tests_utils:assert_file_qos_documents(ExpectedFileQos, QosNameIdMapping, true, 10).

%%%===================================================================
%%% QoS traverse tests
%%%===================================================================

qos_traverse_cancellation_test(_Config) ->
    [Provider1, Provider2 | _] = Providers = oct_background:get_provider_ids(),
    P1Node = oct_background:get_random_provider_node(Provider1),
    P2Node = oct_background:get_random_provider_node(Provider2),
    Name = generator:gen_name(),
    QosRootFilePath = filename:join([<<"/">>, ?SPACE_NAME, Name]),
    
    DirStructure =
        {?SPACE_NAME, [
            {Name, % Dir1
                [
                    {?filename(Name, 0), ?TEST_DATA, [Provider1]},
                    {?filename(Name, 1), ?TEST_DATA, [Provider1]},
                    {?filename(Name, 2), ?TEST_DATA, [Provider1]},
                    {?filename(Name, 3), ?TEST_DATA, [Provider1]}
                ]
            }
        ]},
    
    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = DirStructure
        },
        qos_to_add = [
            #qos_to_add{
                provider_selector = Provider1,
                qos_name = ?QOS1,
                path = QosRootFilePath,
                expression = <<"providerId=", Provider1/binary>>
            }
        ],
        % do not wait for QoS fulfillment
        wait_for_qos_fulfillment = false,
        expected_qos_entries = [
            #expected_qos_entry{
                providers = Providers,
                qos_name = ?QOS1,
                file_key = {path, QosRootFilePath},
                qos_expression = [<<"providerId=", Provider1/binary>>],
                replicas_num = 1,
                possibility_check = {possible, Provider1}
            }
        ]
    },
    
    {GuidsAndPaths, QosNameIdMapping} = qos_tests_utils:fulfill_qos_test_base(QosSpec),
    [QosEntryId] = maps:values(QosNameIdMapping),
    
    DirGuid = qos_tests_utils:get_guid(QosRootFilePath, GuidsAndPaths),
    
    % create file and write to it on remote provider to trigger reconcile transfer
    {ok, {FileGuid, FileHandle}} = lfm_proxy:create_and_open(P2Node, ?SESS_ID(Provider2), DirGuid, 
        generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, _} = lfm_proxy:write(P2Node, FileHandle, 0, <<"new_data">>),
    ok = lfm_proxy:close(P2Node, FileHandle),
    
    % wait for reconciliation transfer to start
    receive {qos_slave_job, _Pid, FileGuid} = Msg ->
        self() ! Msg
    end,
    
    % remove entry to trigger transfer cancellation
    lfm_proxy:remove_qos_entry(P1Node, ?SESS_ID(Provider1), QosEntryId),
    
    % check that 5 transfers were cancelled (4 from traverse and 1 reconciliation)
    test_utils:mock_assert_num_calls(P1Node, replica_synchronizer, cancel, 1, 5, ?ATTEMPTS),
    
    % check that qos_entry document is deleted
    lists:foreach(fun(Node) ->
        ?assertEqual(?ERROR_NOT_FOUND, test_rpc:call(op_worker, Node, qos_entry, get, [QosEntryId]), ?ATTEMPTS)
    end, oct_background:get_all_providers_nodes()),
    
    % finish transfers to unlock waiting slave job processes
    ok = qos_tests_utils:finish_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)] ++ [FileGuid]).

%%%===================================================================
%%% QoS with hardlinks tests
%%%===================================================================

qos_on_hardlink_test(_Config) ->
    qos_test_base:qos_with_hardlink_test_base(direct).

effective_qos_with_hardlinks_test(_Config) ->
    qos_test_base:qos_with_hardlink_test_base(effective).

qos_with_inode_deletion_test(_Config) ->
    qos_test_base:qos_with_hardlink_deletion_test_base(inode).

qos_with_hardlink_deletion_test(_Config) ->
    qos_test_base:qos_with_hardlink_deletion_test_base(hardlink).

qos_with_mixed_deletion_test(_Config) ->
    qos_test_base:qos_with_hardlink_deletion_test_base(mixed).

qos_on_symlink_test(_Config) ->
    qos_test_base:qos_on_symlink_test_base().

effective_qos_with_symlink_test(_Config) ->
    qos_test_base:effective_qos_with_symlink_test_base().

create_hardlink_in_dir_with_qos(_Config) ->
    qos_test_base:create_hardlink_in_dir_with_qos().


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite([{?LOAD_MODULES, [?MODULE, qos_tests_utils]} | Config], #onenv_test_config{
        onenv_scenario = "2op-2nodes",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {provider_token_ttl_sec, 24 * 60 * 60},
            {qos_retry_failed_files_interval_seconds, 5}
        ]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(qos_traverse_cancellation_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    qos_tests_utils:mock_transfers(Workers),
    init_per_testcase(default, Config);
init_per_testcase(_, Config) ->
    qos_tests_utils:reset_qos_parameters(),
    lfm_proxy:init(Config),
    Config.


end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    qos_tests_utils:finish_all_transfers(),
    test_utils:mock_unload(Workers),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Test bases
%%%===================================================================

add_qos_for_dir_and_check_effective_qos(TestSpec) ->
    #effective_qos_test_spec{
        initial_dir_structure = InitialDirStructure,
        qos_to_add = QosToAddList,
        expected_qos_entries = ExpectedQosEntries,
        expected_effective_qos = ExpectedEffectiveQos
    } = TestSpec,

    % create initial dir structure
    GuidsAndPaths = qos_tests_utils:create_dir_structure(InitialDirStructure),
    ?assertMatch(true, qos_tests_utils:assert_distribution_in_dir_structure(InitialDirStructure, GuidsAndPaths)),

    % add QoS and wait for fulfillment
    QosNameIdMapping = qos_tests_utils:add_multiple_qos(QosToAddList),
    qos_tests_utils:wait_for_qos_fulfillment_in_parallel(QosNameIdMapping, ExpectedQosEntries),

    % check documents
    qos_tests_utils:assert_qos_entry_documents(ExpectedQosEntries, QosNameIdMapping, ?ATTEMPTS),
    qos_tests_utils:assert_effective_qos(ExpectedEffectiveQos, QosNameIdMapping, true).


%%%===================================================================
%%% Internal functions
%%%===================================================================

get_expected_structure_for_single_dir(ProviderIdList) ->
    #test_dir_structure{
        dir_structure = {?SPACE_NAME, [
            {<<"dir1">>, [
                {<<"file1">>, <<"test_data">>, ProviderIdList}
            ]}
        ]}
    }.


mock_file_meta_posthooks() ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_new(Nodes, file_meta_posthooks, [passthrough]),
    TestPid = self(),
    ok = test_utils:mock_expect(Nodes, file_meta_posthooks, add_hook,
        fun(FileUuid, Identifier, Module, Function, Args) ->
            Res = meck:passthrough([FileUuid, Identifier, Module, Function, Args]),
            TestPid ! post_hook_created,
            Res
        end).


unmock_file_meta_posthooks() ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_unload(Nodes, file_meta_posthooks).


create_file_with_content(Node, SessId, ParentGuid, Name) ->
    {ok, {G, H1}} = lfm_proxy:create_and_open(Node, SessId, ParentGuid, Name, ?DEFAULT_FILE_PERMS),
    {ok, _} = lfm_proxy:write(Node, H1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(Node, H1),
    G.


mock_fslogic_authz_ensure_authorized(Node) ->
    test_utils:mock_new(Node, fslogic_authz, [passthrough]),
    ok = test_utils:mock_expect(Node, fslogic_authz, ensure_authorized,
        fun(_, FileCtx, _) ->
            FileCtx
        end).

%%%===================================================================
%%% DBSync mocks
%%%===================================================================

mock_dbsync_changes(Nodes, MsgIdentifier) ->
    test_utils:mock_new(Nodes, dbsync_changes, [passthrough]),
    TestPid = self(),

    ok = test_utils:mock_expect(Nodes, dbsync_changes, apply,
        fun(#document{value = Value} = Doc) ->
            RecordType = element(1, Value),
            TestPid ! {MsgIdentifier, RecordType, Doc},
            ok
        end).


unmock_dbsync_changes(Nodes) ->
    test_utils:mock_unload(Nodes, dbsync_changes).


save_matching_docs(Node, MsgIdentifier, Filters) ->
    save_docs(Node, MsgIdentifier, Filters, matching).


save_not_matching_docs(Node, MsgIdentifier, Filters) ->
    save_docs(Node, MsgIdentifier, Filters, not_matching).


save_docs(Node, MsgIdentifier, Filters, Strategy) ->
    LeftOutDocs = save_docs(Node, MsgIdentifier, Filters, [], Strategy),
    lists:foreach(fun(#document{value = Value} = Doc) ->
        RecordType = element(1, Value),
        self() ! {MsgIdentifier, RecordType, Doc}
    end, LeftOutDocs).


save_docs(Node, MsgIdentifier, Filters, LeftOutDocs, Strategy) ->
    ExpectedMatch = Strategy == matching,
    receive
        {MsgIdentifier, _, Doc} ->
            case matches_doc(Doc, Filters) of
                ExpectedMatch ->
                    ?assertMatch(ok, test_rpc:call(op_worker, Node, dbsync_changes, apply, [Doc])),
                    save_docs(Node, MsgIdentifier, Filters, LeftOutDocs, Strategy);
                _ ->
                    save_docs(Node, MsgIdentifier, Filters, [Doc | LeftOutDocs], Strategy)
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
