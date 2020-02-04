%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests for QoS management on single provider.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_test_SUITE).
-author("Michal Cwiertnia").

-include("global_definitions.hrl").
-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("qos_tests_utils.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, init_per_testcase/2,
    end_per_suite/1, end_per_testcase/2
]).

%% test functions
-export([
    % QoS bounded cache tests
    qos_bounded_cache_should_be_periodically_cleaned_if_overfilled/1,
    qos_bounded_cache_should_not_be_cleaned_if_not_overfilled/1,

    % Invalid QoS expression tests
    key_without_value/1,
    two_keys_without_value_connected_with_operand/1,
    operator_without_second_operand/1,
    operator_without_first_operand/1,
    two_operators_in_row/1,
    closing_paren_without_matching_opening_one/1,
    opening_paren_without_matching_closing_one/1,
    mismatching_nested_parens/1,

    % Single QoS expression tests
    simple_key_val_qos/1,
    qos_with_intersection/1,
    qos_with_complement/1,
    qos_with_union/1,
    qos_with_multiple_replicas/1,
    qos_with_multiple_replicas_and_union/1,
    qos_with_intersection_and_union/1,
    qos_with_union_and_complement/1,
    qos_with_intersection_and_complement/1,
    key_val_qos_that_cannot_be_fulfilled/1,
    qos_that_cannot_be_fulfilled/1,
    qos_with_parens/1,

    % Multi QoS tests
    multi_qos_resulting_in_the_same_storages/1,
    same_qos_multiple_times/1,
    contrary_qos/1,
    multi_qos_where_one_cannot_be_satisfied/1,
    multi_qos_that_overlaps/1,
    multi_qos_resulting_in_different_storages/1,

    % Effective QoS tests
    effective_qos_for_file_in_directory/1,
    effective_qos_for_file_in_nested_directories/1,
    effective_qos_for_files_in_different_directories_of_tree_structure/1,
    
    % fixme add to all
    % QoS clean up tests
    qos_cleanup_test/1,
    
    % QoS status tests
    qos_status_during_traverse_test/1
]).

all() -> [
    % QoS bounded cache tests
    qos_bounded_cache_should_be_periodically_cleaned_if_overfilled,
    qos_bounded_cache_should_not_be_cleaned_if_not_overfilled,

    % Invalid QoS expression tests
    key_without_value,
    two_keys_without_value_connected_with_operand,
    operator_without_second_operand,
    operator_without_first_operand,
    two_operators_in_row,
    closing_paren_without_matching_opening_one,
    opening_paren_without_matching_closing_one,
    mismatching_nested_parens,

    % Single QoS expression tests
    simple_key_val_qos,
    qos_with_intersection,
    qos_with_complement,
    qos_with_union,
    qos_with_multiple_replicas,
    qos_with_multiple_replicas_and_union,
    qos_with_intersection_and_union,
    qos_with_union_and_complement,
    qos_with_intersection_and_complement,
    key_val_qos_that_cannot_be_fulfilled,
    qos_that_cannot_be_fulfilled,
    qos_with_parens,

    % Multi QoS tests
    multi_qos_resulting_in_the_same_storages,
    same_qos_multiple_times,
    contrary_qos,
    multi_qos_where_one_cannot_be_satisfied,
    multi_qos_that_overlaps,
    multi_qos_resulting_in_different_storages,

    % Effective QoS tests
    effective_qos_for_file_in_directory,
    effective_qos_for_file_in_nested_directories,
    effective_qos_for_files_in_different_directories_of_tree_structure
].

% Although this test SUITE is single provider, QoS parameters
% for multiple providers are mocked for QoS target storages
% calculation so more complex examples can be tested.
-define(P1_TEST_QOS, #{
    <<"country">> => <<"PL">>,
    <<"type">> => <<"disk">>,
    <<"tier">> => <<"t3">>,
    <<"param1">> => <<"val1">>
}).

-define(P2_TEST_QOS, #{
    <<"country">> => <<"FR">>,
    <<"type">> => <<"tape">>,
    <<"tier">> => <<"t2">>
}).

-define(P3_TEST_QOS, #{
    <<"country">> => <<"PT">>,
    <<"type">> => <<"disk">>,
    <<"tier">> => <<"t2">>,
    <<"param1">> => <<"val1">>
}).

-define(TEST_PROVIDERS_QOS, #{
    <<"p1">> => ?P1_TEST_QOS,
    <<"p2">> => ?P2_TEST_QOS,
    <<"p3">> => ?P3_TEST_QOS
}).


-define(SPACE1_ID, <<"space_id1">>).
-define(SPACE_PATH1, <<"/space_name1">>).
-define(TEST_FILE_PATH, filename:join(?SPACE_PATH1, <<"file1">>)).
-define(TEST_DIR_PATH, filename:join(?SPACE_PATH1, <<"dir1">>)).

-define(PROVIDERS_MAP, #{
    ?P1 => ?P1,
    ?P2 => ?P2,
    ?P3 => ?P3
}).


-define(GET_CACHE_TABLE_SIZE(SPACE_ID),
    element(2, lists:keyfind(size, 1, rpc:call(Worker, ets, info, [?CACHE_TABLE_NAME(SPACE_ID)])))
).

-define(QOS_CACHE_TEST_OPTIONS(Size),
    #{
        size => Size,
        group => true,
        name => ?QOS_BOUNDED_CACHE_GROUP,
        check_frequency => timer:seconds(300)
    }
).


-define(NESTED_DIR_STRUCTURE, {?SPACE_PATH1, [
    {<<"dir1">>, [
        {<<"dir2">>, [
            {<<"dir3">>, [
                {<<"file31">>, <<"data">>},
                {<<"dir4">>, [
                    {<<"file41">>, <<"data">>}
                ]}
            ]}
        ]}
    ]}
]}).


%%%===================================================================
%%% QoS bounded cache tests.
%%%===================================================================

qos_bounded_cache_should_be_periodically_cleaned_if_overfilled(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    Dir1Path = filename:join([?SPACE_PATH1, <<"dir1">>]),
    FilePath = filename:join([?SPACE_PATH1, <<"dir1">>, <<"dir2">>, <<"dir3">>, <<"dir4">>, <<"file41">>]),

    EffQosTestSpec = #effective_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = ?NESTED_DIR_STRUCTURE
        },
        qos_to_add = [
            #qos_to_add{
                path = Dir1Path,
                qos_name = ?QOS1,
                expression = <<"country=FR">>
            }
        ],
        expected_effective_qos = [
            #expected_file_qos{
                path = FilePath,
                qos_entries = [?QOS1],
                assigned_entries = #{?P2 => [?QOS1]}
            }
        ]
    },

    % add QoS and calculate effective QoS to fill in cache
    add_qos_for_dir_and_check_effective_qos(Config, EffQosTestSpec),

    % check that QoS cache is overfilled
    SizeBeforeCleaning = ?GET_CACHE_TABLE_SIZE(?SPACE1_ID),
    ?assertEqual(6, SizeBeforeCleaning),

    % send message that checks cache size and cleans it if necessary
    ?assertMatch(ok, rpc:call(Worker, bounded_cache, check_cache_size, [?QOS_CACHE_TEST_OPTIONS(1)])),

    % check that cache has been cleaned
    SizeAfterCleaning = ?GET_CACHE_TABLE_SIZE(?SPACE1_ID),
    ?assertEqual(0, SizeAfterCleaning).


qos_bounded_cache_should_not_be_cleaned_if_not_overfilled(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    Dir1Path = filename:join([?SPACE_PATH1, <<"dir1">>]),
    FilePath = filename:join([?SPACE_PATH1, <<"dir1">>, <<"dir2">>, <<"dir3">>, <<"dir4">>, <<"file41">>]),

    EffQosTestSpec = #effective_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = ?NESTED_DIR_STRUCTURE
        },
        qos_to_add = [
            #qos_to_add{
                path = Dir1Path,
                qos_name = ?QOS1,
                expression = <<"country=FR">>
            }
        ],
        expected_effective_qos = [
            #expected_file_qos{
                path = FilePath,
                qos_entries = [?QOS1],
                assigned_entries = #{?P2 => [?QOS1]}
            }
        ]
    },

    % add QoS and calculate effective QoS to fill in cache
    add_qos_for_dir_and_check_effective_qos(Config, EffQosTestSpec),

    % check that QoS cache is not empty
    SizeBeforeCleaning = ?GET_CACHE_TABLE_SIZE(?SPACE1_ID),
    ?assertEqual(6, SizeBeforeCleaning),

    % send message that checks cache size and cleans it if necessary
    ?assertMatch(ok, rpc:call(Worker, bounded_cache, check_cache_size, [?QOS_CACHE_TEST_OPTIONS(6)])),

    SizeAfterCleaning = ?GET_CACHE_TABLE_SIZE(?SPACE1_ID),
    ?assertEqual(6, SizeAfterCleaning).


%%%===================================================================
%%% Invalid QoS expression tests.
%%%===================================================================

key_without_value(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        {error, ?EINVAL},
        lfm_proxy:add_qos_entry(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"country">>, 1)
    ).


two_keys_without_value_connected_with_operand(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        {error, ?EINVAL},
        lfm_proxy:add_qos_entry(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"country|type">>, 1)
    ).


operator_without_second_operand(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        {error, ?EINVAL},
        lfm_proxy:add_qos_entry(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"country=PL&">>, 1)
    ).


operator_without_first_operand(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        {error, ?EINVAL},
        lfm_proxy:add_qos_entry(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"|country=PL">>, 1)
    ).


two_operators_in_row(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        {error, ?EINVAL},
        lfm_proxy:add_qos_entry(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"country=PL&-type-disk">>, 1)
    ).


closing_paren_without_matching_opening_one(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        {error, ?EINVAL},
        lfm_proxy:add_qos_entry(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"country=PL)">>, 1)
    ).


opening_paren_without_matching_closing_one(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        {error, ?EINVAL},
        lfm_proxy:add_qos_entry(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"(country=PL">>, 1)
    ).


mismatching_nested_parens(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        {error, ?EINVAL},
        lfm_proxy:add_qos_entry(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"(type=disk|tier=t2&(country=PL)">>, 1)
    ).


%%%===================================================================
%%% Group of tests that adds single QoS expression for file or directory
%%% and checks QoS docs.
%%% Each test case is executed once for file and once for directory.
%%%===================================================================

simple_key_val_qos(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:simple_key_val_qos_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


qos_with_intersection(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:qos_with_intersection_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


qos_with_complement(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:qos_with_complement_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


qos_with_union(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:qos_with_union_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


qos_with_multiple_replicas(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:qos_with_multiple_replicas_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


qos_with_intersection_and_union(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:qos_with_intersection_and_union_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


qos_with_union_and_complement(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:qos_with_union_and_complement_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


qos_with_intersection_and_complement(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:qos_with_intersection_and_complement_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


qos_with_multiple_replicas_and_union(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:qos_with_multiple_replicas_and_union_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).



key_val_qos_that_cannot_be_fulfilled(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:key_val_qos_that_cannot_be_fulfilled_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).



qos_that_cannot_be_fulfilled(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:qos_that_cannot_be_fulfilled_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


qos_with_parens(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:qos_with_parens_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


%%%===================================================================
%%% Group of tests that adds multiple QoS expression for single file or
%%% directory and checks QoS docs.
%%% Each test case is executed once for file and once for directory.
%%%===================================================================

multi_qos_resulting_in_different_storages(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:multi_qos_resulting_in_different_storages_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


multi_qos_resulting_in_the_same_storages(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:multi_qos_resulting_in_the_same_storages_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


same_qos_multiple_times(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:same_qos_multiple_times_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


contrary_qos(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:contrary_qos_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


multi_qos_where_one_cannot_be_satisfied(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:multi_qos_where_one_cannot_be_satisfied_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


multi_qos_that_overlaps(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),
        qos_test_base:multi_qos_that_overlaps_spec(Path, Worker, [Worker], ?PROVIDERS_MAP)
    end).


%%%===================================================================
%%% Group of tests that creates directory structure, adds QoS on different
%%% levels of created structure and checks effective QoS and QoS docs.
%%%===================================================================

effective_qos_for_file_in_directory(Config) ->
    DirPath = filename:join(?SPACE_PATH1, <<"dir1">>),
    FilePath = filename:join(DirPath, <<"file1">>),
    [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),

    QosSpec = qos_test_base:effective_qos_for_file_in_directory_spec(DirPath,
        FilePath, Worker, [Worker], ?PROVIDERS_MAP),
    TestSpec = #effective_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = {?SPACE_PATH1, [
                {<<"dir1">>, [
                    {<<"file1">>, ?TEST_DATA}
                ]}
            ]}
        },
        qos_to_add = QosSpec#qos_spec.qos_to_add,
        expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
        expected_effective_qos = QosSpec#qos_spec.expected_effective_qos
    },

    add_qos_for_dir_and_check_effective_qos(Config, TestSpec).


effective_qos_for_file_in_nested_directories(Config) ->
    Dir2Path = filename:join(<<"/space_name1/dir1">>, <<"dir2">>),
    Dir3Path = filename:join(Dir2Path, <<"dir3">>),
    Dir4Path = filename:join(Dir3Path, <<"dir4">>),
    File31Path = filename:join(Dir3Path, <<"file31">>),
    File41Path = filename:join(Dir4Path, <<"file41">>),
    [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),

    QosSpec = qos_test_base:effective_qos_for_file_in_nested_directories_spec([Dir2Path, Dir3Path, Dir4Path],
        [File31Path, File41Path], Worker, [Worker], ?PROVIDERS_MAP),
    TestSpec = #effective_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = ?NESTED_DIR_STRUCTURE
        },
        qos_to_add = QosSpec#qos_spec.qos_to_add,
        expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
        expected_effective_qos = QosSpec#qos_spec.expected_effective_qos
    },

    add_qos_for_dir_and_check_effective_qos(Config, TestSpec).


effective_qos_for_files_in_different_directories_of_tree_structure(Config) ->
    Dir1Path = filename:join(<<"/space_name1">>, <<"dir1">>),
    Dir2Path = filename:join(Dir1Path, <<"dir2">>),
    Dir3Path = filename:join(Dir1Path, <<"dir3">>),
    File21Path = filename:join(Dir2Path, <<"file21">>),
    File31Path = filename:join(Dir3Path, <<"file31">>),
    [Worker] = qos_tests_utils:get_op_nodes_sorted(Config),

    QosSpec = qos_test_base:effective_qos_for_files_in_different_directories_of_tree_structure_spec(
        [Dir1Path, Dir2Path, Dir3Path], [File21Path, File31Path], Worker, [Worker], ?PROVIDERS_MAP
    ),
    TestSpec = #effective_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = {?SPACE_PATH1, [
                {<<"dir1">>, [
                    {<<"dir2">>, [{<<"file21">>, ?TEST_DATA}]},
                    {<<"dir3">>, [{<<"file31">>, ?TEST_DATA}]}
                ]}
            ]}
        },
        qos_to_add = QosSpec#qos_spec.qos_to_add,
        expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
        expected_effective_qos = QosSpec#qos_spec.expected_effective_qos
    },

    add_qos_for_dir_and_check_effective_qos(Config, TestSpec).

%%%===================================================================
%%% Tests that check QoS documents clean up procedure
%%%===================================================================
% fixme duplicate

-define(simple_dir_structure(Name, Distribution),
    {?SPACE_PATH1, [
        {Name, ?TEST_DATA, Distribution}
    ]}
).

qos_cleanup_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Name = generator:gen_name(),
    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = ?simple_dir_structure(Name, [?GET_DOMAIN_BIN(Worker)])
        },
        qos_to_add = [
            #qos_to_add{
                worker = Worker,
                qos_name = ?QOS1,
                path = filename:join([?SPACE_PATH1, Name]),
                expression = <<"country=PL">>
            }
        ]
    },
    
    {GuidsAndPaths, QosNameIdMapping} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    
    #{files := [{FileGuid, _FilePath} | _]} = GuidsAndPaths,
    
    ok = lfm_proxy:unlink(Worker, SessId, {guid, FileGuid}),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    QosEntryId = maps:get(?QOS1, QosNameIdMapping),
    
    ?assertEqual({error, not_found}, rpc:call(Worker, datastore_model, get, [file_qos:get_ctx(), FileUuid])),
    ?assertEqual({error, {file_meta_missing, FileUuid}}, rpc:call(Worker, file_qos, get_effective, [FileUuid])),
    ?assertEqual({error, not_found}, rpc:call(Worker, qos_entry, get, [QosEntryId])).

%fixme effective qos test and for dir

%%%===================================================================
%%% QoS status tests
%%%===================================================================

% fixme more tests
qos_status_during_traverse_test(Config) ->
    qos_test_base:qos_status_during_traverse_test_base(Config, ?SPACE_PATH1, 4).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = initializer:setup_storage(NewConfig),
        hackney:start(),
        application:start(ssl),
        NewConfig1
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, qos_tests_utils]} | Config].


end_per_suite(Config) ->
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).

init_per_testcase(qos_status_during_traverse_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    % do not start file synchronization
    qos_tests_utils:mock_transfers(Workers),
    mock_space_storages(ConfigWithSessionInfo, maps:keys(?TEST_PROVIDERS_QOS)),
    mock_storage_qos_parameters(Workers, ?TEST_PROVIDERS_QOS),
    mock_storage_get_provider(ConfigWithSessionInfo),
    lfm_proxy:init(ConfigWithSessionInfo);
init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    % do not start file synchronization
    mock_synchronize_transfers(ConfigWithSessionInfo),
    mock_space_storages(ConfigWithSessionInfo, maps:keys(?TEST_PROVIDERS_QOS)),
    mock_storage_qos_parameters(Workers, ?TEST_PROVIDERS_QOS),
    mock_storage_get_provider(ConfigWithSessionInfo),
    lfm_proxy:init(ConfigWithSessionInfo).


end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    test_utils:mock_unload(Workers),
    initializer:clean_test_users_and_spaces_no_validate(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================

run_tests(Config, FileTypes, TestSpecFun) ->
    lists:foreach(fun(FileType) ->
        TestSpec = case FileType of
            file ->
                ct:pal("Starting for file"),
                Path = create_test_file(Config),
                TestSpecFun(Path);
            dir ->
                ct:pal("Starting for dir"),
                Path = create_test_dir_with_file(Config),
                TestSpecFun(Path)
        end,
        add_qos_and_check_qos_docs(Config, TestSpec)
    end, FileTypes).


add_qos_and_check_qos_docs(Config, #qos_spec{
    qos_to_add = QosToAddList,
    expected_qos_entries = ExpectedQosEntries,
    expected_file_qos = ExpectedFileQos
} ) ->
    % add QoS for file and wait for appropriate QoS status
    QosNameIdMapping = qos_tests_utils:add_multiple_qos(Config, QosToAddList),
    qos_tests_utils:wait_for_qos_fulfillment_in_parallel(Config, undefined, QosNameIdMapping, ExpectedQosEntries),

    % check qos documents
    qos_tests_utils:assert_qos_entry_documents(Config, ExpectedQosEntries, QosNameIdMapping),
    qos_tests_utils:assert_file_qos_documents(Config, ExpectedFileQos, QosNameIdMapping, false).


add_qos_for_dir_and_check_effective_qos(Config, #effective_qos_test_spec{
    initial_dir_structure = InitialDirStructure,
    qos_to_add = QosToAddList,
    expected_qos_entries = ExpectedQosEntries,
    expected_effective_qos = ExpectedEffectiveQos
} ) ->
    % create initial dir structure
    qos_tests_utils:create_dir_structure(Config, InitialDirStructure),

    % add QoS and wait for appropriate QoS status
    QosNameIdMapping = qos_tests_utils:add_multiple_qos(Config, QosToAddList),
    qos_tests_utils:wait_for_qos_fulfillment_in_parallel(Config, undefined, QosNameIdMapping, ExpectedQosEntries),

    % check qos_entry documents and effective QoS
    qos_tests_utils:assert_qos_entry_documents(Config, ExpectedQosEntries, QosNameIdMapping),
    qos_tests_utils:assert_effective_qos(Config, ExpectedEffectiveQos, QosNameIdMapping, false).


create_test_file(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    _Guid = qos_tests_utils:create_file(Worker, SessId, ?TEST_FILE_PATH, ?TEST_DATA),
    ?TEST_FILE_PATH.


create_test_dir_with_file(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    _DirGuid = qos_tests_utils:create_directory(Worker, SessId, ?TEST_DIR_PATH),
    FilePath = filename:join(?TEST_DIR_PATH, <<"file1">>),
    _FileGuid = qos_tests_utils:create_file(Worker, SessId, FilePath, ?TEST_DATA),
    ?TEST_DIR_PATH.


%%%====================================================================
%%% Mocks
%%%====================================================================

mock_space_storages(Config, StorageList) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_expect(Workers, space_logic, get_all_storage_ids,
        fun(_) ->
            {ok, StorageList}
        end).


mock_storage_qos_parameters(Workers, StorageQos) ->
    test_utils:mock_expect(Workers, storage_logic, get_qos_parameters_of_remote_storage, fun(StorageId, _SpaceId) ->
        {ok, maps:get(StorageId, StorageQos, #{})}
    end).


mock_synchronize_transfers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, replica_synchronizer, [passthrough]),
    ok = test_utils:mock_expect(Workers, replica_synchronizer, synchronize,
        fun(_, _, _, _, _, _) ->
            {ok, ok}
        end).


mock_storage_get_provider(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Workers, storage),
    lists:foreach(fun(Worker) ->
    ok = test_utils:mock_expect(Workers, storage_logic, get_provider,
        fun(_StorageId) ->
            {ok, ?GET_DOMAIN_BIN(Worker)}
        end)
    end, Workers).
