%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests for single file QoS management on
%%% multiple providers.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_multi_provider_single_file_test_SUITE).
-author("Michal Cwiertnia").

-include("modules/logical_file_manager/lfm.hrl").
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
    simple_key_val_qos/1,
    qos_with_intersection/1,
    qos_with_complement/1,
    qos_with_union/1,
    qos_with_multiple_replicas/1,
    qos_with_intersection_and_union/1,
    qos_with_union_and_complement/1,
    qos_with_intersection_and_complement/1,
    qos_with_multiple_replicas_and_union/1,
    key_val_qos_that_cannot_be_fulfilled/1,
    qos_that_cannot_be_fulfilled/1,
    qos_with_parens/1,

    multi_qos_resulting_in_different_storages/1,
    multi_qos_resulting_in_the_same_storages/1,
    same_qos_multiple_times/1,
    contrary_qos/1,
    multi_qos_where_one_cannot_be_satisfied/1,
    multi_qos_that_overlaps/1,
    
    qos_reconciliation_file_test/1,
    qos_reconciliation_dir_test/1,
    
    qos_transfer_stats_test/1
]).

all() -> [
    simple_key_val_qos,
    qos_with_intersection,
    qos_with_complement,
    qos_with_union,
    qos_with_multiple_replicas,
    qos_with_intersection_and_union,
    qos_with_union_and_complement,
    qos_with_intersection_and_complement,
    qos_with_multiple_replicas_and_union,
    key_val_qos_that_cannot_be_fulfilled,
    qos_that_cannot_be_fulfilled,
    qos_with_parens,

    multi_qos_resulting_in_different_storages,
    multi_qos_resulting_in_the_same_storages,
    same_qos_multiple_times,
    contrary_qos,
    multi_qos_where_one_cannot_be_satisfied,
    multi_qos_that_overlaps,
    
    qos_reconciliation_file_test,
    qos_reconciliation_dir_test,
    
    qos_transfer_stats_test
].


-define(PATH(FileName), filename:join([?SPACE_PATH1, FileName])).
-define(DIRNAME(Name), <<"dir_", Name/binary>>).
-define(FILENAME(Name), <<"file_", Name/binary>>).

-define(SPACE1_PLACEHOLDER, space1).
-define(SPACE_NAME, <<"space1">>).
-define(SPACE_PATH1, <<"/space1">>).

-define(USER_PLACEHOLDER, user2).
-define(SESS_ID(ProviderPlaceholder), oct_background:get_user_session_id(?USER_PLACEHOLDER, ProviderPlaceholder)).

-define(ATTEMPTS, 60).

%%%====================================================================
%%% Test function
%%%====================================================================

%%%===================================================================
%%% Group of tests that adds single QoS expression for file or directory
%%% and checks QoS docs and file distribution.
%%% Each test case is executed once for file and once for directory.
%%%===================================================================

simple_key_val_qos(_Config) ->
    Providers = [Provider1, Provider2 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider2, [Provider1, Provider2],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:simple_key_val_qos_spec(Path, Provider2, Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_intersection(_Config) ->
    Providers = [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1, Provider3],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_intersection_spec(Path, Provider1, Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_complement(_Config) ->
    Providers = [Provider1 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_complement_spec(Path, Provider1, Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_union(_Config) ->
    Providers = [Provider1, _Provider2, _Provider3 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_union_spec(Path, Provider1, Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_multiple_replicas(_Config) ->
    Providers = [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1, Provider3],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_multiple_replicas_spec(Path, Provider1, Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_intersection_and_union(_Config) ->
    Providers = [Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1, Provider2, Provider3],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_intersection_and_union_spec(Path, Provider1, Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_union_and_complement(_Config) ->
    Providers = [Provider1 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_union_and_complement_spec(Path, Provider1, Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_intersection_and_complement(_Config) ->
    Providers = [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1, Provider3],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_intersection_and_complement_spec(Path, Provider1, Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).

qos_with_multiple_replicas_and_union(_Config) ->
    Providers = [Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1, Provider2, Provider3],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_multiple_replicas_and_union_spec(Path, Provider1, Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


key_val_qos_that_cannot_be_fulfilled(_Config) ->
    Providers = [Provider1 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:key_val_qos_that_cannot_be_fulfilled_spec(Path, Provider1, Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_that_cannot_be_fulfilled(_Config) ->
    Providers = [Provider1 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_that_cannot_be_fulfilled_spec(Path, Provider1, Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_parens(_Config) ->
    Providers = [Provider1 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_parens_spec(Path, Provider1, Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


%%%===================================================================
%%% Group of tests that adds multiple QoS expression for single file or
%%% directory and checks QoS docs and file distribution.
%%% Each test case is executed once for file and once for directory.
%%%===================================================================

multi_qos_resulting_in_different_storages(_Config) ->
    Providers = [Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1, Provider2, Provider3],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:multi_qos_resulting_in_different_storages_spec(Path, [Provider1, Provider2], Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


multi_qos_resulting_in_the_same_storages(_Config) ->
    Providers = [Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1, Provider2],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:multi_qos_resulting_in_the_same_storages_spec(Path, [Provider2, Provider3], Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


same_qos_multiple_times(_Config) ->
    Providers = [Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1, Provider2],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:same_qos_multiple_times_spec(Path, [Provider1, Provider2, Provider3], Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


contrary_qos(_Config) ->
    Providers = [Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider2, [Provider1, Provider2],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:contrary_qos_spec(Path, [Provider2, Provider3], Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


multi_qos_where_one_cannot_be_satisfied(_Config) ->
    Providers = [Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1, Provider2],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:multi_qos_where_one_cannot_be_satisfied_spec(Path, [Provider1, Provider3], Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


multi_qos_that_overlaps(_Config) ->
    Providers = [Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    run_tests([file, dir], Provider1, [Provider1, Provider2, Provider3],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:multi_qos_that_overlaps_spec(Path, [Provider2, Provider3], Providers),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_reconciliation_file_test(_Config) ->
    basic_qos_reconciliation_test_base(simple).

qos_reconciliation_dir_test(_Config) ->
    basic_qos_reconciliation_test_base(nested).


basic_qos_reconciliation_test_base(DirStructureType) ->
    [Provider1, Provider2 | _] = oct_background:get_provider_ids(),
    P1Node = oct_background:get_random_provider_node(Provider1),
    SpaceId = oct_background:get_space_id(?SPACE1_PLACEHOLDER),
    
    Filename = generator:gen_name(),
    QosSpec = create_basic_qos_test_spec(DirStructureType, Filename),
    {GuidsAndPaths, _} = qos_tests_utils:fulfill_qos_test_base(QosSpec),
    NewData = <<"new_test_data">>,
    StoragePaths = lists:map(fun({Guid, Path}) ->
        % remove leading slash and space id
        [_, _ | PathTokens] = binary:split(Path, <<"/">>, [global]),
        StoragePath = storage_file_path(oct_background:get_random_provider_node(Provider2),
            SpaceId, filename:join(PathTokens)),
        ?assertEqual({ok, ?TEST_DATA}, read_file(oct_background:get_random_provider_node(Provider2), StoragePath)),
        {ok, FileHandle} = lfm_proxy:open(P1Node, ?SESS_ID(Provider1), ?FILE_REF(Guid), write),
        {ok, _} = lfm_proxy:write(P1Node, FileHandle, 0, NewData),
        ok = lfm_proxy:close(P1Node, FileHandle),
        StoragePath
    end, maps:get(files, GuidsAndPaths)),
    lists:foreach(fun(StoragePath) ->
        lists:foreach(fun(N) ->
            ?assertEqual({ok, NewData}, read_file(N, StoragePath), ?ATTEMPTS)
        end, oct_background:get_provider_nodes(Provider2))
    end, StoragePaths).


create_basic_qos_test_spec(DirStructureType, QosFilename) ->
    [Provider1, Provider2 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE1_PLACEHOLDER),
    {DirStructure, DirStructureAfter} = case DirStructureType of
        simple ->
            {?simple_dir_structure(?SPACE_NAME, QosFilename, [Provider1]),
                ?simple_dir_structure(?SPACE_NAME, QosFilename, [Provider1, Provider2])};
        nested ->
            {?nested_dir_structure(?SPACE_NAME, QosFilename, [Provider1]),
                ?nested_dir_structure(?SPACE_NAME, QosFilename, [Provider1, Provider2])}
    end,
    
    #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = DirStructure
        },
        qos_to_add = [
            #qos_to_add{
                provider_selector = Provider1,
                qos_name = ?QOS1,
                path = ?PATH(QosFilename),
                expression = <<"providerId=", Provider2/binary>>
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                qos_name = ?QOS1,
                file_key = {path, ?PATH(QosFilename)},
                qos_expression = [<<"providerId=", Provider2/binary>>],
                replicas_num = 1,
                possibility_check = {possible, Provider1}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                path = ?PATH(QosFilename),
                qos_entries = [?QOS1],
                assigned_entries = #{
                    opt_spaces:get_storage_id(Provider2, SpaceId) => [?QOS1]
                }
            }
        ],
        expected_dir_structure = #test_dir_structure{
            dir_structure = DirStructureAfter
        }
    }.


%%%===================================================================
%%% QoS transfer stats tests
%%%===================================================================

qos_transfer_stats_test(_Config) ->
    [Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    P1Node = oct_background:get_random_provider_node(Provider1),
    P2Node = oct_background:get_random_provider_node(Provider2),
    P3Node = oct_background:get_random_provider_node(Provider3),
    SpaceId = oct_background:get_space_id(?SPACE1_PLACEHOLDER),
    
    Name = generator:gen_name(),
    Guid = qos_tests_utils:create_file(Provider1, ?SESS_ID(Provider1), ?PATH(Name), ?TEST_DATA),
    {ok, QosEntryId} = opt_qos:add_qos_entry(P1Node, ?SESS_ID(Provider1), ?FILE_REF(Guid), <<"providerId=", Provider2/binary>>, 1),
    % wait for qos entries to be dbsynced to other provider
    ?assertMatch({ok, _}, opt_qos:get_qos_entry(P2Node, ?SESS_ID(Provider2), QosEntryId), ?ATTEMPTS),
    ?assertEqual({ok, ?FULFILLED_QOS_STATUS}, opt_qos:check_qos_status(P1Node, ?SESS_ID(Provider1), QosEntryId), ?ATTEMPTS),
    
    check_transfer_stats(Provider1, QosEntryId, ?BYTES_STATS, [<<"total">>], empty),
    check_transfer_stats(Provider2, QosEntryId, ?BYTES_STATS, [<<"total">>, opt_spaces:get_storage_id(Provider1, SpaceId)], {1, byte_size(?TEST_DATA)}),
    check_transfer_stats(Provider1, QosEntryId, ?FILES_STATS, [<<"total">>], empty),
    check_transfer_stats(Provider2, QosEntryId, ?FILES_STATS, [<<"total">>, opt_spaces:get_storage_id(Provider2, SpaceId)], {1, 1}),
    
    NewData = crypto:strong_rand_bytes(8),
    lfm_test_utils:write_file(P3Node, ?SESS_ID(Provider3), Guid, NewData),

    ?assertEqual(NewData, lfm_test_utils:read_file(P2Node, ?SESS_ID(Provider2), Guid, byte_size(NewData)), ?ATTEMPTS),
    ?assertEqual({ok, ?FULFILLED_QOS_STATUS}, opt_qos:check_qos_status(P2Node, ?SESS_ID(Provider2), QosEntryId), ?ATTEMPTS),
    
    check_transfer_stats(Provider1, QosEntryId, ?BYTES_STATS, [<<"total">>], empty),
    check_transfer_stats(Provider2, QosEntryId, ?BYTES_STATS, [opt_spaces:get_storage_id(Provider1, SpaceId)], {1, byte_size(?TEST_DATA)}),
    check_transfer_stats(Provider2, QosEntryId, ?BYTES_STATS, [opt_spaces:get_storage_id(Provider3, SpaceId)], {1, byte_size(NewData)}),
    check_transfer_stats(Provider2, QosEntryId, ?BYTES_STATS, [<<"total">>], {2, byte_size(NewData) + byte_size(?TEST_DATA)}),
    check_transfer_stats(Provider1, QosEntryId, ?FILES_STATS, [<<"total">>], empty),
    check_transfer_stats(Provider2, QosEntryId, ?FILES_STATS, [<<"total">>, opt_spaces:get_storage_id(Provider2, SpaceId)], {2, 2}).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite([{?LOAD_MODULES, [?MODULE, qos_tests_utils, dir_stats_test_utils]} | Config],
        #onenv_test_config{
            onenv_scenario = "3op",
            envs = [{op_worker, op_worker, [
                {fuse_session_grace_period_seconds, 24 * 60 * 60},
                {provider_token_ttl_sec, 24 * 60 * 60}
            ]}],
            posthook = fun(NewConfig) ->
                dir_stats_test_utils:disable_stats_counting(NewConfig),
                [Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),
                SpaceId = oct_background:get_space_id(?SPACE1_PLACEHOLDER),
                qos_tests_utils:set_qos_parameters(Provider1, opt_spaces:get_storage_id(Provider1, SpaceId), #{
                    <<"type">> => <<"disk">>,
                    <<"tier">> => <<"t3">>,
                    <<"param1">> => <<"val1">>
                }),
                qos_tests_utils:set_qos_parameters(Provider2, opt_spaces:get_storage_id(Provider2, SpaceId), #{
                    <<"type">> => <<"tape">>,
                    <<"tier">> => <<"t2">>
                }),
                qos_tests_utils:set_qos_parameters(Provider3, opt_spaces:get_storage_id(Provider3, SpaceId), #{
                    <<"type">> => <<"disk">>,
                    <<"tier">> => <<"t2">>,
                    <<"param1">> => <<"val1">>
                }),
                NewConfig
            end
        }).


end_per_suite(Config) ->
    oct_background:end_per_suite(),
    dir_stats_test_utils:enable_stats_counting(Config).


init_per_testcase(qos_transfer_stats_test, Config) ->
    time_test_utils:freeze_time(Config),
    init_per_testcase(default, Config);
init_per_testcase(_, Config) ->
    lfm_proxy:init(Config),
    Config.


end_per_testcase(qos_transfer_stats_test, Config) ->
    time_test_utils:unfreeze_time(Config),
    end_per_testcase(default, Config);
end_per_testcase(_, Config) ->
    qos_tests_utils:finish_all_transfers(),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================

run_tests(FileTypes, SourceProvider, TargetProviders, TestSpecFun) ->
    lists:foreach(fun
        (file) ->
            ct:pal("Starting for file"),
            Filename = generator:gen_name(),
            InitialDirStructure = get_initial_structure_with_single_file(SourceProvider, Filename),
            ExpectedDirStructure = get_expected_structure_for_single_file(TargetProviders, Filename),
            qos_tests_utils:fulfill_qos_test_base(TestSpecFun(?PATH(Filename), InitialDirStructure, ExpectedDirStructure));
        (dir) ->
            ct:pal("Starting for dir"),
            Name = generator:gen_name(),
            InitialDirStructure = get_initial_structure_with_single_dir(SourceProvider, Name),
            ExpectedDirStructure = get_expected_structure_for_single_dir(TargetProviders, Name),
            qos_tests_utils:fulfill_qos_test_base(TestSpecFun(?PATH(?DIRNAME(Name)), InitialDirStructure, ExpectedDirStructure))
    end, FileTypes).


get_initial_structure_with_single_file(Provider, Filename) ->
    #test_dir_structure{
        provider = Provider,
        dir_structure = {?SPACE_NAME, [
            {Filename, <<"test_data">>, [Provider]}
        ]}
    }.


get_expected_structure_for_single_file(ProviderIdList, Filename) ->
    #test_dir_structure{
        dir_structure = {?SPACE_NAME, [
            {Filename, <<"test_data">>, ProviderIdList}
        ]}
    }.



get_initial_structure_with_single_dir(Provider, Name) ->
    #test_dir_structure{
        provider = Provider,
        dir_structure = {?SPACE_NAME, [
            {?DIRNAME(Name), [
                {?FILENAME(Name), <<"test_data">>, [Provider]}  
            ]}
        ]}
    }.


get_expected_structure_for_single_dir(ProviderIdList, Name) ->
    #test_dir_structure{
        dir_structure = {?SPACE_NAME, [
            {?DIRNAME(Name), [
                {?FILENAME(Name), <<"test_data">>, ProviderIdList}
            ]}
        ]}
    }.


storage_file_path(Node, SpaceId, FilePath) ->
    SpaceMnt = get_space_mount_point(Node, SpaceId),
    filename:join([SpaceMnt, SpaceId, FilePath]).


get_space_mount_point(Node, SpaceId) ->
    {ok, StorageId} = opw_test_rpc:call(Node, space_logic, get_local_supporting_storage, [SpaceId]),
    storage_mount_point(Node, StorageId).


storage_mount_point(Node, StorageId) ->
    Helper = opw_test_rpc:call(Node, storage, get_helper, [StorageId]),
    HelperArgs = helper:get_args(Helper),
    maps:get(<<"mountPoint">>, HelperArgs).


read_file(Node, FilePath) ->
    opw_test_rpc:call(Node, file, read_file, [FilePath]).


check_transfer_stats(Provider, QosEntryId, Type, ExpectedSeries, ExpectedValue) ->
    {ok, Stats} = opw_test_rpc:call(Provider, qos_transfer_stats, list_windows, [QosEntryId, Type]),
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
