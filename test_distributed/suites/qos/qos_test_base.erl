%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MNonExistingProviderId license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains QoS specification for tests (includes specification
%%% of requirements, expected qos_entry records, expected file_qos /
%%% effective_file_qos records).
%%% @end
%%%--------------------------------------------------------------------
-module(qos_test_base).
-author("Michal Cwiertnia").


-include("qos_tests_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

% QoS specification for tests
-export([
    % Single QoS expression specs
    simple_key_val_qos_spec/3,
    qos_with_intersection_spec/3,
    qos_with_complement_spec/3,
    qos_with_union_spec/3,
    qos_with_multiple_replicas_spec/3,
    qos_with_multiple_replicas_and_union_spec/3,
    qos_with_intersection_and_union_spec/3,
    qos_with_union_and_complement_spec/3,
    qos_with_intersection_and_complement_spec/3,
    key_val_qos_that_cannot_be_fulfilled_spec/3,
    qos_that_cannot_be_fulfilled_spec/3,
    qos_with_parens_spec/3,

    % Multi QoS for single file specs
    multi_qos_resulting_in_the_same_storages_spec/3,
    same_qos_multiple_times_spec/3,
    contrary_qos_spec/3,
    multi_qos_where_one_cannot_be_satisfied_spec/3,
    multi_qos_that_overlaps_spec/3,
    multi_qos_resulting_in_different_storages_spec/3,

    % Effective QoS specs
    effective_qos_for_file_in_directory_spec/4,
    effective_qos_for_file_in_nested_directories_spec/4,
    effective_qos_for_files_in_different_directories_of_tree_structure_spec/4,
    
    % QoS status test bases
    qos_status_during_traverse_test_base/1,
    qos_status_during_traverse_with_hardlinks_test_base/0,
    qos_status_during_traverse_with_file_deletion_test_base/2,
    qos_status_during_traverse_with_dir_deletion_test_base/2,
    qos_status_during_traverse_file_without_qos_test_base/0,
    qos_status_during_reconciliation_test_base/2,
    qos_status_during_reconciliation_with_file_deletion_test_base/2,
    qos_status_during_reconciliation_with_dir_deletion_test_base/2,
    qos_status_after_failed_transfer/1,
    qos_status_after_failed_transfer_deleted_file/1,
    qos_status_after_failed_transfer_deleted_entry/1,
    
    % QoS with hardlinks test bases
    qos_with_hardlink_test_base/1,
    qos_with_hardlink_deletion_test_base/1,
    qos_on_symlink_test_base/0,
    effective_qos_with_symlink_test_base/0,
    create_hardlink_in_dir_with_qos/0
]).

-export([
    init_per_testcase/1, end_per_testcase/1
]).

% file_type can be one of 
%   * reg_file - created file is a regular file
%   * hardlink - created file is a hardlink
%   * random - created file is randomly chosen between regular file and hardlink
-type file_type() :: reg_file | hardlink | random.

-define(ATTEMPTS, 60).
-define(USER_PLACEHOLDER, user2).
-define(SPACE_PLACEHOLDER, space1).
-define(SPACE_NAME, <<"space1">>).
-define(SESS_ID(ProviderPlaceholder), oct_background:get_user_session_id(?USER_PLACEHOLDER, ProviderPlaceholder)).

%%%===================================================================
%%% Group of tests that adds single QoS expression for file or directory
%%% and checks QoS docs.
%%% Each test case is executed once for file and once for directory.
%%%===================================================================

simple_key_val_qos_spec(Path, ProviderAddingQos, AssertionProviders) ->
    [Provider1 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"providerId=", Provider1/binary>>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"providerId=", Provider1/binary>>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{qos_tests_utils:get_storage_id(Provider1, SpaceId) => [?QOS1]}
            }
        ]
    }.


qos_with_intersection_spec(Path, ProviderAddingQos, AssertionProviders) ->
    [_Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=disk & tier=t2">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=disk">>, <<"tier=t2">>, <<"&">>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{qos_tests_utils:get_storage_id(Provider3, SpaceId) => [?QOS1]}
            }
        ]
    }.


qos_with_complement_spec(Path, ProviderAddingQos, AssertionProviders) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=disk\\providerId=", Provider3/binary>>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=disk">>, <<"providerId=", Provider3/binary>>, <<"-">>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{qos_tests_utils:get_storage_id(Provider1, SpaceId) => [?QOS1]}
            }
        ]
    }.


qos_with_union_spec(Path, ProviderAddingQos, AssertionProviders) ->
    [Provider1, _Provider2, _Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"providerId=", Provider1/binary, "|tier=t3">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"providerId=", Provider1/binary>>, <<"tier=t3">>, <<"|">>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{qos_tests_utils:get_storage_id(Provider1, SpaceId) => [?QOS1]}
            }
        ]
    }.


qos_with_multiple_replicas_spec(Path, ProviderAddingQos, AssertionProviders) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=disk">>,
                replicas_num = 2
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=disk">>],
                replicas_num = 2,
                possibility_check = {possible, ProviderAddingQos}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{
                    qos_tests_utils:get_storage_id(Provider1, SpaceId) => [?QOS1],
                    qos_tests_utils:get_storage_id(Provider3, SpaceId) => [?QOS1]
                }
            }
        ]
    }.


qos_with_intersection_and_union_spec(Path, ProviderAddingQos, AssertionProviders) ->
    [_Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=disk&tier=t2|providerId=", Provider2/binary>>,
                replicas_num = 2
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=disk">>, <<"tier=t2">>, <<"&">>, <<"providerId=", Provider2/binary>>, <<"|">>],
                replicas_num = 2,
                possibility_check = {possible, ProviderAddingQos}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{
                    qos_tests_utils:get_storage_id(Provider2, SpaceId) => [?QOS1],
                    qos_tests_utils:get_storage_id(Provider3, SpaceId) => [?QOS1]
                }
            }
        ]
    }.


qos_with_union_and_complement_spec(Path, ProviderAddingQos, AssertionProviders) ->
    [Provider1, Provider2, _Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"providerId=", Provider1/binary, "|providerId=", Provider2/binary, "\\type=tape">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"providerId=", Provider1/binary>>, <<"providerId=", Provider2/binary>>, <<"|">>, <<"type=tape">>, <<"-">>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{qos_tests_utils:get_storage_id(Provider1, SpaceId) => [?QOS1]}
            }
        ]
    }.


qos_with_intersection_and_complement_spec(Path, ProviderAddingQos, AssertionProviders) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=disk & param1=val1 \\ providerId=", Provider1/binary>>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=disk">>, <<"param1=val1">>, <<"&">>, <<"providerId=", Provider1/binary>>, <<"-">>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{qos_tests_utils:get_storage_id(Provider3, SpaceId) => [?QOS1]}
            }
        ]
    }.


qos_with_multiple_replicas_and_union_spec(Path, ProviderAddingQos, AssertionProviders) ->
    [Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"providerId=", Provider1/binary, "| providerId=", Provider2/binary, "| providerId=", Provider3/binary>>,
                replicas_num = 3
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"providerId=", Provider1/binary>>, <<"providerId=", Provider2/binary>>, <<"|">>, <<"providerId=", Provider3/binary>>, <<"|">>],
                replicas_num = 3,
                possibility_check = {possible, ProviderAddingQos}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{
                    qos_tests_utils:get_storage_id(Provider1, SpaceId) => [?QOS1],
                    qos_tests_utils:get_storage_id(Provider2, SpaceId) => [?QOS1],
                    qos_tests_utils:get_storage_id(Provider3, SpaceId) => [?QOS1]
                }
            }
        ]
    }.


key_val_qos_that_cannot_be_fulfilled_spec(Path, ProviderAddingQos, AssertionProviders) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"providerId=NonExistingProviderId">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"providerId=NonExistingProviderId">>],
                replicas_num = 1,
                possibility_check = {impossible, ProviderAddingQos}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{}
            }
        ]
    }.


qos_that_cannot_be_fulfilled_spec(Path, ProviderAddingQos, AssertionProviders) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"providerId=", Provider1/binary, "|providerId=", Provider3/binary, "\\type=disk">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"providerId=", Provider1/binary>>, <<"providerId=", Provider3/binary>>, <<"|">>, <<"type=disk">>, <<"-">>],
                replicas_num = 1,
                possibility_check = {impossible, ProviderAddingQos}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{}
            }
        ]
    }.


qos_with_parens_spec(Path, ProviderAddingQos, AssertionProviders) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"providerId=", Provider1/binary, "| (providerId=", Provider3/binary, "\\ type=disk)">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"providerId=", Provider1/binary>>, <<"providerId=", Provider3/binary>>, <<"type=disk">>, <<"-">>, <<"|">>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos}

            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{qos_tests_utils:get_storage_id(Provider1, SpaceId) => [?QOS1]}
            }
        ]
    }.


%%%===================================================================
%%% Group of tests that adds multiple QoS expression for single file or
%%% directory and checks QoS docs.
%%% Each test case is executed once for file and once for directory.
%%%===================================================================

multi_qos_resulting_in_different_storages_spec(
    Path, ProviderAddingQos, AssertionProviders
) when not is_list(ProviderAddingQos) ->
    multi_qos_resulting_in_different_storages_spec(
        Path, [ProviderAddingQos, ProviderAddingQos], AssertionProviders
    );

multi_qos_resulting_in_different_storages_spec(
    Path, [ProviderAddingQos1, ProviderAddingQos2], AssertionProviders
) ->
    [_Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos1,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=disk&tier=t2">>,
                replicas_num = 1
            },
            #qos_to_add{
                provider = ProviderAddingQos2,
                path = Path,
                qos_name = ?QOS2,
                expression = <<"providerId=", Provider2/binary>>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=disk">>, <<"tier=t2">>, <<"&">>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos1}
            },
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression = [<<"providerId=", Provider2/binary>>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos2}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1, ?QOS2],
                assigned_entries = #{
                    qos_tests_utils:get_storage_id(Provider2, SpaceId) => [?QOS2],
                    qos_tests_utils:get_storage_id(Provider3, SpaceId) => [?QOS1]
                }
            }
        ]
    }.


multi_qos_resulting_in_the_same_storages_spec(
    Path, ProviderAddingQos, AssertionProviders
) when not is_list(ProviderAddingQos) ->
    multi_qos_resulting_in_the_same_storages_spec(
        Path, [ProviderAddingQos, ProviderAddingQos], AssertionProviders
    );

multi_qos_resulting_in_the_same_storages_spec(
    Path, [ProviderAddingQos1, ProviderAddingQos2], AssertionProviders
) ->
    [_Provider1, Provider2, _Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos1,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=tape">>,
                replicas_num = 1
            },
            #qos_to_add{
                provider = ProviderAddingQos2,
                path = Path,
                qos_name = ?QOS2,
                expression = <<"providerId=", Provider2/binary>>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=tape">>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos1}
            },
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression = [<<"providerId=", Provider2/binary>>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos2}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1, ?QOS2],
                assigned_entries = #{
                    qos_tests_utils:get_storage_id(Provider2, SpaceId) => [?QOS1, ?QOS2]
                }
            }
        ]
    }.


same_qos_multiple_times_spec(
    Path, ProviderAddingQos, AssertionProviders
) when not is_list(ProviderAddingQos) ->
    same_qos_multiple_times_spec(
        Path, [ProviderAddingQos, ProviderAddingQos, ProviderAddingQos], AssertionProviders
    );

same_qos_multiple_times_spec(
    Path, [ProviderAddingQos1, ProviderAddingQos2, ProviderAddingQos3], AssertionProviders
) ->
    [_Provider1, Provider2, _Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos1,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=tape">>,
                replicas_num = 1
            },
            #qos_to_add{
                provider = ProviderAddingQos2,
                path = Path,
                qos_name = ?QOS2,
                expression = <<"type=tape">>,
                replicas_num = 1
            },
            #qos_to_add{
                provider = ProviderAddingQos3,
                path = Path,
                qos_name = ?QOS3,
                expression = <<"type=tape">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=tape">>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos1}
            },
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression = [<<"type=tape">>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos2}
            },
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS3,
                file_key = {path, Path},
                qos_expression = [<<"type=tape">>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos3}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1, ?QOS2, ?QOS3],
                assigned_entries = #{qos_tests_utils:get_storage_id(Provider2, SpaceId) => [?QOS1, ?QOS2, ?QOS3]}
            }
        ]
    }.


contrary_qos_spec(Path, ProviderAddingQos, AssertionProviders) when not is_list(ProviderAddingQos) ->
    contrary_qos_spec(Path, [ProviderAddingQos, ProviderAddingQos], AssertionProviders);

contrary_qos_spec(Path, [ProviderAddingQos1, ProviderAddingQos2], AssertionProviders) ->
    [Provider1, Provider2, _Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos1,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"providerId=", Provider1/binary>>,
                replicas_num = 1
            },
            #qos_to_add{
                provider = ProviderAddingQos2,
                path = Path,
                qos_name = ?QOS2,
                expression = <<"type=tape \\ providerId=", Provider1/binary>>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"providerId=", Provider1/binary>>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos1}
            },
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression = [<<"type=tape">>, <<"providerId=", Provider1/binary>>, <<"-">>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos2}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1, ?QOS2],
                assigned_entries = #{
                    qos_tests_utils:get_storage_id(Provider1, SpaceId) => [?QOS1],
                    qos_tests_utils:get_storage_id(Provider2, SpaceId) => [?QOS2]
                }
            }
        ]
    }.


multi_qos_where_one_cannot_be_satisfied_spec(
    Path, ProviderAddingQos, AssertionProviders
) when not is_list(ProviderAddingQos) ->
    multi_qos_where_one_cannot_be_satisfied_spec(
        Path, [ProviderAddingQos, ProviderAddingQos], AssertionProviders
    );

multi_qos_where_one_cannot_be_satisfied_spec(
    Path, [ProviderAddingQos1, ProviderAddingQos2], AssertionProviders
) ->
    [_Provider1, Provider2, _Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos1,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"providerId=", Provider2/binary>>,
                replicas_num = 1
            },
            #qos_to_add{
                provider = ProviderAddingQos2,
                path = Path,
                qos_name = ?QOS2,
                expression = <<"providerId=NonExistingProviderId">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"providerId=", Provider2/binary>>],
                replicas_num = 1,
                possibility_check = {possible, ProviderAddingQos1}
            },
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression = [<<"providerId=NonExistingProviderId">>],
                replicas_num = 1,
                possibility_check = {impossible, ProviderAddingQos2}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1, ?QOS2],
                assigned_entries = #{qos_tests_utils:get_storage_id(Provider2, SpaceId) => [?QOS1]}
            }
        ]
    }.


multi_qos_that_overlaps_spec(Path, ProviderAddingQos, AssertionProviders) when not is_list(ProviderAddingQos) ->
    multi_qos_that_overlaps_spec(
        Path, [ProviderAddingQos, ProviderAddingQos], AssertionProviders
    );

multi_qos_that_overlaps_spec(Path, [ProviderAddingQos1, ProviderAddingQos2], AssertionProviders) ->
    [Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos1,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=disk">>,
                replicas_num = 2
            },
            #qos_to_add{
                provider = ProviderAddingQos2,
                path = Path,
                qos_name = ?QOS2,
                expression = <<"tier=t2">>,
                replicas_num = 2
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=disk">>],
                replicas_num = 2,
                possibility_check = {possible, ProviderAddingQos1}
            },
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression = [<<"tier=t2">>],
                replicas_num = 2,
                possibility_check = {possible, ProviderAddingQos2}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = Path,
                qos_entries = [?QOS1, ?QOS2],
                assigned_entries = #{
                    qos_tests_utils:get_storage_id(Provider1, SpaceId) => [?QOS1],
                    qos_tests_utils:get_storage_id(Provider2, SpaceId) => [?QOS2],
                    qos_tests_utils:get_storage_id(Provider3, SpaceId) => [?QOS1, ?QOS2]
                }
            }
        ]
    }.


%%%===================================================================
%%% Group of tests that creates directory structure, adds QoS on different
%%% levels of created structure and checks effective QoS and QoS docs.
%%%===================================================================

effective_qos_for_file_in_directory_spec(DirPath, FilePath, ProviderAddingQos, AssertionProviders) ->
    [Provider1 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos,
                path = DirPath,
                qos_name = ?QOS1,
                expression = <<"providerId=", Provider1/binary>>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                qos_expression = [<<"providerId=", Provider1/binary>>],
                replicas_num = 1,
                file_key = {path, DirPath},
                possibility_check = {possible, ProviderAddingQos}
            }
        ],
        expected_effective_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = FilePath,
                qos_entries = [?QOS1],
                assigned_entries = #{qos_tests_utils:get_storage_id(Provider1, SpaceId) => [?QOS1]}
            }
        ]
    }.


effective_qos_for_file_in_nested_directories_spec(
    [Dir1Path, Dir2Path, Dir3Path], [FileInDir2Path, FileInDir3Path], ProviderAddingQos, AssertionProviders
) when not is_list(ProviderAddingQos) ->
    effective_qos_for_file_in_nested_directories_spec(
        [Dir1Path, Dir2Path, Dir3Path], [FileInDir2Path, FileInDir3Path],
        [ProviderAddingQos, ProviderAddingQos, ProviderAddingQos], AssertionProviders
    );

effective_qos_for_file_in_nested_directories_spec(
    [Dir1Path, Dir2Path, Dir3Path], [FileInDir2Path, FileInDir3Path],
    [ProviderAddingQos1, ProviderAddingQos2, ProviderAddingQos3], AssertionProviders
) ->
    [Provider1, Provider2 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos1,
                path = Dir1Path,
                qos_name = ?QOS1,
                expression = <<"providerId=", Provider1/binary>>,
                replicas_num = 1
            },
            #qos_to_add{
                provider = ProviderAddingQos2,
                path = Dir2Path,
                qos_name = ?QOS2,
                expression = <<"providerId=", Provider1/binary>>,
                replicas_num = 1
            },
            #qos_to_add{
                provider = ProviderAddingQos3,
                path = Dir3Path,
                qos_name = ?QOS3,
                expression = <<"providerId=", Provider2/binary>>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                qos_expression = [<<"providerId=", Provider1/binary>>],
                replicas_num = 1,
                file_key = {path, Dir1Path},
                possibility_check = {possible, ProviderAddingQos1}
            },
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS2,
                qos_expression = [<<"providerId=", Provider1/binary>>],
                replicas_num = 1,
                file_key = {path, Dir2Path},
                possibility_check = {possible, ProviderAddingQos2}
            },
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS3,
                qos_expression = [<<"providerId=", Provider2/binary>>],
                replicas_num = 1,
                file_key = {path, Dir3Path},
                possibility_check = {possible, ProviderAddingQos3}
            }
        ],
        expected_effective_qos = [
            #expected_file_qos{
                path = FileInDir2Path,
                qos_entries = [?QOS1, ?QOS2],
                assigned_entries = #{
                    qos_tests_utils:get_storage_id(Provider1, SpaceId) => [?QOS1, ?QOS2]
                }
            },
            #expected_file_qos{
                path = FileInDir3Path,
                qos_entries = [?QOS1, ?QOS2, ?QOS3],
                assigned_entries = #{
                    qos_tests_utils:get_storage_id(Provider1, SpaceId) => [?QOS1, ?QOS2],
                    qos_tests_utils:get_storage_id(Provider2, SpaceId) => [?QOS3]
                }
            }
        ]
    }.


effective_qos_for_files_in_different_directories_of_tree_structure_spec(
    [Dir1Path, Dir2Path, Dir3Path], [FileInDir2Path, FileInDir3Path], ProviderAddingQos, AssertionProviders
) when not is_list(ProviderAddingQos) ->
    effective_qos_for_files_in_different_directories_of_tree_structure_spec(
        [Dir1Path, Dir2Path, Dir3Path], [FileInDir2Path, FileInDir3Path],
        [ProviderAddingQos, ProviderAddingQos, ProviderAddingQos], AssertionProviders
    );

effective_qos_for_files_in_different_directories_of_tree_structure_spec(
    [Dir1Path, Dir2Path, Dir3Path], [FileInDir2Path, FileInDir3Path],
    [ProviderAddingQos1, ProviderAddingQos2, ProviderAddingQos3], AssertionProviders
) ->
    [Provider1, Provider2 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                provider = ProviderAddingQos1,
                path = Dir1Path,
                qos_name = ?QOS1,
                expression = <<"providerId=", Provider1/binary>>,
                replicas_num = 1
            },
            #qos_to_add{
                provider = ProviderAddingQos2,
                path = Dir2Path,
                qos_name = ?QOS2,
                expression = <<"providerId=", Provider1/binary>>,
                replicas_num = 1
            },
            #qos_to_add{
                provider = ProviderAddingQos3,
                path = Dir3Path,
                qos_name = ?QOS3,
                expression = <<"providerId=", Provider2/binary>>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS1,
                qos_expression = [<<"providerId=", Provider1/binary>>],
                replicas_num = 1,
                file_key = {path, Dir1Path},
                possibility_check = {possible, ProviderAddingQos1}
            },
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS2,
                qos_expression = [<<"providerId=", Provider1/binary>>],
                replicas_num = 1,
                file_key = {path, Dir2Path},
                possibility_check = {possible, ProviderAddingQos2}
            },
            #expected_qos_entry{
                providers = AssertionProviders,
                qos_name = ?QOS3,
                qos_expression = [<<"providerId=", Provider2/binary>>],
                replicas_num = 1,
                file_key = {path, Dir3Path},
                possibility_check = {possible, ProviderAddingQos3}
            }
        ],
        expected_effective_qos = [
            #expected_file_qos{
                providers = AssertionProviders,
                path = FileInDir3Path,
                qos_entries = [?QOS1, ?QOS3],
                assigned_entries = #{
                    qos_tests_utils:get_storage_id(Provider1, SpaceId) => [?QOS1],
                    qos_tests_utils:get_storage_id(Provider2, SpaceId) => [?QOS3]
                }
            },
            #expected_file_qos{
                providers = AssertionProviders,
                path = FileInDir2Path,
                qos_entries = [?QOS1, ?QOS2],
                assigned_entries = #{
                    qos_tests_utils:get_storage_id(Provider1, SpaceId) => [?QOS1, ?QOS2]
                }
            }
        ]
    }.


%%%===================================================================
%%% QoS status tests bases
%%%===================================================================

qos_status_during_traverse_test_base(NumberOfFilesInDir) ->
    [Provider1 | _] = oct_background:get_provider_ids(),
    Name = generator:gen_name(),
    DirStructure =
        {?SPACE_NAME, [
            {Name, [ % Dir1
                {?filename(Name, 1), % Dir2
                    lists:map(fun(Num) -> {?filename(Name, Num), ?TEST_DATA, [Provider1]} end, lists:seq(1, NumberOfFilesInDir)) % Guids2
                },
                {?filename(Name, 2), [ % Dir3
                    {?filename(Name, 1), % Dir4
                        lists:map(fun(Num) -> {?filename(Name, Num), ?TEST_DATA, [Provider1]} end, lists:seq(1, NumberOfFilesInDir)) % Guids3
                    }
                ]} ] ++ 
                lists:map(fun(Num) -> {?filename(Name, Num), ?TEST_DATA, [Provider1]} end, lists:seq(3, 2 + NumberOfFilesInDir)) % Guids1
            }
        ]},
    
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(DirStructure, ?SPACE_NAME, Name),

    Guids1 = lists:map(fun(Num) ->
        qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, [Num]), GuidsAndPaths)
    end, lists:seq(3, 2 + NumberOfFilesInDir)),
    
    Guids2 = lists:map(fun(Num) ->
        qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, [1, Num]), GuidsAndPaths)
    end, lists:seq(1, NumberOfFilesInDir)),
    
    Guids3 = lists:map(fun(Num) ->
        qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, [2, 1, Num]), GuidsAndPaths)
    end, lists:seq(1, NumberOfFilesInDir)),
    
    Dir1 = qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, []), GuidsAndPaths),
    Dir2 = qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, [1]), GuidsAndPaths),
    Dir3 = qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, [2]), GuidsAndPaths),
    Dir4 = qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, [2,1]), GuidsAndPaths),
    
    
    ok = qos_tests_utils:finish_transfers(Guids1),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes(Guids1, QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes(Guids2 ++ Guids3 ++ [Dir1, Dir2, Dir3, Dir4], QosList, ?PENDING_QOS_STATUS), ?ATTEMPTS),
    
    ok = qos_tests_utils:finish_transfers(Guids2),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes(Guids1 ++ Guids2 ++ [Dir2], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes(Guids3 ++ [Dir1, Dir3, Dir4], QosList, ?PENDING_QOS_STATUS), ?ATTEMPTS),
    
    ok = qos_tests_utils:finish_transfers(Guids3),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes(Guids1 ++ Guids2 ++ Guids3 ++ [Dir1, Dir2, Dir3, Dir4], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS).


qos_status_during_traverse_with_hardlinks_test_base() ->
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    [Provider1, Provider2 | _] = oct_background:get_provider_ids(),
    P1Node = oct_background:get_random_provider_node(Provider1),
    SpaceGuid = rpc:call(P1Node, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    {ok, Dir1Guid} = lfm_proxy:mkdir(P1Node, ?SESS_ID(Provider1), SpaceGuid, generator:gen_name(), ?DEFAULT_DIR_PERMS),
    {ok, Dir2Guid} = lfm_proxy:mkdir(P1Node, ?SESS_ID(Provider1), SpaceGuid, generator:gen_name(), ?DEFAULT_DIR_PERMS),
    
    {ok, FileGuid1} = lfm_proxy:create(P1Node, ?SESS_ID(Provider1), Dir1Guid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, FileGuid2} = lfm_proxy:create(P1Node, ?SESS_ID(Provider1), Dir1Guid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_link(P1Node, ?SESS_ID(Provider1), ?FILE_REF(FileGuid1), ?FILE_REF(Dir2Guid), generator:gen_name()),
    
    AllNodes = oct_background:get_all_providers_nodes(),
    await_files_sync_between_nodes(AllNodes, [FileGuid1, FileGuid2, LinkGuid]),
    qos_tests_utils:mock_transfers(AllNodes),
    
    {ok, QosEntryId} = lfm_proxy:add_qos_entry(P1Node, ?SESS_ID(Provider1), ?FILE_REF(Dir1Guid), <<"providerId=", Provider2/binary>>, 1),
    assert_effective_entry(P1Node, ?SESS_ID(Provider1), QosEntryId, [FileGuid1, FileGuid2, LinkGuid], []),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([FileGuid1, FileGuid2, LinkGuid], [QosEntryId], ?PENDING_QOS_STATUS), ?ATTEMPTS),
    qos_tests_utils:finish_transfers([FileGuid1]),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([FileGuid1, LinkGuid], [QosEntryId], ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([FileGuid2], [QosEntryId], ?PENDING_QOS_STATUS), ?ATTEMPTS),
    qos_tests_utils:finish_transfers([FileGuid2]),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([FileGuid1, FileGuid2, LinkGuid], [QosEntryId], ?FULFILLED_QOS_STATUS), ?ATTEMPTS).


-spec qos_status_during_traverse_with_file_deletion_test_base(pos_integer(), file_type()) -> ok.
qos_status_during_traverse_with_file_deletion_test_base(NumberOfFilesInDir, FileType) ->
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    [Provider1 | _] = oct_background:get_provider_ids(),
    P1Node = oct_background:get_random_provider_node(Provider1),
    Name = generator:gen_name(),
    SpaceGuid = rpc:call(P1Node, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    {ok, FileToLinkGuid} = lfm_proxy:create(P1Node, ?SESS_ID(Provider1), SpaceGuid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    DirStructure =
        {?SPACE_NAME, [
            {Name, % Dir1
                lists:map(fun(Num) ->
                    TypeSpec = prepare_type_spec(FileType, oct_background:get_all_providers_nodes(), {target, FileToLinkGuid}),
                    {?filename(Name, Num), ?TEST_DATA, [Provider1], TypeSpec} 
                end, lists:seq(1, NumberOfFilesInDir))
            }
        ]},
    
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(DirStructure, ?SPACE_NAME, Name),
    
    {ToFinish, ToDelete} = lists:foldl(fun(Num, {F, D}) ->
        Guid = qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, [Num]), GuidsAndPaths),
        case Num rem 2 of
            0 -> {[Guid | F], D};
            1 -> {F, [Guid | D]}
        end
    end, {[], []}, lists:seq(1, NumberOfFilesInDir)),
    
    lists:foreach(fun(Guid) ->
        ok = lfm_proxy:unlink(P1Node, ?SESS_ID(Provider1), ?FILE_REF(Guid))
    end, ToDelete),
    
    ok = qos_tests_utils:finish_transfers(ToFinish),
    
    Dir1 = qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, []), GuidsAndPaths),
    {StillReferenced, NoLongerReferenced} = lists:foldl(fun(Guid, {StillReferencedAcc, NoLongerReferencedAcc}) ->
        case lfm_proxy:get_file_references(P1Node, ?SESS_ID(Provider1), ?FILE_REF(Guid)) of
            {ok, []} -> {StillReferencedAcc, [Guid | NoLongerReferencedAcc]};
            {ok, [_ | _]} -> {[Guid | StillReferencedAcc], NoLongerReferencedAcc}
        end
    end, {[], []}, ToDelete),
    ok = ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes(ToFinish ++ [Dir1] ++ StillReferenced, QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    % finish transfers to unlock waiting slave job processes
    ok = qos_tests_utils:finish_transfers(ToDelete),
    % These files where deleted so QoS is not fulfilled
    ok = ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes(NoLongerReferenced, QosList, {error, enoent}), ?ATTEMPTS).


-spec qos_status_during_traverse_with_dir_deletion_test_base(pos_integer(), file_type()) -> ok.
qos_status_during_traverse_with_dir_deletion_test_base(NumberOfFilesInDir, FileType) ->
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    [Provider1 | _] = oct_background:get_provider_ids(),
    P1Node = oct_background:get_random_provider_node(Provider1),
    Name = generator:gen_name(),
    SpaceGuid = rpc:call(P1Node, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    {ok, FileToLinkGuid} = lfm_proxy:create(P1Node, ?SESS_ID(Provider1), SpaceGuid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    DirStructure =
        {?SPACE_NAME, [
            {Name, [ % Dir1
                {?filename(Name, 1), % Dir2
                    lists:map(fun(Num) ->
                        TypeSpec = prepare_type_spec(FileType, oct_background:get_all_providers_nodes(), {target, FileToLinkGuid}),
                        {?filename(Name, Num), ?TEST_DATA, [Provider1], TypeSpec}
                    end, lists:seq(1, NumberOfFilesInDir))
                }
            ]}
        ]},

    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(DirStructure, ?SPACE_NAME, Name),
    
    Dir1 = qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, []), GuidsAndPaths),
    Dir2 = qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, [1]), GuidsAndPaths),
    
    ok = lfm_proxy:rm_recursive(P1Node, ?SESS_ID(Provider1), ?FILE_REF(Dir2)),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([Dir1], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    % finish transfers to unlock waiting slave job processes
    ok = qos_tests_utils:finish_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)]).


qos_status_during_traverse_file_without_qos_test_base() ->
    [Provider1 | _] = oct_background:get_provider_ids(),
    P1Node = oct_background:get_random_provider_node(Provider1),
    
    Name = generator:gen_name(),
    DirStructure =
        {?SPACE_NAME, [
            {Name, [ % Dir1
                {?filename(Name, 1), 
                    [{?filename(Name, 0), ?TEST_DATA, [Provider1]}]
                }]
            }
        ]},
    
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(DirStructure, ?SPACE_NAME, filename:join(Name, ?filename(Name,1))),
    Dir1 = qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, []), GuidsAndPaths),
    
    % create file outside QoS range
    {ok, {FileGuid, FileHandle}} = lfm_proxy:create_and_open(P1Node, ?SESS_ID(Provider1), Dir1, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, _} = lfm_proxy:write(P1Node, FileHandle, 0, <<"new_data">>),
    ok = lfm_proxy:close(P1Node, FileHandle),
    lists:foreach(fun(N) ->
        ?assertMatch({ok, _}, lfm_proxy:stat(N, ?ROOT_SESS_ID, ?FILE_REF(FileGuid)), ?ATTEMPTS)
    end, oct_background:get_all_providers_nodes()),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([Dir1, FileGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    % finish transfer to unlock waiting slave job process
    ok = qos_tests_utils:finish_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)]).
    

qos_status_during_reconciliation_test_base(DirStructure, Filename) ->
    [Provider1 | _] = oct_background:get_provider_ids(),
    P1Node = oct_background:get_random_provider_node(Provider1),
    
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(DirStructure, ?SPACE_NAME, Filename),
    
    ok = qos_tests_utils:finish_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)]),
    
    FilesAndDirs = maps:get(files, GuidsAndPaths) ++ maps:get(dirs, GuidsAndPaths),
    FilesAndDirsGuids = lists:map(fun({G, _}) -> G end, FilesAndDirs),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes(FilesAndDirsGuids, QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    IsAncestor = fun
        (F, F) -> true;
        (A, F) -> str_utils:binary_starts_with(F, <<A/binary, "/">>)
    end,
    
    lists:foreach(fun({FileGuid, FilePath}) ->
        ct:pal("writing to file ~p on node ~p", [FilePath, P1Node]),
        {ok, FileHandle} = lfm_proxy:open(P1Node, ?SESS_ID(Provider1), ?FILE_REF(FileGuid), write),
        {ok, _} = lfm_proxy:write(P1Node, FileHandle, 0, <<"new_data">>),
        ok = lfm_proxy:close(P1Node, FileHandle),
        lists:foreach(fun({G, P}) ->
            ct:pal("Checking file: ~p~n\tis_ancestor: ~p", [P, IsAncestor(P, FilePath)]),
            ExpectedStatus = case IsAncestor(P, FilePath) of
                true -> ?PENDING_QOS_STATUS;
                false -> ?FULFILLED_QOS_STATUS
            end,
            ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([G], QosList, ExpectedStatus), ?ATTEMPTS)
        end, FilesAndDirs),
        ok = qos_tests_utils:finish_transfers([FileGuid]),
        ct:pal("Checking after finish"),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes(FilesAndDirsGuids, QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS)
    end, maps:get(files, GuidsAndPaths)).


-spec qos_status_during_reconciliation_with_file_deletion_test_base(pos_integer(), file_type()) -> ok.
qos_status_during_reconciliation_with_file_deletion_test_base(NumOfFiles, FileType) ->
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    [Provider1 | _] = oct_background:get_provider_ids(),
    P1Node = oct_background:get_random_provider_node(Provider1),
    Nodes = oct_background:get_all_providers_nodes(),
    Name = generator:gen_name(),
    DirStructure =
        {?SPACE_NAME, [
            {Name, % Dir1
                [{?filename(Name, 0), ?TEST_DATA, [Provider1]}]
            }
        ]},
    
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(DirStructure, ?SPACE_NAME, Name),
    
    TypeSpec = prepare_type_spec(FileType, Nodes, {target, create_link_target(P1Node, ?SESS_ID(Provider1), SpaceId)}),
    ok = qos_tests_utils:finish_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)]),
    
    FilesAndDirs = maps:get(files, GuidsAndPaths) ++ maps:get(dirs, GuidsAndPaths),
    FilesAndDirsGuids = lists:map(fun({G, _}) -> G end, FilesAndDirs),
    [{Dir1, _}] = maps:get(dirs, GuidsAndPaths),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes(FilesAndDirsGuids, QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    lists:foreach(fun(Provider) ->
        lists:foreach(fun(Node) ->
            Guids = create_files_and_write(P1Node, ?SESS_ID(Provider1), Dir1, TypeSpec, NumOfFiles),
            ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([Dir1 | Guids], QosList, ?PENDING_QOS_STATUS), ?ATTEMPTS),
            lists:foreach(fun(FileGuid) ->
                ok = lfm_proxy:unlink(Node, ?SESS_ID(Provider), ?FILE_REF(FileGuid))
            end, Guids),
            ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes(FilesAndDirsGuids, QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
            % finish transfer to unlock waiting slave job process
            ok = qos_tests_utils:finish_transfers(Guids, non_strict) % all hardlinks are to the same file so only one transfer started
        end, oct_background:get_provider_nodes(Provider))
    end, oct_background:get_provider_ids()).


-spec qos_status_during_reconciliation_with_dir_deletion_test_base(pos_integer(), file_type()) -> ok.
qos_status_during_reconciliation_with_dir_deletion_test_base(NumOfFiles, FileType) ->
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    [Provider1 | _] = oct_background:get_provider_ids(),
    P1Node = oct_background:get_random_provider_node(Provider1),
    Nodes = oct_background:get_all_providers_nodes(),
    Name = generator:gen_name(),
    DirStructure =
        {?SPACE_NAME, [
            {Name, % Dir1
                [{?filename(Name, 0), ?TEST_DATA, [Provider1]}]
            }
        ]},
    
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(DirStructure, ?SPACE_NAME, Name),
    Dir1 = qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, []), GuidsAndPaths),
    ok = qos_tests_utils:finish_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)]),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([Dir1], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    TypeSpec = prepare_type_spec(FileType, Nodes, {target, create_link_target(P1Node, ?SESS_ID(Provider1), SpaceId)}),
    
    lists:foreach(fun(Provider) ->
        lists:foreach(fun(Node) ->
        ct:print("Deleting node: ~p", [Node]), % log current deleting node for greater verbosity during failures
        {ok, DirGuid} = lfm_proxy:mkdir(P1Node, ?SESS_ID(Provider1), Dir1, generator:gen_name(), ?DEFAULT_DIR_PERMS),
        Guids = create_files_and_write(P1Node, ?SESS_ID(Provider1), DirGuid, TypeSpec, NumOfFiles),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([Dir1, DirGuid | Guids], QosList, ?PENDING_QOS_STATUS), ?ATTEMPTS),
        ok = lfm_proxy:rm_recursive(Node, ?SESS_ID(Provider), ?FILE_REF(DirGuid)),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([Dir1], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
        % finish transfer to unlock waiting slave job process
        ok = qos_tests_utils:finish_transfers(Guids, non_strict) % all hardlinks are to the same file so only one transfer started
        end, oct_background:get_provider_nodes(Provider))
    end, oct_background:get_provider_ids()).


qos_status_after_failed_transfer(TargetProvider) ->
    [Provider1 | _] = Providers = oct_background:get_provider_ids(),
    Nodes = oct_background:get_all_providers_nodes(),
    qos_tests_utils:mock_replica_synchronizer(Nodes, {error, some_error}),
    
    Name = generator:gen_name(),
    DirStructure = fun(Distribution) ->
        {?SPACE_NAME, [
            {Name, ?TEST_DATA, Distribution}
        ]}
    end,
    % create new QoS entry and check, that it is not fulfilled
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(DirStructure([Provider1]), ?SPACE_NAME, Name),
    % check that file is on qos failed files list
    FileGuid = qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, []), GuidsAndPaths),
    ?assertEqual(true, is_file_in_failed_files_list(TargetProvider, FileGuid), ?ATTEMPTS),
    ?assert(is_failed_files_list_empty(Providers -- [TargetProvider], file_id:guid_to_space_id(FileGuid))),
    % check file distribution (file blocks should be only on source provider)
    ?assert(qos_tests_utils:assert_distribution_in_dir_structure(DirStructure([Provider1]), GuidsAndPaths)),
    % initialize periodic check of failed files
    utils:rpc_multicall(Nodes, qos_worker, init_retry_failed_files, []),
    
    % check that after a successful transfer QoS entry is eventually fulfilled
    qos_tests_utils:mock_replica_synchronizer(Nodes, passthrough),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([FileGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    % check file distribution again (file blocks should be on both source and target provider)
    ?assertEqual(true, qos_tests_utils:assert_distribution_in_dir_structure(
        DirStructure(lists:usort([Provider1, TargetProvider])), GuidsAndPaths)),
    % check that failed files list is empty 
    ?assert(is_failed_files_list_empty(Providers, file_id:guid_to_space_id(FileGuid))).


qos_status_after_failed_transfer_deleted_file(TargetProvider) ->
    [Provider1 | _] = Providers = oct_background:get_provider_ids(),
    Nodes = oct_background:get_all_providers_nodes(),
    qos_tests_utils:mock_replica_synchronizer(Nodes, {error, some_error}),
    
    Name = generator:gen_name(),
    DirStructure = fun(Distribution) ->
        {?SPACE_NAME, [
            {Name, [
                {?filename(Name, 1), ?TEST_DATA, Distribution}
            ]}
        ]}
    end,
    
    % create new QoS entry and check, that it is not fulfilled
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(DirStructure([Provider1]), ?SPACE_NAME, Name),
    % check that file is on qos failed files list
    FileGuid = qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, [1]), GuidsAndPaths),
    ?assertEqual(true, is_file_in_failed_files_list(TargetProvider, FileGuid), ?ATTEMPTS),
    ?assert(is_failed_files_list_empty(Providers -- [TargetProvider], file_id:guid_to_space_id(FileGuid))),
    % check file distribution (file blocks should be only on source provider)
    ?assert(qos_tests_utils:assert_distribution_in_dir_structure(DirStructure([Provider1]), GuidsAndPaths)),
    % initialize periodic check of failed files
    utils:rpc_multicall(Nodes, qos_worker, init_retry_failed_files, []),
    
    % delete file on random node
    DirGuid = qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, []), GuidsAndPaths),
    DeletingProvider = lists_utils:random_element(Providers),
    ok = lfm_proxy:unlink(oct_background:get_random_provider_node(DeletingProvider), ?SESS_ID(DeletingProvider), ?FILE_REF(FileGuid)),
    lists:foreach(fun(N) ->
        ?assertEqual({error, enoent}, lfm_proxy:stat(N, ?ROOT_SESS_ID, ?FILE_REF(FileGuid)), ?ATTEMPTS)
    end, Nodes),
    % check that QoS entry is eventually fulfilled
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([DirGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    % no need to check distribution as file was deleted
    % check that failed files list is empty (attempts needed to wait for failed 
    % files retry to execute - qos status change was triggered by file deletion)
    ?assertEqual(true, is_failed_files_list_empty(Providers, file_id:guid_to_space_id(FileGuid)), 10).


qos_status_after_failed_transfer_deleted_entry(TargetProvider) ->
    [Provider1 | _] = Providers = oct_background:get_provider_ids(),
    Nodes = oct_background:get_all_providers_nodes(),
    qos_tests_utils:mock_replica_synchronizer(Nodes, {error, some_error}),
    
    Name = generator:gen_name(),
    DirStructure = fun(Distribution) ->
        {?SPACE_NAME, [
            {Name, ?TEST_DATA, Distribution}
        ]}
    end,
    
    % create new QoS entry and check, that it is not fulfilled
    {GuidsAndPaths, [QosEntryId]} = prepare_qos_status_test_env(DirStructure([Provider1]), ?SPACE_NAME, Name),
    % add second entry to the same file
    {_, QosEntryList} = prepare_qos_status_test_env(undefined, ?SPACE_NAME, Name),
    % check that file is on qos failed files list
    FileGuid = qos_tests_utils:get_guid(resolve_path(?SPACE_NAME, Name, []), GuidsAndPaths),
    ?assertEqual(true, is_file_in_failed_files_list(TargetProvider, FileGuid), ?ATTEMPTS),
    ?assert(is_failed_files_list_empty(Providers -- [TargetProvider], file_id:guid_to_space_id(FileGuid))),
    % check file distribution (file blocks should be only on source provider)
    ?assert(qos_tests_utils:assert_distribution_in_dir_structure(DirStructure([Provider1]), GuidsAndPaths)),
    % initialize periodic check of failed files
    utils:rpc_multicall(Nodes, qos_worker, init_retry_failed_files, []),
    
    % delete one QoS entry on random provider
    DeletingProvider = lists_utils:random_element(Providers),
    ok = lfm_proxy:remove_qos_entry(oct_background:get_random_provider_node(DeletingProvider), ?SESS_ID(DeletingProvider), QosEntryId),
    lists:foreach(fun(N) ->
        ?assertEqual({error, not_found}, lfm_proxy:get_qos_entry(N, ?ROOT_SESS_ID, QosEntryId), ?ATTEMPTS)
    end, Nodes),
    
    % check that after a successful transfer QoS entry is eventually fulfilled
    qos_tests_utils:mock_replica_synchronizer(Nodes, passthrough),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([FileGuid], QosEntryList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    % check file distribution again (file blocks should be on both source and target provider)
    ?assertEqual(true, qos_tests_utils:assert_distribution_in_dir_structure(
        DirStructure(lists:usort([Provider1, TargetProvider])), GuidsAndPaths)),
    % check that failed files list is empty 
    ?assert(is_failed_files_list_empty(Providers, file_id:guid_to_space_id(FileGuid))).


%%%===================================================================
%%% QoS with links test bases
%%%===================================================================

qos_with_hardlink_test_base(Mode) ->
    % Mode can be one of 
    %   * direct - QoS entry is added directly to file/hardlink; 
    %   * effective - file and hardlink are in different directories, QoS entry is added on these directories 
    
    [Provider1, Provider2 | _] = oct_background:get_provider_ids(),
    P1Node = oct_background:get_random_provider_node(Provider1),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    SpaceGuid = rpc:call(P1Node, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    {FileParent, LinkParent} = case Mode of
        direct -> {SpaceGuid, SpaceGuid};
        effective ->
            {ok, Dir1Guid} = lfm_proxy:mkdir(P1Node, ?SESS_ID(Provider1), SpaceGuid, generator:gen_name(), ?DEFAULT_DIR_PERMS),
            {ok, Dir2Guid} = lfm_proxy:mkdir(P1Node, ?SESS_ID(Provider1), SpaceGuid, generator:gen_name(), ?DEFAULT_DIR_PERMS),
            {Dir1Guid, Dir2Guid}
    end,
    
    {ok, FileGuid} = lfm_proxy:create(P1Node, ?SESS_ID(Provider1), FileParent, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_link(P1Node, ?SESS_ID(Provider1), ?FILE_REF(FileGuid), ?FILE_REF(LinkParent), generator:gen_name()),
    await_files_sync_between_nodes(oct_background:get_all_providers_nodes(), [FileGuid, LinkGuid]),
    
    QosTargets = case Mode of
        direct -> [FileGuid, LinkGuid];
        effective -> [FileParent, LinkParent]
    end,
    
    QosList = lists:map(fun(GuidToAddQos) ->
        {ok, QosEntryId} = lfm_proxy:add_qos_entry(P1Node, ?SESS_ID(Provider1), ?FILE_REF(GuidToAddQos), <<"providerId=", Provider2/binary>>, 1),
        assert_effective_entry(P1Node, ?SESS_ID(Provider1), QosEntryId, [FileGuid, LinkGuid], []),
        QosEntryId
    end, QosTargets),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([FileGuid, LinkGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    qos_tests_utils:mock_transfers(oct_background:get_all_providers_nodes()),
    lists:foreach(fun(GuidToWrite) ->
        {ok, Handle} = lfm_proxy:open(P1Node, ?SESS_ID(Provider1), ?FILE_REF(GuidToWrite), write),
        {ok, _} = lfm_proxy:write(P1Node, Handle, 0, crypto:strong_rand_bytes(123)),
        ok = lfm_proxy:close(P1Node, Handle),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([FileGuid, LinkGuid], QosList, ?PENDING_QOS_STATUS), ?ATTEMPTS),
        
        qos_tests_utils:finish_transfers([FileGuid]),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([FileGuid, LinkGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS)
    end, [FileGuid, LinkGuid]).


qos_with_hardlink_deletion_test_base(ToDelete) ->
    % ToDelete can be one of 
    %   * inode - QoS entry is added to inode and it is later deleted; 
    %   * hardlink - QoS entry is added to hardlink and it is later deleted; 
    %   * mixed - file that QoS entry is added to is randomly selected, and deleted is the other one
    
    [Provider1, Provider2 | _] = oct_background:get_provider_ids(),
    P1Node = oct_background:get_random_provider_node(Provider1),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    SpaceGuid = rpc:call(P1Node, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    
    {ok, FileGuid} = lfm_proxy:create(P1Node, ?SESS_ID(Provider1), SpaceGuid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_link(P1Node, ?SESS_ID(Provider1), ?FILE_REF(FileGuid), ?FILE_REF(SpaceGuid), generator:gen_name()),
    await_files_sync_between_nodes(oct_background:get_all_providers_nodes(), [FileGuid, LinkGuid]),
    Guids = [FileGuid, LinkGuid],
    
    {ToAddQosGuid, ToDeleteGuid} = case ToDelete of
        inode -> {FileGuid, FileGuid};
        hardlink -> {LinkGuid, LinkGuid};
        mixed -> 
            case rand:uniform(2) of
                1 -> {FileGuid, LinkGuid};
                2 -> {LinkGuid, FileGuid}
            end
    end,
    {ok, QosEntryId} = lfm_proxy:add_qos_entry(P1Node, ?SESS_ID(Provider1), ?FILE_REF(ToAddQosGuid), <<"providerId=", Provider2/binary>>, 1),
    assert_effective_entry(P1Node, ?SESS_ID(Provider1), QosEntryId, Guids, []),
    ok = lfm_proxy:unlink(P1Node, ?SESS_ID(Provider1), ?FILE_REF(ToDeleteGuid)),
    assert_effective_entry(P1Node, ?SESS_ID(Provider1), QosEntryId, Guids -- [ToDeleteGuid], []).


qos_on_symlink_test_base() ->
    [Provider1, Provider2 | _] = oct_background:get_provider_ids(),
    P1Node = oct_background:get_random_provider_node(Provider1),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    SpaceGuid = rpc:call(P1Node, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    
    {ok, FileGuid} = lfm_proxy:create(P1Node, ?SESS_ID(Provider1), SpaceGuid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, FilePath} = lfm_proxy:get_file_path(P1Node, ?SESS_ID(Provider1), FileGuid),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_symlink(P1Node, ?SESS_ID(Provider1), ?FILE_REF(SpaceGuid), generator:gen_name(), FilePath),
    await_files_sync_between_nodes(oct_background:get_all_providers_nodes(), [FileGuid, LinkGuid]),
    
    {ok, QosEntryId} = lfm_proxy:add_qos_entry(P1Node, ?SESS_ID(Provider1), ?FILE_REF(LinkGuid), <<"providerId=", Provider2/binary>>, 1),
    assert_effective_entry(P1Node, ?SESS_ID(Provider1), QosEntryId, [LinkGuid], [FileGuid]),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([FileGuid, LinkGuid], QosEntryId, ?FULFILLED_QOS_STATUS), ?ATTEMPTS).


effective_qos_with_symlink_test_base() ->
    [Provider1, Provider2 | _] = oct_background:get_provider_ids(),
    P1Node = oct_background:get_random_provider_node(Provider1),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    SpaceGuid = rpc:call(P1Node, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    {ok, Dir1Guid} = lfm_proxy:mkdir(P1Node, ?SESS_ID(Provider1), SpaceGuid, generator:gen_name(), ?DEFAULT_DIR_PERMS),
    {ok, Dir2Guid} = lfm_proxy:mkdir(P1Node, ?SESS_ID(Provider1), SpaceGuid, generator:gen_name(), ?DEFAULT_DIR_PERMS),
    
    {ok, FileGuid} = lfm_proxy:create(P1Node, ?SESS_ID(Provider1), Dir1Guid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, FilePath} = lfm_proxy:get_file_path(P1Node, ?SESS_ID(Provider1), FileGuid),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_symlink(P1Node, ?SESS_ID(Provider1), ?FILE_REF(Dir2Guid), generator:gen_name(), FilePath),
    await_files_sync_between_nodes(oct_background:get_all_providers_nodes(), [FileGuid, LinkGuid]),
    
    {ok, QosEntryId1} = lfm_proxy:add_qos_entry(P1Node, ?SESS_ID(Provider1), ?FILE_REF(Dir1Guid), <<"providerId=", Provider2/binary>>, 1),
    assert_effective_entry(P1Node, ?SESS_ID(Provider1), QosEntryId1, [FileGuid], [LinkGuid]),
    {ok, QosEntryId2} = lfm_proxy:add_qos_entry(P1Node, ?SESS_ID(Provider1), ?FILE_REF(Dir2Guid), <<"providerId=", Provider2/binary>>, 1),
    assert_effective_entry(P1Node, ?SESS_ID(Provider1), QosEntryId2, [LinkGuid], [FileGuid]),
    
    QosList = [QosEntryId1, QosEntryId2],
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([FileGuid, LinkGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    qos_tests_utils:mock_transfers(oct_background:get_all_providers_nodes()),
    {ok, Handle} = lfm_proxy:open(P1Node, ?SESS_ID(Provider1), ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(P1Node, Handle, 0, crypto:strong_rand_bytes(123)),
    ok = lfm_proxy:close(P1Node, Handle),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([FileGuid], QosList, ?PENDING_QOS_STATUS), ?ATTEMPTS),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([LinkGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    qos_tests_utils:finish_transfers([FileGuid]),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([FileGuid, LinkGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS).


create_hardlink_in_dir_with_qos() ->
    [Provider1, Provider2 | _] = Providers = oct_background:get_provider_ids(),
    P1Node = oct_background:get_random_provider_node(Provider1),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    SpaceGuid = rpc:call(P1Node, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    {ok, Dir1Guid} = lfm_proxy:mkdir(P1Node, ?SESS_ID(Provider1), SpaceGuid, generator:gen_name(), ?DEFAULT_DIR_PERMS),
    {ok, FileGuid} = lfm_proxy:create(P1Node, ?SESS_ID(Provider1), SpaceGuid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, QosEntryId} = lfm_proxy:add_qos_entry(P1Node, ?SESS_ID(Provider1), ?FILE_REF(Dir1Guid), <<"providerId=", Provider2/binary>>, 1),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([FileGuid, Dir1Guid], [QosEntryId], ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    qos_tests_utils:mock_transfers(oct_background:get_all_providers_nodes()),
    lists:foreach(fun(Provider) ->
        lists:foreach(fun(Node) ->
            {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_link(Node, ?SESS_ID(Provider), ?FILE_REF(FileGuid), ?FILE_REF(Dir1Guid), generator:gen_name()),
            await_files_sync_between_nodes(oct_background:get_all_providers_nodes(), [FileGuid, LinkGuid]),
            assert_effective_entry(Node, ?SESS_ID(Provider), QosEntryId, [LinkGuid, FileGuid], []),
            ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([Dir1Guid, LinkGuid], [QosEntryId], ?PENDING_QOS_STATUS), ?ATTEMPTS),
            qos_tests_utils:finish_transfers([LinkGuid]),
            ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes([Dir1Guid, LinkGuid], [QosEntryId], ?FULFILLED_QOS_STATUS), ?ATTEMPTS)
        end, oct_background:get_provider_nodes(Provider))
    end, Providers).
    

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(Config) ->
    ct:timetrap(timer:minutes(10)),
    NewConfig = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(NewConfig),
    NewConfig.


end_per_testcase(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    qos_tests_utils:finish_all_transfers(),
    test_utils:mock_unload(Nodes),
    initializer:clean_test_users_and_spaces_no_validate(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
prepare_qos_status_test_env(DirStructure, SpaceName, Name) ->
    [Provider1, Provider2 | _] = Providers = oct_background:get_provider_ids(),
    QosRootFilePath = filename:join([<<"/">>, SpaceName, Name]),
    
    TestDirStructure = case DirStructure of
        undefined -> undefined;
        _ -> #test_dir_structure{dir_structure = DirStructure}
    end,
    
    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = TestDirStructure,
        qos_to_add = [
            #qos_to_add{
                provider = Provider1,
                qos_name = ?QOS1,
                path = QosRootFilePath,
                expression = <<"providerId=", Provider2/binary>>
            }
        ],
        % do not wait for QoS fulfillment
        wait_for_qos_fulfillment = false,
        expected_qos_entries = [
            #expected_qos_entry{
                providers = Providers,
                qos_name = ?QOS1,
                file_key = {path, QosRootFilePath},
                qos_expression = [<<"providerId=", Provider2/binary>>],
                replicas_num = 1,
                possibility_check = {possible, Provider1}
            }
        ]
    },
    
    {GuidsAndPaths, QosNameIdMapping} = qos_tests_utils:fulfill_qos_test_base(QosSpec),
    QosList = maps:values(QosNameIdMapping),
    
    FilesAndDirs = maps:get(files, GuidsAndPaths, []) ++ maps:get(dirs, GuidsAndPaths, []),
    FilesAndDirsGuids = lists:filtermap(fun({G, P}) when P >= QosRootFilePath -> {true, G}; (_) -> false end, FilesAndDirs),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_nodes(FilesAndDirsGuids, QosList, ?PENDING_QOS_STATUS)),
    {GuidsAndPaths, QosList}.


%% @private
resolve_path(SpaceName, Name, Files) ->
    Files1 = lists:map(fun(A) -> ?filename(Name, A) end, Files),
    filepath_utils:join([<<"/">>, SpaceName, Name | Files1]).


%% @private
is_failed_files_list_empty(Providers, SpaceId) ->
    lists:all(fun(Provider) ->
        {ok, []} == get_qos_failed_files_list(Provider, SpaceId)
    end, Providers).


%% @private
is_file_in_failed_files_list(Provider, FileGuid) ->
    Uuid = file_id:guid_to_uuid(FileGuid),
    SpaceId = file_id:guid_to_space_id(FileGuid),
    {ok, List} = get_qos_failed_files_list(Provider, SpaceId),
    lists:member(Uuid, List).


%% @private
get_qos_failed_files_list(Provider, SpaceId) ->
    Node = oct_background:get_random_provider_node(Provider),
    rpc:call(Node, datastore_model, fold_links, [
        #{model => qos_entry}, 
        <<"failed_files_qos_key_", SpaceId/binary>>, 
        Provider,
        fun qos_entry:accumulate_link_names/2,
        [], 
        #{}
    ]).


%% @private
prepare_type_spec(reg_file, _Nodes, _Target) -> reg_file;
prepare_type_spec(hardlink, Nodes, {target, FileToLinkGuid}) ->
    lists:foreach(fun(Node) ->
        ?assertMatch({ok, _}, lfm_proxy:stat(Node, ?ROOT_SESS_ID, ?FILE_REF(FileToLinkGuid)), ?ATTEMPTS)
    end, Nodes),
    {hardlink, FileToLinkGuid};
prepare_type_spec(random, Nodes, Target) -> 
    NewFileType = case rand:uniform(2) of
        1 -> reg_file;
        2 -> hardlink
    end,
    prepare_type_spec(NewFileType, Nodes, Target).


%% @private
create_link_target(Node, SessId, SpaceId) ->
    SpaceGuid = rpc:call(Node, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    {ok, FileToLinkGuid} = lfm_proxy:create(Node, SessId, SpaceGuid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    FileToLinkGuid.


%% @private
await_files_sync_between_nodes(Nodes, Guids) ->
    lists:foreach(fun(Node) ->
        lists:foreach(fun(Guid) ->
            ?assertMatch({ok, _}, lfm_proxy:stat(Node, ?ROOT_SESS_ID, ?FILE_REF(Guid)), ?ATTEMPTS)
        end, Guids)
    end, Nodes).


%% @private
create_files_and_write(Node, SessId, ParentGuid, TypeSpec, NumOfFiles) ->
    lists:map(fun(_) ->
        {ok, {FileGuid, FileHandle}} =  qos_tests_utils:create_and_open(Node, SessId, ParentGuid, TypeSpec),
        {ok, _} = lfm_proxy:write(Node, FileHandle, 0, <<"new_data">>),
        ok = lfm_proxy:close(Node, FileHandle),
        FileGuid
    end, lists:seq(1, NumOfFiles)).


%% @private
assert_effective_entry(Node, SessId, QosEntryId, FilesToAssertTrue, FilesToAssertFalse) ->
    lists:foreach(fun(Guid) ->
        ?assertMatch({ok, {#{QosEntryId := _}, _}}, lfm_proxy:get_effective_file_qos(Node, SessId, ?FILE_REF(Guid)))
    end, FilesToAssertTrue),
    lists:foreach(fun(Guid) ->
        ?assertNotMatch({ok, {#{QosEntryId := _}, _}}, lfm_proxy:get_effective_file_qos(Node, SessId, ?FILE_REF(Guid)))
    end, FilesToAssertFalse).
