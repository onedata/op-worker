%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Common macros used across logic tests and gs_channel tests.
%%% Two instances of each record synchronized via graph sync channel are mocked.
%%% For simplicity, all entities are related to each other.
%%% @end
%%%-------------------------------------------------------------------
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/handshake_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/space_support/support_parameters.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

-define(DUMMY_PROVIDER_ID, <<"dummyProviderId">>).
-define(DUMMY_ONEZONE_DOMAIN, <<"onezone.test">>).

-define(MOCK_PROVIDER_IDENTITY_TOKEN(__ProviderId), <<"DUMMY-PROVIDER-IDENTITY-TOKEN-", __ProviderId/binary>>).
-define(MOCK_PROVIDER_ACCESS_TOKEN(__ProviderId), <<"DUMMY-PROVIDER-ACCESS-TOKEN-", __ProviderId/binary>>).

% WebSocket path is used to control gs_client:start_link mock behaviour
-define(PATH_CAUSING_CONN_ERROR, "/conn_err").
-define(PATH_CAUSING_NOBODY_IDENTITY, "/nobody_iden").
-define(PATH_CAUSING_CORRECT_CONNECTION, "/graph_sync").

% Mocked records
-define(USER_1, <<"user1Id">>).
-define(USER_2, <<"user2Id">>).
-define(USER_3, <<"user3Id">>). % This user is not related to any entity
-define(USER_INCREASING_REV, <<"userIncRev">>). % This record has a higher rev every time it is fetched
-define(GROUP_1, <<"group1Id">>).
-define(GROUP_2, <<"group2Id">>).
-define(SPACE_1, <<"space1Id">>).
-define(SPACE_2, <<"space2Id">>).
-define(SHARE_1, <<"share1Id">>).
-define(SHARE_2, <<"share2Id">>).
-define(PROVIDER_1, <<"provider1Id">>).
-define(PROVIDER_2, <<"provider2Id">>).
-define(HANDLE_SERVICE_1, <<"hservice1Id">>).
-define(HANDLE_SERVICE_2, <<"hservice2Id">>).
-define(HANDLE_1, <<"handle1Id">>).
-define(HANDLE_2, <<"handle2Id">>).
-define(HARVESTER_1, <<"harvester1Id">>).
-define(HARVESTER_2, <<"harvester2Id">>).
-define(STORAGE_1, <<"storage1Id">>).
-define(STORAGE_2, <<"storage2Id">>).
-define(TOKEN_1, <<"token1Id">>).
-define(TOKEN_2, <<"token2Id">>).
-define(ATM_INVENTORY_1, <<"atmInventory1Id">>).
-define(ATM_LAMBDA_1, <<"atmLambda1Id">>).
-define(ATM_WORKFLOW_SCHEMA_1, <<"atmWorkflowSchema1Id">>).

% User authorizations
% Token auth is translated to {token, Token} before graph sync request.
-define(USER_GS_TOKEN_AUTH(Token), {token, Token}).


-define(USER_PERMS_IN_GROUP_VALUE_BINARIES, #{?USER_1 => [atom_to_binary(?GROUP_VIEW, utf8)], ?USER_2 => [atom_to_binary(?GROUP_VIEW, utf8)]}).
-define(USER_PERMS_IN_GROUP_MATCHER_ATOMS, #{?USER_1 := [?GROUP_VIEW], ?USER_2 := [?GROUP_VIEW]}).
-define(GROUP_PERMS_IN_GROUP_VALUE_BINARIES, #{?GROUP_1 => [atom_to_binary(?GROUP_VIEW, utf8)], ?GROUP_2 => [atom_to_binary(?GROUP_VIEW, utf8)]}).
-define(GROUP_PERMS_IN_GROUP_MATCHER_ATOMS, #{?GROUP_1 := [?GROUP_VIEW], ?GROUP_2 := [?GROUP_VIEW]}).

-define(USER_PERMS_IN_SPACE_VALUE_BINARIES, #{?USER_1 => [atom_to_binary(?SPACE_VIEW, utf8)], ?USER_2 => [atom_to_binary(?SPACE_VIEW, utf8)]}).
-define(USER_PERMS_IN_SPACE_MATCHER_ATOMS, #{?USER_1 := [?SPACE_VIEW], ?USER_2 := [?SPACE_VIEW]}).
-define(GROUP_PERMS_IN_SPACE_VALUE_BINARIES, #{?GROUP_1 => [atom_to_binary(?SPACE_VIEW, utf8)], ?GROUP_2 => [atom_to_binary(?SPACE_VIEW, utf8)]}).
-define(GROUP_PERMS_IN_SPACE_MATCHER_ATOMS, #{?GROUP_1 := [?SPACE_VIEW], ?GROUP_2 := [?SPACE_VIEW]}).

-define(USER_PERMS_IN_HSERVICE_VALUE_BINARIES, #{?USER_1 => [atom_to_binary(?HANDLE_SERVICE_VIEW, utf8)], ?USER_2 => [atom_to_binary(?HANDLE_SERVICE_VIEW, utf8)]}).
-define(USER_PERMS_IN_HSERVICE_MATCHER_ATOMS, #{?USER_1 := [?HANDLE_SERVICE_VIEW], ?USER_2 := [?HANDLE_SERVICE_VIEW]}).
-define(GROUP_PERMS_IN_HSERVICE_VALUE_BINARIES, #{?GROUP_1 => [atom_to_binary(?HANDLE_SERVICE_VIEW, utf8)], ?GROUP_2 => [atom_to_binary(?HANDLE_SERVICE_VIEW, utf8)]}).
-define(GROUP_PERMS_IN_HSERVICE_MATCHER_ATOMS, #{?GROUP_1 := [?HANDLE_SERVICE_VIEW], ?GROUP_2 := [?HANDLE_SERVICE_VIEW]}).

-define(USER_PERMS_IN_HANDLE_VALUE_BINARIES, #{?USER_1 => [atom_to_binary(?HANDLE_VIEW, utf8)], ?USER_2 => [atom_to_binary(?HANDLE_VIEW, utf8)]}).
-define(USER_PERMS_IN_HANDLE_MATCHER_ATOMS, #{?USER_1 := [?HANDLE_VIEW], ?USER_2 := [?HANDLE_VIEW]}).
-define(GROUP_PERMS_IN_HANDLE_VALUE_BINARIES, #{?GROUP_1 => [atom_to_binary(?HANDLE_VIEW, utf8)], ?GROUP_2 => [atom_to_binary(?HANDLE_VIEW, utf8)]}).
-define(GROUP_PERMS_IN_HANDLE_MATCHER_ATOMS, #{?GROUP_1 := [?HANDLE_VIEW], ?GROUP_2 := [?HANDLE_VIEW]}).

% Mocked user data
-define(USER_FULL_NAME(__User), __User).
-define(USER_USERNAME(__User), __User).
-define(USER_EMAIL_LIST(__User), [__User]).
-define(USER_LINKED_ACCOUNTS_VALUE(__User), [#{<<"userId">> => __User}]).
-define(USER_LINKED_ACCOUNTS_MATCHER(__User), [#{<<"userId">> := __User}]).
-define(USER_SPACE_ALIASES(__User), #{}).
-define(USER_EFF_GROUPS(__User), [?GROUP_1, ?GROUP_2]).
-define(USER_EFF_SPACES(__User), [?SPACE_1, ?SPACE_2]).
-define(USER_EFF_HANDLE_SERVICES(__User), [?HANDLE_SERVICE_1, ?HANDLE_SERVICE_2]).
-define(USER_EFF_ATM_INVENTORIES(__User), [?ATM_INVENTORY_1]).

% Mocked group data
-define(GROUP_NAME(__Group), __Group).
-define(GROUP_TYPE_JSON(__Group), <<"role">>).
-define(GROUP_TYPE_ATOM(__Group), role).

% Mocked space data
-define(SPACE_NAME(__Space), __Space).
-define(SPACE_OWNERS(__Space), [?USER_1]).
-define(SPACE_DIRECT_USERS_VALUE(__Space), ?USER_PERMS_IN_SPACE_VALUE_BINARIES).
-define(SPACE_DIRECT_USERS_MATCHER(__Space), ?USER_PERMS_IN_SPACE_MATCHER_ATOMS).
-define(SPACE_EFF_USERS_VALUE(__Space), ?USER_PERMS_IN_SPACE_VALUE_BINARIES).
-define(SPACE_EFF_USERS_MATCHER(__Space), ?USER_PERMS_IN_SPACE_MATCHER_ATOMS).
-define(SPACE_DIRECT_GROUPS_VALUE(__Space), ?GROUP_PERMS_IN_SPACE_VALUE_BINARIES).
-define(SPACE_DIRECT_GROUPS_MATCHER(__Space), ?GROUP_PERMS_IN_SPACE_MATCHER_ATOMS).
-define(SPACE_EFF_GROUPS_VALUE(__Space), ?GROUP_PERMS_IN_SPACE_VALUE_BINARIES).
-define(SPACE_EFF_GROUPS_MATCHER(__Space), ?GROUP_PERMS_IN_SPACE_MATCHER_ATOMS).
-define(SPACE_PROVIDERS_VALUE(__Space), #{?PROVIDER_1 => 1000000000, ?PROVIDER_2 => 1000000000}).
-define(SPACE_PROVIDERS_MATCHER(__Space), #{?PROVIDER_1 := 1000000000, ?PROVIDER_2 := 1000000000}).
-define(SPACE_SHARES(__Space), [?SHARE_1, ?SHARE_2]).
-define(SPACE_HARVESTERS(__Space), [?HARVESTER_1, ?HARVESTER_2]).
-define(SPACE_STORAGES_VALUE(__Space), #{?STORAGE_1 => 1000000000, ?STORAGE_2 => 1000000000}).
-define(SPACE_STORAGES_MATCHER(__Space), #{?STORAGE_1 := 1000000000, ?STORAGE_2 := 1000000000}).
-define(SPACE_SUPPORT_PARAMETERS_REGISTRY_VALUE(__Space), jsonable_record:to_json(#support_parameters_registry{registry = #{
    ?PROVIDER_1 => #support_parameters{accounting_enabled = true, dir_stats_service_enabled = true, dir_stats_service_status = initializing},
    ?PROVIDER_2 => #support_parameters{accounting_enabled = false, dir_stats_service_enabled = false, dir_stats_service_status = disabled}
}}, support_parameters_registry)).
-define(SPACE_SUPPORT_PARAMETERS_REGISTRY_MATCHER(__Space), #support_parameters_registry{registry = #{
    ?PROVIDER_1 := #support_parameters{accounting_enabled = true, dir_stats_service_enabled = true, dir_stats_service_status = initializing},
    ?PROVIDER_2 := #support_parameters{accounting_enabled = false, dir_stats_service_enabled = false, dir_stats_service_status = disabled}
}}).

% Mocked share data
-define(SHARE_NAME(__Share), __Share).
-define(SHARE_DESCRIPTION(__Share), __Share).
-define(SHARE_PUBLIC_URL(__Share), __Share).
-define(SHARE_PUBLIC_REST_URL(__Share), __Share).
-define(SHARE_FILE_TYPE(__Share), ?DIRECTORY_TYPE).
-define(SHARE_SPACE(__Share), ?SPACE_1).
-define(SHARE_HANDLE(__Share), ?HANDLE_1).
-define(SHARE_ROOT_FILE(__Share), __Share).

% Mocked provider data
-define(PROVIDER_NAME(__Provider), __Provider).
-define(PROVIDER_ADMIN_EMAIL(__Provider), __Provider).
-define(PROVIDER_DOMAIN(__Provider), __Provider).
-define(PROVIDER_ONLINE(__Provider), true).
-define(PROVIDER_SUBDOMAIN_DELEGATION(__Provider), false).
-define(PROVIDER_SUBDOMAIN(__Provider), undefined).
-define(PROVIDER_SPACES_VALUE(__Provider), #{?SPACE_1 => 1000000000, ?SPACE_2 => 1000000000}).
-define(PROVIDER_SPACES_MATCHER(__Provider), #{?SPACE_1 := 1000000000, ?SPACE_2 := 1000000000}).
-define(PROVIDER_STORAGES(__Provider), case __Provider of
    ?PROVIDER_1 -> [?STORAGE_1];
    ?PROVIDER_2 -> [?STORAGE_2];
    _ -> []
end).
-define(PROVIDER_EFF_USERS(__Provider), [?USER_1, ?USER_2]).
-define(PROVIDER_EFF_GROUPS(__Provider), [?GROUP_1, ?GROUP_2]).
-define(PROVIDER_LATITUDE(__Provider), 0.0).
-define(PROVIDER_LONGITUDE(__Provider), 0.0).

% Mocked handle service data
-define(HANDLE_SERVICE_NAME(__HService), __HService).

% Mocked handle data
-define(HANDLE_PUBLIC_HANDLE(__Handle), __Handle).
-define(HANDLE_RESOURCE_TYPE(__Handle), <<"Share">>).
-define(HANDLE_RESOURCE_ID(__Handle), ?SHARE_1).
-define(HANDLE_METADATA_PREFIX(__Handle), <<"oai_dc">>).
-define(HANDLE_METADATA(__Handle), __Handle).
-define(HANDLE_H_SERVICE(__Handle), ?HANDLE_SERVICE_1).

% Mocked harvester data
-define(HARVESTER_SPACE1(__Harvester), <<"harvesterSpace1">>).
-define(HARVESTER_SPACE2(__Harvester), <<"harvesterSpace2">>).
-define(HARVESTER_SPACE3(__Harvester), <<"harvesterSpace3">>).

-define(HARVESTER_INDEX1(__Harvester), <<"harvesterIndex1">>).
-define(HARVESTER_INDEX2(__Harvester), <<"harvesterIndex2">>).
-define(HARVESTER_INDEX3(__Harvester), <<"harvesterIndex3">>).

-define(HARVESTER_SPACES(__Harvester), [
    ?HARVESTER_SPACE1(__Harvester)
]).
-define(HARVESTER_SPACES2(__Harvester), [
    ?HARVESTER_SPACE2(__Harvester),
    ?HARVESTER_SPACE3(__Harvester)
]).

-define(HARVESTER_INDICES(__Harvester), [
    ?HARVESTER_INDEX1(__Harvester)
]).
-define(HARVESTER_INDICES2(__Harvester), [
    ?HARVESTER_INDEX2(__Harvester),
    ?HARVESTER_INDEX3(__Harvester)
]).

% Mocked storage data
-define(STORAGE_NAME(__Storage), __Storage).
-define(STORAGE_PROVIDER(__Storage), case __Storage of
    ?STORAGE_1 -> [?PROVIDER_1];
    ?STORAGE_2 -> [?PROVIDER_2]
end).

% Mocked atm_inventory data
-define(ATM_INVENTORY_NAME(__AtmInventory), __AtmInventory).

% Mocked atm_lambda data
-define(ATM_LAMBDA_DATA_SPEC, #atm_boolean_data_spec{}).
-define(ATM_LAMBDA_FIRST_REVISION(__AtmLambda), #atm_lambda_revision{
    name = <<"example_name">>,
    summary = <<"example_summary">>,
    description = <<"example_description">>,
    operation_spec = #atm_openfaas_operation_spec{
        docker_image = <<"example_docker_image">>,
        docker_execution_options = #atm_docker_execution_options{
            readonly = false,
            mount_oneclient = true,
            oneclient_mount_point = <<"/a/b/c/d">>,
            oneclient_options = <<"--a --b">>
        }
    },
    config_parameter_specs = [#atm_parameter_spec{
        name = <<"param">>,
        data_spec = ?ATM_LAMBDA_DATA_SPEC,
        is_optional = true,
        default_value = true
    }],
    argument_specs = [#atm_parameter_spec{
        name = <<"arg">>,
        data_spec = ?ATM_LAMBDA_DATA_SPEC,
        is_optional = true,
        default_value = false
    }],
    result_specs = [#atm_lambda_result_spec{
        name = <<"res">>,
        data_spec = ?ATM_LAMBDA_DATA_SPEC,
        relay_method = return_value
    }],
    preferred_batch_size = 10,
    resource_spec = #atm_resource_spec{
        cpu_requested = 2.0, cpu_limit = 4.0,
        memory_requested = 1000000000, memory_limit = 5000000000,
        ephemeral_storage_requested = 1000000000, ephemeral_storage_limit = 5000000000
    },
    checksum = <<"3f737525fe2e905b2b5b532d0264175b">>,
    state = stable
}).
-define(ATM_LAMBDA_REVISION_REGISTRY_VALUE(__AtmLambda), #atm_lambda_revision_registry{
    registry = #{
        1 => ?ATM_LAMBDA_FIRST_REVISION(__AtmLambda)
    }
}).
-define(ATM_LAMBDA_REVISION_REGISTRY_MATCHER(__AtmLambda), #atm_lambda_revision_registry{
    registry = #{
        1 := ?ATM_LAMBDA_FIRST_REVISION(__AtmLambda)
    }
}).
-define(ATM_LAMBDA_INVENTORIES(__AtmLambda), [?ATM_INVENTORY_1]).

% Mocked atm_workflow_schema data
-define(ATM_WORKFLOW_SCHEMA_NAME(__AtmWorkflowSchema), __AtmWorkflowSchema).
-define(ATM_WORKFLOW_SCHEMA_SUMMARY(__AtmWorkflowSchema), <<"example summary">>).
-define(ATM_WORKFLOW_SCHEMA_FIRST_REVISION(__AtmWorkflowSchema), #atm_workflow_schema_revision{
    description = <<"example description">>,
    stores = [
        #atm_store_schema{
            id = <<"store1Id">>,
            name = <<"store1Name">>,
            description = <<"store1Desc">>,
            type = list,
            config = #atm_list_store_config{item_data_spec = #atm_file_data_spec{
                file_type = 'ANY',
                attributes = [?attr_guid]
            }},
            requires_initial_content = true
        },
        #atm_store_schema{
            id = <<"store2Id">>,
            name = <<"store2Name">>,
            description = <<"store2Desc">>,
            type = single_value,
            config = #atm_single_value_store_config{item_data_spec = #atm_number_data_spec{
                integers_only = false,
                allowed_values = undefined
            }},
            requires_initial_content = false
        }
    ],
    lanes = [
        #atm_lane_schema{
            id = <<"lane1Id">>,
            name = <<"lane1Name">>,
            store_iterator_spec = #atm_store_iterator_spec{
                store_schema_id = <<"store1Id">>,
                max_batch_size = 1
            },
            parallel_boxes = [
                #atm_parallel_box_schema{
                    id = <<"pbox1Id">>,
                    name = <<"pbox1Name">>,
                    tasks = [
                        #atm_task_schema{
                            id = <<"task1Id">>,
                            name = <<"task1Name">>,
                            lambda_id = <<"task1Lambda">>,
                            lambda_config = #{},
                            argument_mappings = [
                                #atm_task_schema_argument_mapper{
                                    argument_name = <<"lambda1ArgName">>,
                                    value_builder = #atm_task_argument_value_builder{
                                        type = iterated_item, recipe = undefined
                                    }
                                }
                            ],
                            result_mappings = [
                                #atm_task_schema_result_mapper{
                                    result_name = <<"lambda1ResName">>,
                                    store_schema_id = <<"store1Id">>,
                                    store_content_update_options = #atm_list_store_content_update_options{
                                        function = append
                                    }
                                }
                            ]
                        }
                    ]
                },
                #atm_parallel_box_schema{
                    id = <<"pbox2Id">>,
                    name = <<"pbox2Name">>,
                    tasks = [
                        #atm_task_schema{
                            id = <<"task2Id">>,
                            name = <<"task2Name">>,
                            lambda_id = <<"task2Lambda">>,
                            lambda_config = #{},
                            argument_mappings = [
                                #atm_task_schema_argument_mapper{
                                    argument_name = <<"lambda2ArgName">>,
                                    value_builder = #atm_task_argument_value_builder{
                                        type = const, recipe = 27.8
                                    }
                                }
                            ],
                            result_mappings = [
                                #atm_task_schema_result_mapper{
                                    result_name = <<"lambda2ResName">>,
                                    store_schema_id = <<"store2Id">>,
                                    store_content_update_options = #atm_single_value_store_content_update_options{}
                                }
                            ]
                        }
                    ]
                }
            ],
            max_retries = 3
        },
        #atm_lane_schema{
            id = <<"lane2Id">>,
            name = <<"lane2Name">>,
            store_iterator_spec = #atm_store_iterator_spec{
                store_schema_id = <<"store2Id">>,
                max_batch_size = 1000
            },
            parallel_boxes = [
                #atm_parallel_box_schema{
                    id = <<"pbox3Id">>,
                    name = <<"pbox3Name">>,
                    tasks = [
                        #atm_task_schema{
                            id = <<"task3Id">>,
                            name = <<"task3Name">>,
                            lambda_id = <<"task3Lambda">>,
                            lambda_config = #{},
                            argument_mappings = [
                                #atm_task_schema_argument_mapper{
                                    argument_name = <<"lambda3ArgName">>,
                                    value_builder = #atm_task_argument_value_builder{
                                        type = object, recipe = #{}
                                    }
                                }
                            ],
                            result_mappings = [
                                #atm_task_schema_result_mapper{
                                    result_name = <<"lambda3ResName">>,
                                    store_schema_id = <<"store3Id">>,
                                    store_content_update_options = #atm_range_store_content_update_options{}
                                }
                            ]
                        },
                        #atm_task_schema{
                            id = <<"task4Id">>,
                            name = <<"task4Name">>,
                            lambda_id = <<"task4Lambda">>,
                            lambda_config = #{},
                            argument_mappings = [
                                #atm_task_schema_argument_mapper{
                                    argument_name = <<"lambda4ArgName">>,
                                    value_builder = #atm_task_argument_value_builder{
                                        type = iterated_item, recipe = undefined
                                    }
                                }
                            ],
                            result_mappings = [
                                #atm_task_schema_result_mapper{
                                    result_name = <<"lambda4ResName">>,
                                    store_schema_id = <<"store4Id">>,
                                    store_content_update_options = #atm_tree_forest_store_content_update_options{
                                        function = extend
                                    }
                                }
                            ]
                        }
                    ]
                }
            ],
            max_retries = 0
        }
    ],
    state = stable
}).
-define(ATM_WORKFLOW_SCHEMA_REVISION_REGISTRY_VALUE(__AtmWorkflowSchema), #atm_workflow_schema_revision_registry{
    registry = #{
        1 => ?ATM_WORKFLOW_SCHEMA_FIRST_REVISION(__AtmWorkflowSchema)
    }
}).
-define(ATM_WORKFLOW_SCHEMA_REVISION_REGISTRY_MATCHER(__AtmWorkflowSchema), #atm_workflow_schema_revision_registry{
    registry = #{
        1 := ?ATM_WORKFLOW_SCHEMA_FIRST_REVISION(__AtmWorkflowSchema)
    }
}).
-define(ATM_WORKFLOW_SCHEMA_INVENTORY(__AtmWorkflowSchema), ?ATM_INVENTORY_1).


-define(MOCK_JOIN_GROUP_TOKEN, <<"mockJoinGroupToken">>).
-define(MOCK_JOINED_GROUP_ID, <<"mockJoinedGroupId">>).
-define(MOCK_CREATED_GROUP_ID, <<"mockCreatedGroupId">>).
-define(MOCK_JOIN_SPACE_TOKEN, <<"mockJoinSpaceToken">>).
-define(MOCK_JOINED_SPACE_ID, <<"mockJoinedSpaceId">>).
-define(MOCK_CREATED_SPACE_ID, <<"mockCreatedSpaceId">>).
-define(MOCK_CREATED_SHARE_ID, <<"mockCreatedHandleId">>).
-define(MOCK_CREATED_HANDLE_ID, <<"mockCreatedHandleId">>).

-define(MOCK_INVITE_USER_TOKEN, <<"mockInviteUserToken">>).
-define(MOCK_INVITE_GROUP_TOKEN, <<"mockInviteGroupToken">>).
-define(MOCK_INVITE_PROVIDER_TOKEN, <<"mockInviteProviderToken">>).
-define(MOCK_IDP_ACCESS_TOKEN, <<"mockIdPAccessToken">>).
-define(MOCK_IDP, <<"mockIdP">>).

-define(USER_PRIVATE_DATA_MATCHER(__User), ?USER_PRIVATE_DATA_MATCHER(__User, {false, unchanged})).
-define(USER_PRIVATE_DATA_MATCHER(__User, __BlockedValue), #document{key = __User, value = #od_user{
    full_name = ?USER_FULL_NAME(__User),
    username = ?USER_USERNAME(__User),
    emails = ?USER_EMAIL_LIST(__User),
    linked_accounts = ?USER_LINKED_ACCOUNTS_MATCHER(__User),
    space_aliases = ?USER_SPACE_ALIASES(__User),
    blocked = __BlockedValue,
    eff_groups = ?USER_EFF_GROUPS(__User),
    eff_spaces = ?USER_EFF_SPACES(__User),
    eff_handle_services = ?USER_EFF_HANDLE_SERVICES(__User),
    eff_atm_inventories = ?USER_EFF_ATM_INVENTORIES(__User)
}}).
-define(USER_PROTECTED_DATA_MATCHER(__User), ?USER_PROTECTED_DATA_MATCHER(__User, {false, unchanged})).
-define(USER_PROTECTED_DATA_MATCHER(__User, __BlockedValue), #document{key = __User, value = #od_user{
    full_name = ?USER_FULL_NAME(__User),
    username = ?USER_USERNAME(__User),
    emails = ?USER_EMAIL_LIST(__User),
    linked_accounts = ?USER_LINKED_ACCOUNTS_MATCHER(__User),
    blocked = __BlockedValue,
    space_aliases = #{},
    eff_groups = [],
    eff_spaces = [],
    eff_handle_services = [],
    eff_atm_inventories = []
}}).
-define(USER_SHARED_DATA_MATCHER(__User), #document{key = __User, value = #od_user{
    full_name = ?USER_FULL_NAME(__User),
    username = ?USER_USERNAME(__User),
    emails = [],
    linked_accounts = [],
    blocked = {undefined, unchanged},
    space_aliases = #{},
    eff_groups = [],
    eff_spaces = [],
    eff_handle_services = [],
    eff_atm_inventories = []
}}).


-define(GROUP_SHARED_DATA_MATCHER(__Group), #document{key = __Group, value = #od_group{
    name = ?GROUP_NAME(__Group),
    type = ?GROUP_TYPE_ATOM(__Group)
}}).


-define(SPACE_PRIVATE_DATA_MATCHER(__Space), #document{key = __Space, value = #od_space{
    name = ?SPACE_NAME(__Space),
    owners = ?SPACE_OWNERS(__Space),
    direct_users = ?SPACE_DIRECT_USERS_MATCHER(__Space),
    eff_users = ?SPACE_EFF_USERS_MATCHER(__Space),
    direct_groups = ?SPACE_DIRECT_GROUPS_MATCHER(__Space),
    eff_groups = ?SPACE_EFF_GROUPS_MATCHER(__Space),
    providers = ?SPACE_PROVIDERS_MATCHER(__Space),
    shares = ?SPACE_SHARES(__Space),
    harvesters = ?SPACE_HARVESTERS(__Space),
    storages = ?SPACE_STORAGES_MATCHER(__Space),
    support_parameters_registry = ?SPACE_SUPPORT_PARAMETERS_REGISTRY_MATCHER(__Space)
}}).
-define(SPACE_PROTECTED_DATA_MATCHER(__Space), #document{key = __Space, value = #od_space{
    name = ?SPACE_NAME(__Space),
    owners = [],
    direct_users = #{},
    eff_users = #{},
    direct_groups = #{},
    eff_groups = #{},
    providers = ?SPACE_PROVIDERS_MATCHER(__Space),
    shares = [],
    harvesters = [],
    support_parameters_registry = ?SPACE_SUPPORT_PARAMETERS_REGISTRY_MATCHER(__Space)
}}).


-define(SHARE_PRIVATE_DATA_MATCHER(__Share), #document{key = __Share, value = #od_share{
    name = ?SHARE_NAME(__Share),
    description = ?SHARE_DESCRIPTION(__Share),
    public_url = ?SHARE_PUBLIC_URL(__Share),
    file_type = ?SHARE_FILE_TYPE(__ShareId),
    space = ?SHARE_SPACE(__Share),
    handle = ?SHARE_HANDLE(__Share),
    root_file = ?SHARE_ROOT_FILE(__Share)
}}).
-define(SHARE_PUBLIC_DATA_MATCHER(__Share), #document{key = __Share, value = #od_share{
    name = ?SHARE_NAME(__Share),
    description = ?SHARE_DESCRIPTION(__Share),
    public_url = ?SHARE_PUBLIC_URL(__Share),
    file_type = ?SHARE_FILE_TYPE(__ShareId),
    space = ?SHARE_SPACE(__Share),
    handle = ?SHARE_HANDLE(__Share),
    root_file = ?SHARE_ROOT_FILE(__Share)
}}).


-define(PROVIDER_PRIVATE_DATA_MATCHER(__Provider, __Storages), #document{key = __Provider, value = #od_provider{
    name = ?PROVIDER_NAME(__Provider),
    admin_email = ?PROVIDER_ADMIN_EMAIL(__Provider),
    subdomain_delegation = ?PROVIDER_SUBDOMAIN_DELEGATION(__Provider),
    domain = ?PROVIDER_DOMAIN(__Provider),
    version = <<"21.02.2">>,
    online = ?PROVIDER_ONLINE(__Provider),
    storages = __Storages,
    eff_spaces = ?PROVIDER_SPACES_MATCHER(__Provider),
    eff_users = ?PROVIDER_EFF_USERS(__Provider),
    eff_groups = ?PROVIDER_EFF_GROUPS(__Provider)
}}).
-define(PROVIDER_PROTECTED_DATA_MATCHER(__Provider), #document{key = __Provider, value = #od_provider{
    name = ?PROVIDER_NAME(__Provider),
    domain = ?PROVIDER_DOMAIN(__Provider),
    version = <<"21.02.2">>,
    online = ?PROVIDER_ONLINE(__Provider),
    eff_spaces = #{},
    eff_users = [],
    eff_groups = []
}}).


-define(HANDLE_SERVICE_PUBLIC_DATA_MATCHER(__HService), #document{key = __HService, value = #od_handle_service{
    name = ?HANDLE_SERVICE_NAME(__HService)
}}).


-define(HANDLE_PUBLIC_DATA_MATCHER(__Handle), #document{key = __Handle, value = #od_handle{
    public_handle = ?HANDLE_PUBLIC_HANDLE(__Handle),
    metadata_prefix = ?HANDLE_METADATA_PREFIX(__Handle),
    metadata = ?HANDLE_METADATA(__Handle),
    handle_service = ?HANDLE_H_SERVICE(__Handle)
}}).

-define(HARVESTER_PRIVATE_DATA_MATCHER(__Harvester), #document{key = __Harvester, value = #od_harvester{
    indices = ?HARVESTER_INDICES(__Harvester),
    spaces = ?HARVESTER_SPACES(__Harvester)
}}).

-define(STORAGE_PRIVATE_DATA_MATCHER(__Storage, __Provider), #document{key = __Storage, value = #od_storage{
    name = ?STORAGE_NAME(__Storage),
    provider = __Provider,
    spaces = [],
    qos_parameters = #{},
    imported = false,
    readonly = false
}}).

-define(STORAGE_SHARED_DATA_MATCHER(__Storage, __Provider), #document{key = __Storage, value = #od_storage{
    name = ?STORAGE_NAME(__Storage),
    provider = __Provider,
    qos_parameters = #{},
    readonly = false
}}).

-define(ATM_INVENTORY_PRIVATE_DATA_MATCHER(__AtmInventory), #document{key = __AtmInventory, value = #od_atm_inventory{
    name = ?ATM_INVENTORY_NAME(__AtmInventory),
    atm_lambdas = [?ATM_LAMBDA_1],
    atm_workflow_schemas = [?ATM_WORKFLOW_SCHEMA_1]
}}).

-define(ATM_LAMBDA_PRIVATE_DATA_MATCHER(__AtmLambda),
    #document{key = __AtmLambda, value = #od_atm_lambda{
        revision_registry = ?ATM_LAMBDA_REVISION_REGISTRY_MATCHER(__AtmLambda),
        atm_inventories = ?ATM_LAMBDA_INVENTORIES(__AtmLambda)
    }}).

-define(ATM_WORKFLOW_SCHEMA_PRIVATE_DATA_MATCHER(__AtmWorkflowSchema),
    #document{key = __AtmWorkflowSchema, value = #od_atm_workflow_schema{
        name = ?ATM_WORKFLOW_SCHEMA_NAME(__AtmWorkflowSchema),
        summary = ?ATM_WORKFLOW_SCHEMA_SUMMARY(__AtmWorkflowSchema),
        revision_registry = ?ATM_WORKFLOW_SCHEMA_REVISION_REGISTRY_MATCHER(__AtmWorkflowSchema),
        atm_inventory = ?ATM_WORKFLOW_SCHEMA_INVENTORY(__AtmWorkflowSchema)
    }}).


-define(USER_SHARED_DATA_VALUE(__UserId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_user, id = __UserId, aspect = instance, scope = shared}),
    <<"fullName">> => ?USER_FULL_NAME(__UserId),
    <<"username">> => ?USER_USERNAME(__UserId),
    % TODO VFS-4506 deprecated fields, included for backward compatibility
    <<"name">> => ?USER_FULL_NAME(__UserId),
    <<"login">> => ?USER_USERNAME(__UserId),
    <<"alias">> => ?USER_USERNAME(__UserId)
}).
-define(USER_PROTECTED_DATA_VALUE(__UserId), begin
    __SharedData = ?USER_SHARED_DATA_VALUE(__UserId),
    __SharedData#{
        <<"gri">> => gri:serialize(#gri{type = od_user, id = __UserId, aspect = instance, scope = protected}),
        <<"emailList">> => ?USER_EMAIL_LIST(__UserId),
        <<"linkedAccounts">> => ?USER_LINKED_ACCOUNTS_VALUE(__UserId),
        <<"blocked">> => false
    }
end).
-define(USER_PRIVATE_DATA_VALUE(__UserId), begin
    __ProtectedData = ?USER_PROTECTED_DATA_VALUE(__UserId),
    __ProtectedData#{
        <<"gri">> => gri:serialize(#gri{type = od_user, id = __UserId, aspect = instance, scope = private}),
        <<"spaceAliases">> => ?USER_SPACE_ALIASES(__UserId),

        <<"effectiveGroups">> => ?USER_EFF_GROUPS(__UserId),
        <<"effectiveSpaces">> => ?USER_EFF_SPACES(__UserId),
        <<"effectiveHandleServices">> => ?USER_EFF_HANDLE_SERVICES(__UserId),
        <<"effectiveAtmInventories">> => ?USER_EFF_ATM_INVENTORIES(__UserId)
    }
end).


-define(GROUP_SHARED_DATA_VALUE(__GroupId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_group, id = __GroupId, aspect = instance, scope = shared}),
    <<"name">> => ?GROUP_NAME(__GroupId),
    <<"type">> => ?GROUP_TYPE_JSON(__GroupId)
}).


-define(SPACE_PROTECTED_DATA_VALUE(__SpaceId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_space, id = __SpaceId, aspect = instance, scope = protected}),
    <<"name">> => ?SPACE_NAME(__SpaceId),
    <<"providers">> => ?SPACE_PROVIDERS_VALUE(__SpaceId),
    <<"supportParametersRegistry">> => ?SPACE_SUPPORT_PARAMETERS_REGISTRY_VALUE(__SpaceId)
}).
-define(SPACE_PRIVATE_DATA_VALUE(__SpaceId), begin
    (?SPACE_PROTECTED_DATA_VALUE(__SpaceId))#{
        <<"gri">> => gri:serialize(#gri{type = od_space, id = __SpaceId, aspect = instance, scope = private}),
        <<"owners">> => ?SPACE_OWNERS(__SpaceId),

        <<"users">> => ?SPACE_DIRECT_USERS_VALUE(__SpaceId),
        <<"effectiveUsers">> => ?SPACE_EFF_USERS_VALUE(__SpaceId),

        <<"groups">> => ?SPACE_DIRECT_GROUPS_VALUE(__SpaceId),
        <<"effectiveGroups">> => ?SPACE_EFF_GROUPS_VALUE(__SpaceId),

        <<"storages">> => ?SPACE_STORAGES_VALUE(__SpaceId),
        <<"shares">> => ?SPACE_SHARES(__SpaceId),
        <<"harvesters">> => ?SPACE_HARVESTERS(__SpaceId)
    }
end).


-define(SHARE_PUBLIC_DATA_VALUE(__ShareId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_share, id = __ShareId, aspect = instance, scope = public}),
    <<"spaceId">> => ?SHARE_SPACE(__ShareId),
    <<"name">> => ?SHARE_NAME(__ShareId),
    <<"description">> => ?SHARE_DESCRIPTION(__ShareId),
    <<"publicUrl">> => ?SHARE_PUBLIC_URL(__ShareId),
    <<"publicRestUrl">> => ?SHARE_PUBLIC_REST_URL(__ShareId),
    <<"fileType">> => atom_to_binary(?SHARE_FILE_TYPE(__ShareId), utf8),
    <<"handleId">> => ?SHARE_HANDLE(__ShareId),
    <<"rootFileId">> => ?SHARE_ROOT_FILE(__ShareId)
}).
-define(SHARE_PRIVATE_DATA_VALUE(__ShareId), begin
    __PublicData = ?SHARE_PUBLIC_DATA_VALUE(__ShareId),
    __PublicData#{
        <<"gri">> => gri:serialize(#gri{type = od_share, id = __ShareId, aspect = instance, scope = private})
    }
end).


-define(PROVIDER_PROTECTED_DATA_VALUE(__ProviderId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_provider, id = __ProviderId, aspect = instance, scope = protected}),
    <<"name">> => ?PROVIDER_NAME(__ProviderId),
    <<"domain">> => ?PROVIDER_DOMAIN(__ProviderId),
    <<"version">> => <<"21.02.2">>,
    <<"online">> => ?PROVIDER_ONLINE(__ProviderId),
    <<"latitude">> => ?PROVIDER_LATITUDE(__ProviderId),
    <<"longitude">> => ?PROVIDER_LONGITUDE(__ProviderId)
}).
-define(PROVIDER_PRIVATE_DATA_VALUE(__ProviderId), begin
    __ProtectedData = ?PROVIDER_PROTECTED_DATA_VALUE(__ProviderId),
    __ProtectedData#{
        <<"adminEmail">> => ?PROVIDER_ADMIN_EMAIL(__ProviderId),
        <<"subdomainDelegation">> => ?PROVIDER_SUBDOMAIN_DELEGATION(__ProviderId),
        <<"subdomain">> => ?PROVIDER_SUBDOMAIN(__ProviderId),
        <<"gri">> => gri:serialize(#gri{type = od_provider, id = __ProviderId, aspect = instance, scope = private}),
        <<"effectiveSpaces">> => case __ProviderId of
            ?DUMMY_PROVIDER_ID -> #{};
            _ -> ?PROVIDER_SPACES_VALUE(__ProviderId)
        end,
        <<"storages">> => ?PROVIDER_STORAGES(__ProviderId),
        <<"effectiveUsers">> => case __ProviderId of
            ?DUMMY_PROVIDER_ID -> [];
            _ -> ?PROVIDER_EFF_USERS(__ProviderId)
        end,
        <<"effectiveGroups">> => case __ProviderId of
            ?DUMMY_PROVIDER_ID -> [];
            _ -> ?PROVIDER_EFF_GROUPS(__ProviderId)
        end
    }
end).


-define(HANDLE_SERVICE_PUBLIC_DATA_VALUE(__HServiceId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_handle_service, id = __HServiceId, aspect = instance, scope = public}),
    <<"name">> => ?HANDLE_SERVICE_NAME(__HServiceId)
}).


-define(HANDLE_PUBLIC_DATA_VALUE(__HandleId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_handle, id = __HandleId, aspect = instance, scope = public}),
    <<"handleServiceId">> => ?HANDLE_H_SERVICE(__HandleId),
    <<"publicHandle">> => ?HANDLE_PUBLIC_HANDLE(__HandleId),
    <<"metadataPrefix">> => ?HANDLE_METADATA_PREFIX(__HandleId),
    <<"metadata">> => ?HANDLE_METADATA(__HandleId)
}).


-define(HARVESTER_PRIVATE_DATA_VALUE(__HarvesterId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_harvester, id = __HarvesterId, aspect = instance, scope = private}),
    <<"indices">> => ?HARVESTER_INDICES(__HarvesterId),
    <<"spaces">> => ?HARVESTER_SPACES(__HarvesterId)
}).

-define(STORAGE_PRIVATE_DATA_VALUE(__StorageId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_storage, id = __StorageId, aspect = instance, scope = private}),
    <<"name">> => ?STORAGE_NAME(__StorageId),
    <<"provider">> => ?STORAGE_PROVIDER(__StorageId),
    <<"spaces">> => [],
    <<"qosParameters">> => #{},
    <<"imported">> => false,
    <<"readonly">> => false
}).

-define(STORAGE_SHARED_DATA_VALUE(__StorageId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_storage, id = __StorageId, aspect = instance, scope = shared}),
    <<"name">> => ?STORAGE_NAME(__StorageId),
    <<"provider">> => ?STORAGE_PROVIDER(__StorageId),
    <<"qosParameters">> => #{},
    <<"readonly">> => false
}).

-define(TOKEN_SHARED_DATA_VALUE(__TokenId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_token, id = __TokenId, aspect = instance, scope = shared}),
    <<"revoked">> => false
}).

-define(TOKEN_SHARED_DATA_MATCHER(__TokenId), #document{key = __TokenId, value = #od_token{
    revoked = false
}}).


-define(TEMPORARY_TOKENS_SECRET_SHARED_DATA_VALUE(__UserId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = temporary_token_secret, id = __UserId, aspect = user, scope = shared}),
    <<"generation">> => 1
}).

-define(TEMPORARY_TOKENS_SECRET_GENERATION(__UserId), 1).


-define(ATM_INVENTORY_PRIVATE_DATA_VALUE(__AtmInventoryId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_atm_inventory, id = __AtmInventoryId, aspect = instance, scope = private}),
    <<"name">> => ?ATM_INVENTORY_NAME(__AtmInventoryId),
    <<"atmLambdas">> => [?ATM_LAMBDA_1],
    <<"atmWorkflowSchemas">> => [?ATM_WORKFLOW_SCHEMA_1]
}).


-define(ATM_LAMBDA_PRIVATE_DATA_VALUE(__AtmLambdaId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_atm_lambda, id = __AtmLambdaId, aspect = instance, scope = private}),
    <<"revisionRegistry">> => jsonable_record:to_json(?ATM_LAMBDA_REVISION_REGISTRY_VALUE(__AtmLambdaId), atm_lambda_revision_registry),
    <<"atmInventories">> => ?ATM_LAMBDA_INVENTORIES(__AtmLambdaId)
}).


-define(ATM_WORKFLOW_SCHEMA_PRIVATE_DATA_VALUE(__AtmWorkflowSchemaId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_atm_workflow_schema, id = __AtmWorkflowSchemaId, aspect = instance, scope = private}),
    <<"name">> => ?ATM_WORKFLOW_SCHEMA_NAME(__AtmWorkflowSchemaId),
    <<"summary">> => ?ATM_WORKFLOW_SCHEMA_SUMMARY(__AtmWorkflowSchemaId),
    <<"revisionRegistry">> => jsonable_record:to_json(?ATM_WORKFLOW_SCHEMA_REVISION_REGISTRY_VALUE(__AtmWorkflowSchemaId), atm_workflow_schema_revision_registry),
    <<"atmInventoryId">> => ?ATM_WORKFLOW_SCHEMA_INVENTORY(__AtmWorkflowSchemaId)
}).
