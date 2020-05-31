%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning transfer create API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_create_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_test_runner.hrl").
-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("test_utils/initializer.hrl").
-include("../transfers_test_mechanism.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("inets/include/httpd.hrl").


%% httpd callback
-export([do/1]).

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_file_replication/1,
    create_file_eviction/1,
    create_file_migration/1,

    create_view_replication/1,
    create_view_eviction/1,
    create_view_migration/1
]).

all() ->
    ?ALL([
        create_file_replication,
        create_file_eviction,
        create_file_migration,

        create_view_replication,
        create_view_eviction,
        create_view_migration
    ]).


-define(TRANSFER_TYPES, [<<"replication">>, <<"eviction">>, <<"migration">>]).
-define(DATA_SOURCE_TYPES, [<<"file">>, <<"view">>]).

-define(CLIENT_SPEC_FOR_TRANSFER_SCENARIOS(__CONFIG), #client_spec{
    correct = [?USER_IN_BOTH_SPACES_AUTH],
    unauthorized = [?NOBODY],
    forbidden_not_in_space = [?USER_IN_SPACE_1_AUTH],
    forbidden_in_space = [
        % forbidden by lack of privileges (even though being owner of files)
        ?USER_IN_SPACE_2_AUTH
    ],
    supported_clients_per_node = ?SUPPORTED_CLIENTS_PER_NODE(__CONFIG)
}).

% Parameters having below value were not assigned proper value in data_spec()
% definition and should be given one by `prepare_arg_fun`
-define(PLACEHOLDER, placeholder).

-define(TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES, [
    {<<"type">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"type">>)}},
    {<<"type">>, <<"transfer">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"type">>, ?TRANSFER_TYPES)},
    {<<"dataSourceType">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"dataSourceType">>)}},
    {<<"dataSourceType">>, <<"data">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"dataSourceType">>, ?DATA_SOURCE_TYPES)}
]).

-define(PROVIDER_ID_REPLICA_ERRORS(__KEY), [
    {__KEY, 100, {gs, ?ERROR_BAD_VALUE_BINARY(__KEY)}},
    {__KEY, <<"NonExistingProvider">>, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"NonExistingProvider">>)}
]).
-define(PROVIDER_ID_TRANSFER_ERRORS(__KEY), [
    {__KEY, 100, ?ERROR_BAD_VALUE_BINARY(__KEY)},
    {__KEY, <<"NonExistingProvider">>, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"NonExistingProvider">>)}
]).
-define(CALLBACK_ERRORS(__KEY), [{__KEY, 100, __KEY}]).

-define(BYTES_NUM, 20).

-define(HTTP_SERVER_PORT, 8080).
-define(ENDED_TRANSFERS_PATH, "/ended_transfers").

-define(TEST_PROCESS, test_process).
-define(CALLBACK_CALL_TIME(__TRANSFER_ID, __TIME), {callback_call_time, __TRANSFER_ID, __TIME}).


%%%===================================================================
%%% File transfer test functions
%%%===================================================================


create_file_replication(Config) ->
    create_file_transfer(Config, replication).


create_file_eviction(Config) ->
    create_file_transfer(Config, eviction).


create_file_migration(Config) ->
    create_file_transfer(Config, migration).


create_file_transfer(Config, Type) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?SESS_ID(?USER_IN_SPACE_2, P1, Config),
    SessIdP2 = ?SESS_ID(?USER_IN_SPACE_2, P2, Config),

    % Shared file will be used to assert that shared file transfer will be forbidden
    % (it will be added to '#data_spec.bad_values')
    FileGuid = create_file(P1, SessIdP1, filename:join(["/", ?SPACE_2])),
    {ok, ShareId} = lfm_proxy:create_share(P1, SessIdP1, {guid, FileGuid}, <<"share">>),
    api_test_utils:wait_for_file_sync(P2, SessIdP2, FileGuid),

    RequiredPrivs = file_transfer_required_privs(Type),
    set_space_privileges(Providers, ?SPACE_2, ?USER_IN_SPACE_2, privileges:space_admin() -- RequiredPrivs),
    set_space_privileges(Providers, ?SPACE_2, ?USER_IN_BOTH_SPACES, RequiredPrivs),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_TRANSFER_SCENARIOS(Config),
            setup_fun = create_setup_file_replication_env_fun(Type, P1, P2, ?USER_IN_SPACE_2, Config),
            verify_fun = create_verify_transfer_env_fun(Type, P2, ?USER_IN_SPACE_2, P1, P2, Config),
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Transfer (~p) file using gs transfer api", [Type]),
                    type = gs,
                    prepare_args_fun = create_prepare_transfer_create_instance_gs_args_fun(private),
                    validate_result_fun = fun validate_transfer_gs_call_result/2
                }
            ],
            data_spec = get_file_transfer_op_transfer_spec(Type, P1, P2, FileGuid, ShareId)
        },

        %% TEST DEPRECATED REPLICAS ENDPOINTS

        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_TRANSFER_SCENARIOS(Config),
            setup_fun = create_setup_file_replication_env_fun(Type, P1, P2, ?USER_IN_SPACE_2, Config),
            verify_fun = create_verify_transfer_env_fun(Type, P2, ?USER_IN_SPACE_2, P1, P2, Config),
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Transfer (~p) file using /replicas/ rest endpoint", [Type]),
                    type = rest_with_file_path,
                    prepare_args_fun = create_prepare_replica_rest_args_fun(Type),
                    validate_result_fun = fun validate_transfer_rest_call_result/2
                },
                #scenario_template{
                    name = str_utils:format("Transfer (~p) file using /replicas-id/ rest endpoint", [Type]),
                    type = rest,
                    prepare_args_fun = create_prepare_replica_rest_args_fun(Type),
                    validate_result_fun = fun validate_transfer_rest_call_result/2
                },
                #scenario_template{
                    name = str_utils:format("Transfer (~p) file using op_replica gs api", [Type]),
                    type = gs,
                    prepare_args_fun = create_prepare_replica_gs_args_fun(Type, private),
                    validate_result_fun = fun validate_transfer_gs_call_result/2
                }
            ],
            data_spec = get_file_transfer_op_replica_spec(Type, P1, P2, FileGuid, ShareId)
        }
    ])).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates either single file or directory with 5 files (random select) and
%% awaits metadata synchronization between nodes. In case of 'eviction' also
%% copies data so that it would exist on all nodes.
%% FileGuid to transfer and expected transfer stats are saved in returned map.
%% @end
%%--------------------------------------------------------------------
create_setup_file_replication_env_fun(TransferType, SrcNode, DstNode, UserId, Config) ->
    fun() ->
        SessId1 = ?SESS_ID(UserId, SrcNode, Config),
        SessId2 = ?SESS_ID(UserId, DstNode, Config),

        RootFileType = api_test_utils:randomly_choose_file_type_for_test(false),
        RootFilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
        {ok, RootFileGuid} = api_test_utils:create_file(
            RootFileType, SrcNode, SessId1, RootFilePath, 8#777
        ),
        {ok, RootFileObjectId} = file_id:guid_to_objectid(RootFileGuid),

        FilesToTransfer = case RootFileType of
            <<"file">> ->
                api_test_utils:fill_file_with_dummy_data(SrcNode, SessId1, RootFileGuid, ?BYTES_NUM),
                [RootFileGuid];
            <<"dir">> ->
                lists:map(fun(_) ->
                    create_file(SrcNode, SessId1, RootFilePath)
                end, lists:seq(1, 5))
        end,
        OtherFiles = [create_file(SrcNode, SessId1, filename:join(["/", ?SPACE_2]))],

        sync_files_between_nodes(
            TransferType, SrcNode, SessId1, DstNode, SessId2,
            OtherFiles ++ FilesToTransfer
        ),

        ExpTransfer = get_exp_transfer_stats(
            TransferType, RootFileType, SrcNode, DstNode, length(FilesToTransfer)
        ),

        #{
            root_file_guid => RootFileGuid,
            root_file_path => RootFilePath,
            root_file_cdmi_id => RootFileObjectId,
            exp_transfer => ExpTransfer#{
                space_id => ?SPACE_2,
                file_uuid => file_id:guid_to_uuid(RootFileGuid),
                path => RootFilePath,
                failed_files => 0
            },
            files_to_transfer => FilesToTransfer,
            other_files => OtherFiles
        }
    end.


%% @private
file_transfer_required_privs(replication) ->
    [?SPACE_SCHEDULE_REPLICATION];
file_transfer_required_privs(eviction) ->
    [?SPACE_SCHEDULE_EVICTION];
file_transfer_required_privs(migration) ->
    [?SPACE_SCHEDULE_REPLICATION, ?SPACE_SCHEDULE_EVICTION].


%% @private
get_file_transfer_op_replica_spec(replication, _SrcNode, DstNode, FileGuid, ShareId) ->
    api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
        FileGuid, ShareId, #data_spec{
            required = [<<"provider_id">>],
            optional = [<<"url">>],
            correct_values = #{
                <<"provider_id">> => [?GET_DOMAIN_BIN(DstNode)],
                <<"url">> => [get_callback_url()]
            },
            bad_values = ?PROVIDER_ID_REPLICA_ERRORS(<<"provider_id">>) ++ [
                {<<"url">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"url">>)}}
            ]
        }
    );
get_file_transfer_op_replica_spec(eviction, SrcNode, _DstNode, FileGuid, ShareId) ->
    api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
        FileGuid, ShareId, #data_spec{
            required = [<<"provider_id">>],
            correct_values = #{
                <<"provider_id">> => [?GET_DOMAIN_BIN(SrcNode)]
            },
            bad_values = ?PROVIDER_ID_REPLICA_ERRORS(<<"provider_id">>)
        }
    );
get_file_transfer_op_replica_spec(migration, SrcNode, DstNode, FileGuid, ShareId) ->
    api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
        FileGuid, ShareId, #data_spec{
            required = [<<"provider_id">>, <<"migration_provider_id">>],
            correct_values = #{
                <<"provider_id">> => [?GET_DOMAIN_BIN(SrcNode)],
                <<"migration_provider_id">> => [?GET_DOMAIN_BIN(DstNode)]
            },
            bad_values = lists:flatten([
                ?PROVIDER_ID_REPLICA_ERRORS(<<"provider_id">>),
                ?PROVIDER_ID_REPLICA_ERRORS(<<"migration_provider_id">>)
            ])
        }
    ).


%% @private
get_file_transfer_op_transfer_spec(replication, _SrcNode, DstNode, FileGuid, ShareId) ->
    add_file_id_bad_values(FileGuid, ?SPACE_2, ShareId, #data_spec{
        required = [
            <<"type">>, <<"dataSourceType">>, <<"fileId">>,
            <<"replicatingProviderId">>
        ],
        optional = [<<"callback">>],
        correct_values = #{
            <<"type">> => [<<"replication">>],
            <<"dataSourceType">> => [<<"file">>],
            <<"fileId">> => [?PLACEHOLDER],
            <<"replicatingProviderId">> => [?GET_DOMAIN_BIN(DstNode)],
            <<"callback">> => [get_callback_url()]
        },
        bad_values = lists:flatten([
            ?TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES,
            ?PROVIDER_ID_TRANSFER_ERRORS(<<"replicatingProviderId">>),
            ?CALLBACK_ERRORS(<<"callback">>)
        ])
    });
get_file_transfer_op_transfer_spec(eviction, SrcNode, _DstNode, FileGuid, ShareId) ->
    add_file_id_bad_values(FileGuid, ?SPACE_2, ShareId, #data_spec{
        required = [
            <<"type">>, <<"dataSourceType">>, <<"fileId">>,
            <<"evictingProviderId">>
        ],
        optional = [<<"callback">>],
        correct_values = #{
            <<"type">> => [<<"eviction">>],
            <<"dataSourceType">> => [<<"file">>],
            <<"fileId">> => [?PLACEHOLDER],
            <<"evictingProviderId">> => [?GET_DOMAIN_BIN(SrcNode)],
            <<"callback">> => [get_callback_url()]
        },
        bad_values = lists:flatten([
            ?TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES,
            ?PROVIDER_ID_TRANSFER_ERRORS(<<"evictingProviderId">>),
            ?CALLBACK_ERRORS(<<"callback">>)
        ])
    });
get_file_transfer_op_transfer_spec(migration, SrcNode, DstNode, FileGuid, ShareId) ->
    add_file_id_bad_values(FileGuid, ?SPACE_2, ShareId, #data_spec{
        required = [
            <<"type">>, <<"dataSourceType">>, <<"fileId">>,
            <<"replicatingProviderId">>, <<"evictingProviderId">>
        ],
        optional = [<<"callback">>],
        correct_values = #{
            <<"type">> => [<<"migration">>],
            <<"dataSourceType">> => [<<"file">>],
            <<"fileId">> => [?PLACEHOLDER],
            <<"replicatingProviderId">> => [?GET_DOMAIN_BIN(DstNode)],
            <<"evictingProviderId">> => [?GET_DOMAIN_BIN(SrcNode)],
            <<"callback">> => [get_callback_url()]
        },
        bad_values = lists:flatten([
            ?TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES,
            ?PROVIDER_ID_TRANSFER_ERRORS(<<"replicatingProviderId">>),
            ?PROVIDER_ID_TRANSFER_ERRORS(<<"evictingProviderId">>),
            ?CALLBACK_ERRORS(<<"callback">>)
        ])
    }).


%% @private
-spec add_file_id_bad_values(file_id:file_guid(), od_space:id(), od_share:id(), data_spec()) ->
    data_spec().
add_file_id_bad_values(FileGuid, SpaceId, ShareId, #data_spec{bad_values = BadValues} = DataSpec) ->
    {ok, DummyObjectId} = file_id:guid_to_objectid(<<"DummyGuid">>),

    NonExistentSpaceGuid = file_id:pack_guid(<<"InvalidUuid">>, ?NOT_SUPPORTED_SPACE_ID),
    {ok, NonExistentSpaceObjectId} = file_id:guid_to_objectid(NonExistentSpaceGuid),

    NonExistentSpaceShareGuid = file_id:guid_to_share_guid(NonExistentSpaceGuid, ShareId),
    {ok, NonExistentSpaceShareObjectId} = file_id:guid_to_objectid(NonExistentSpaceShareGuid),

    NonExistentFileGuid = file_id:pack_guid(<<"InvalidUuid">>, SpaceId),
    {ok, NonExistentFileObjectId} = file_id:guid_to_objectid(NonExistentFileGuid),

    NonExistentFileShareGuid = file_id:guid_to_share_guid(NonExistentFileGuid, ShareId),
    {ok, NonExistentFileShareObjectId} = file_id:guid_to_objectid(NonExistentFileShareGuid),

    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    {ok, ShareFileObjectId} = file_id:guid_to_objectid(ShareFileGuid),

    BadFileIdValues = [
        {<<"fileId">>, <<"InvalidObjectId">>, ?ERROR_BAD_VALUE_IDENTIFIER(<<"fileId">>)},
        {<<"fileId">>, DummyObjectId, ?ERROR_BAD_VALUE_IDENTIFIER(<<"fileId">>)},

        {<<"fileId">>, NonExistentSpaceObjectId, ?ERROR_FORBIDDEN},
        {<<"fileId">>, NonExistentSpaceShareObjectId, ?ERROR_FORBIDDEN},

        {<<"fileId">>, NonExistentFileObjectId, ?ERROR_POSIX(?ENOENT)},
        {<<"fileId">>, NonExistentFileShareObjectId, ?ERROR_FORBIDDEN},

        {<<"fileId">>, ShareFileObjectId, ?ERROR_FORBIDDEN}
    ],
    DataSpec#data_spec{bad_values = BadFileIdValues ++ BadValues}.


%%%===================================================================
%%% View transfer test functions
%%%===================================================================


create_view_replication(Config) ->
    create_view_transfer(Config, replication).


create_view_eviction(Config) ->
    create_view_transfer(Config, eviction).


create_view_migration(Config) ->
    create_view_transfer(Config, migration).


%% @private
create_view_transfer(Config, Type) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),

    RequiredPrivs = view_transfer_required_privs(Type),
    set_space_privileges(Providers, ?SPACE_2, ?USER_IN_SPACE_2, privileges:space_admin() -- RequiredPrivs),
    set_space_privileges(Providers, ?SPACE_2, ?USER_IN_BOTH_SPACES, RequiredPrivs),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_TRANSFER_SCENARIOS(Config),
            setup_fun = create_setup_view_transfer_env_fun(Type, P1, P2, ?USER_IN_SPACE_2, Config),
            verify_fun = create_verify_transfer_env_fun(Type, P2, ?USER_IN_SPACE_2, P1, P2, Config),
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Transfer (~p) view using gs transfer api", [Type]),
                    type = gs,
                    prepare_args_fun = create_prepare_transfer_create_instance_gs_args_fun(private),
                    validate_result_fun = fun validate_transfer_gs_call_result/2
                }
            ],
            data_spec = get_view_transfer_op_transfer_spec(Type, P1, P2)
        },

        %% TEST DEPRECATED REPLICAS ENDPOINTS

        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_TRANSFER_SCENARIOS(Config),
            setup_fun = create_setup_view_transfer_env_fun(Type, P1, P2, ?USER_IN_SPACE_2, Config),
            verify_fun = create_verify_transfer_env_fun(Type, P2, ?USER_IN_SPACE_2, P1, P2, Config),
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Transfer (~p) view using /replicas-view/ rest endpoint", [Type]),
                    type = rest,
                    prepare_args_fun = create_prepare_replica_rest_args_fun(Type),
                    validate_result_fun = fun validate_transfer_rest_call_result/2
                },
                #scenario_template{
                    name = str_utils:format("Transfer (~p) view using op_replica gs api", [Type]),
                    type = gs,
                    prepare_args_fun = create_prepare_replica_gs_args_fun(Type, private),
                    validate_result_fun = fun validate_transfer_gs_call_result/2
                }
            ],
            data_spec = get_view_transfer_op_replica_spec(Type, P1, P2)
        }
    ])).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates view with 3 files to transfer. ViewName and expected transfer
%% stats are saved in returned map.
%% @end
%%--------------------------------------------------------------------
create_setup_view_transfer_env_fun(TransferType, SrcNode, DstNode, UserId, Config) ->
    fun() ->
        SessId1 = ?SESS_ID(UserId, SrcNode, Config),
        SessId2 = ?SESS_ID(UserId, DstNode, Config),

        FilesToTransferNum = 3,
        RootDirPath = filename:join(["/", ?SPACE_2]),
        {ViewName, Xattr} = create_view(TransferType, SrcNode, DstNode),

        FilesToTransfer = lists:map(fun(_) ->
            FileGuid = create_file(SrcNode, SessId1, RootDirPath),
            ?assertMatch(ok, lfm_proxy:set_xattr(SrcNode, SessId1, {guid, FileGuid}, Xattr)),
            FileGuid
        end, lists:seq(1, FilesToTransferNum)),

        OtherFiles = [create_file(SrcNode, SessId1, RootDirPath)],

        sync_files_between_nodes(
            TransferType, SrcNode, SessId1, DstNode, SessId2,
            OtherFiles ++ FilesToTransfer
        ),

        ObjectIds = api_test_utils:guids_to_object_ids(FilesToTransfer),
        QueryViewParams = [{key, Xattr#xattr.value}],

        case TransferType of
            replication ->
                % Wait until all FilesToTransfer are indexed by view on DstNode.
                % Otherwise some of them could be omitted from replication.
                ?assertViewQuery(ObjectIds, DstNode, ?SPACE_2, ViewName,  QueryViewParams);
            eviction ->
                % Wait until all FilesToTransfer are indexed by view on SrcNode.
                % Otherwise some of them could be omitted from eviction.
                ?assertViewQuery(ObjectIds, SrcNode, ?SPACE_2, ViewName,  QueryViewParams);
            migration ->
                % Wait until FilesToTransfer are all indexed by view on SrcNode and DstNode.
                % Otherwise some of them could be omitted from replication/eviction.
                ?assertViewQuery(ObjectIds, SrcNode, ?SPACE_2, ViewName,  QueryViewParams),
                ?assertViewQuery(ObjectIds, DstNode, ?SPACE_2, ViewName,  QueryViewParams)
        end,
        ExpTransfer = get_exp_transfer_stats(
            TransferType, <<"dir">>, SrcNode, DstNode, length(FilesToTransfer)
        ),

        #{
            view_name => ViewName,
            exp_transfer => ExpTransfer#{
                space_id => ?SPACE_2,
                file_uuid => fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_2),
                path => RootDirPath,
                failed_files => 0
            },
            files_to_transfer => FilesToTransfer,
            other_files => OtherFiles
        }
    end.


%% @private
view_transfer_required_privs(replication) ->
    [?SPACE_SCHEDULE_REPLICATION, ?SPACE_QUERY_VIEWS];
view_transfer_required_privs(eviction) ->
    [?SPACE_SCHEDULE_EVICTION, ?SPACE_QUERY_VIEWS];
view_transfer_required_privs(migration) ->
    [?SPACE_SCHEDULE_REPLICATION, ?SPACE_SCHEDULE_EVICTION, ?SPACE_QUERY_VIEWS].


%% @private
get_view_transfer_op_replica_spec(replication, _SrcNode, DstNode) ->
    #data_spec{
        required = [<<"provider_id">>],
        optional = [<<"url">>],
        correct_values = #{
            <<"provider_id">> => [?GET_DOMAIN_BIN(DstNode)],
            <<"url">> => [get_callback_url()]
        },
        bad_values = ?PROVIDER_ID_REPLICA_ERRORS(<<"provider_id">>) ++ [
            {<<"url">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"url">>)}}
        ]
    };
get_view_transfer_op_replica_spec(eviction, SrcNode, _DstNode) ->
    #data_spec{
        required = [<<"provider_id">>],
        correct_values = #{
            <<"provider_id">> => [?GET_DOMAIN_BIN(SrcNode)]
        },
        bad_values = ?PROVIDER_ID_REPLICA_ERRORS(<<"provider_id">>)
    };
get_view_transfer_op_replica_spec(migration, SrcNode, DstNode) ->
    #data_spec{
        required = [<<"provider_id">>, <<"migration_provider_id">>, <<"space_id">>],
        correct_values = #{
            <<"provider_id">> => [?GET_DOMAIN_BIN(SrcNode)],
            <<"migration_provider_id">> => [?GET_DOMAIN_BIN(DstNode)],
            <<"space_id">> => [?SPACE_2]
        },
        bad_values = lists:flatten([
            ?PROVIDER_ID_REPLICA_ERRORS(<<"provider_id">>),
            ?PROVIDER_ID_REPLICA_ERRORS(<<"migration_provider_id">>)
        ])
    }.


%% @private
get_view_transfer_op_transfer_spec(replication, _SrcNode, DstNode) ->
    #data_spec{
        required = [
            <<"type">>, <<"dataSourceType">>, <<"spaceId">>, <<"viewName">>,
            <<"replicatingProviderId">>
        ],
        optional = [<<"callback">>],
        correct_values = #{
            <<"type">> => [<<"replication">>],
            <<"dataSourceType">> => [<<"view">>],
            <<"spaceId">> => [?SPACE_2],
            <<"viewName">> => [?PLACEHOLDER],
            <<"replicatingProviderId">> => [?GET_DOMAIN_BIN(DstNode)],
            <<"callback">> => [get_callback_url()]
        },
        bad_values = lists:flatten([
            ?TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES,
            ?PROVIDER_ID_TRANSFER_ERRORS(<<"replicatingProviderId">>),
            ?CALLBACK_ERRORS(<<"callback">>)
        ])
    };
get_view_transfer_op_transfer_spec(eviction, SrcNode, _DstNode) ->
    #data_spec{
        required = [
            <<"type">>, <<"dataSourceType">>, <<"spaceId">>, <<"viewName">>,
            <<"evictingProviderId">>
        ],
        optional = [<<"callback">>],
        correct_values = #{
            <<"type">> => [<<"eviction">>],
            <<"dataSourceType">> => [<<"view">>],
            <<"spaceId">> => [?SPACE_2],
            <<"viewName">> => [?PLACEHOLDER],
            <<"evictingProviderId">> => [?GET_DOMAIN_BIN(SrcNode)],
            <<"callback">> => [get_callback_url()]
        },
        bad_values = lists:flatten([
            ?TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES,
            ?PROVIDER_ID_TRANSFER_ERRORS(<<"evictingProviderId">>),
            ?CALLBACK_ERRORS(<<"callback">>)
        ])
    };
get_view_transfer_op_transfer_spec(migration, SrcNode, DstNode) ->
    #data_spec{
        required = [
            <<"type">>, <<"dataSourceType">>, <<"spaceId">>, <<"viewName">>,
            <<"replicatingProviderId">>, <<"evictingProviderId">>
        ],
        optional = [<<"callback">>],
        correct_values = #{
            <<"type">> => [<<"migration">>],
            <<"dataSourceType">> => [<<"view">>],
            <<"spaceId">> => [?SPACE_2],
            <<"viewName">> => [?PLACEHOLDER],
            <<"replicatingProviderId">> => [?GET_DOMAIN_BIN(DstNode)],
            <<"evictingProviderId">> => [?GET_DOMAIN_BIN(SrcNode)],
            <<"callback">> => [get_callback_url()]
        },
        bad_values = lists:flatten([
            ?TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES,
            ?PROVIDER_ID_TRANSFER_ERRORS(<<"replicatingProviderId">>),
            ?PROVIDER_ID_TRANSFER_ERRORS(<<"evictingProviderId">>),
            ?CALLBACK_ERRORS(<<"callback">>)
        ])
    }.


%%%===================================================================
%%% Common transfer helper functions
%%%===================================================================


%% @private
create_prepare_transfer_create_instance_gs_args_fun(Scope) ->
    fun(#api_test_ctx{env = Env, data = Data0}) ->
        Data1 = case Env of
            #{root_file_cdmi_id := FileObjectId} ->
                replace_placeholder_value(<<"fileId">>, FileObjectId, Data0);
            #{view_name := ViewName} ->
                replace_placeholder_value(<<"viewName">>, ViewName, Data0)
        end,
        #gs_args{
            operation = create,
            gri = #gri{type = op_transfer, aspect = instance, scope = Scope},
            data = Data1
        }
    end.


%% @private
create_prepare_replica_rest_args_fun(Type) ->
    Method = case Type of
        replication -> post;
        _ -> delete
    end,

    fun(#api_test_ctx{scenario = Scenario, env = Env, data = Data0}) ->
        Data1 = api_test_utils:ensure_defined(Data0, #{}),
        {InvalidId, Data2} = api_test_utils:maybe_substitute_id(undefined, Data1),
        RestPath = case Env of
            #{root_file_path := FilePath} when Scenario =:= rest_with_file_path  ->
                <<"replicas", (api_test_utils:ensure_defined(InvalidId, FilePath))/binary>>;
            #{root_file_cdmi_id := FileObjectId} ->
                <<"replicas-id/", (api_test_utils:ensure_defined(InvalidId, FileObjectId))/binary>>;
            #{view_name := ViewName} ->
                <<"replicas-view/", (api_test_utils:ensure_defined(InvalidId, ViewName))/binary>>
        end,
        {Body, Data4} = case maps:take(<<"url">>, Data2) of
            {Url, Data3} ->
                {json_utils:encode(#{<<"url">> => Url}), Data3};
            error ->
                {<<>>, Data2}
        end,
        #rest_args{
            method = Method,
            path = http_utils:append_url_parameters(RestPath, Data4),
            headers = #{<<"content-type">> => <<"application/json">>},
            body = Body
        }
    end.


%% @private
create_prepare_replica_gs_args_fun(Type, Scope) ->
    Operation = case Type of
        replication -> create;
        _ -> delete
    end,

    fun(#api_test_ctx{env = Env, data = Data0}) ->
        {ValidId, Aspect} = case Env of
            #{root_file_guid := FileGuid} ->
                {FileGuid, instance};
            #{view_name := ViewName} when Operation == create ->
                {ViewName, replicate_by_view};
            #{view_name := ViewName} when Operation == delete ->
                {ViewName, evict_by_view}
        end,
        {GriId, Data1} = api_test_utils:maybe_substitute_id(ValidId, Data0),

        #gs_args{
            operation = Operation,
            gri = #gri{type = op_replica, id = GriId, aspect = Aspect, scope = Scope},
            data = Data1
        }
    end.


%% @private
get_exp_transfer_stats(replication, <<"file">>, _SrcNode, DstNode, _FilesToTransferNum) ->
    #{
        replication_status => completed,
        eviction_status => skipped,
        replicating_provider => transfers_test_utils:provider_id(DstNode),
        evicting_provider => undefined,
        files_to_process => 1,
        files_processed => 1,
        files_replicated => 1,
        bytes_replicated => ?BYTES_NUM,
        files_evicted => 0
    };
get_exp_transfer_stats(eviction, <<"file">>, SrcNode, _DstNode, _FilesToTransferNum) ->
    #{
        replication_status => skipped,
        eviction_status => completed,
        replicating_provider => undefined,
        evicting_provider => transfers_test_utils:provider_id(SrcNode),
        files_to_process => 1,
        files_processed => 1,
        files_replicated => 0,
        bytes_replicated => 0,
        files_evicted => 1
    };
get_exp_transfer_stats(migration, <<"file">>, SrcNode, DstNode, _FilesToTransferNum) ->
    #{
        replication_status => completed,
        eviction_status => completed,
        replicating_provider => transfers_test_utils:provider_id(DstNode),
        evicting_provider => transfers_test_utils:provider_id(SrcNode),
        files_to_process => 2,
        files_processed => 2,
        files_replicated => 1,
        bytes_replicated => ?BYTES_NUM,
        files_evicted => 1
    };
get_exp_transfer_stats(replication, <<"dir">>, _SrcNode, DstNode, FilesToTransferNum) ->
    #{
        replication_status => completed,
        eviction_status => skipped,
        replicating_provider => transfers_test_utils:provider_id(DstNode),
        evicting_provider => undefined,
        files_to_process => 1 + FilesToTransferNum,
        files_processed => 1 + FilesToTransferNum,
        files_replicated => FilesToTransferNum,
        bytes_replicated => FilesToTransferNum * ?BYTES_NUM,
        files_evicted => 0
    };
get_exp_transfer_stats(eviction, <<"dir">>, SrcNode, _DstNode, FilesToTransferNum) ->
    #{
        replication_status => skipped,
        eviction_status => completed,
        replicating_provider => undefined,
        evicting_provider => transfers_test_utils:provider_id(SrcNode),
        files_to_process => 1 + FilesToTransferNum,
        files_processed => 1 + FilesToTransferNum,
        files_replicated => 0,
        bytes_replicated => 0,
        files_evicted => FilesToTransferNum
    };
get_exp_transfer_stats(migration, <<"dir">>, SrcNode, DstNode, FilesToTransferNum) ->
    #{
        replication_status => completed,
        eviction_status => completed,
        replicating_provider => transfers_test_utils:provider_id(DstNode),
        evicting_provider => transfers_test_utils:provider_id(SrcNode),
        files_to_process => 2 * (1 + FilesToTransferNum),
        files_processed => 2 * (1 + FilesToTransferNum),
        files_replicated => FilesToTransferNum,
        bytes_replicated => FilesToTransferNum * ?BYTES_NUM,
        files_evicted => FilesToTransferNum
    }.


%% @private
validate_transfer_rest_call_result(#api_test_ctx{node = Node} = TestCtx, Result) ->
    {ok, _, Headers, Body} = ?assertMatch(
        {ok, ?HTTP_201_CREATED, #{<<"Location">> := _}, #{<<"transferId">> := _}},
        Result
    ),
    TransferId = maps:get(<<"transferId">>, Body),

    ExpLocation = list_to_binary(rpc:call(Node, oneprovider, get_rest_endpoint, [
        string:trim(filename:join([<<"/">>, <<"transfers">>, TransferId]), leading, [$/])
    ])),
    ?assertEqual(ExpLocation, maps:get(<<"Location">>, Headers)),

    validate_transfer_call_result(TransferId, TestCtx).


%% @private
validate_transfer_gs_call_result(TestCtx, Result) ->
    {ok, #{<<"transferId">> := TransferId}} = ?assertMatch({ok, _}, Result),
    validate_transfer_call_result(TransferId, TestCtx).


%% @private
validate_transfer_call_result(TransferId, #api_test_ctx{
    node = TestNode,
    client = ?USER(UserId),
    data = Data,
    env = #{exp_transfer := ExpTransferStats}
}) ->
    ExpTransfer = ExpTransferStats#{
        user_id => UserId,
        scheduling_provider => transfers_test_utils:provider_id(TestNode)
    },

    % Await transfer end and assert proper transfer stats
    transfers_test_utils:assert_transfer_state(TestNode, TransferId, ExpTransfer, ?ATTEMPTS),

    % If callback/url was supplied await feedback about transfer end
    % and assert that it came right after transfer ended - satisfy predicate:
    % CallTime - 10 < #transfer.finish_time <= CallTime
    case maps:is_key(<<"callback">>, Data) orelse maps:is_key(<<"url">>, Data) of
        true ->
            receive
                ?CALLBACK_CALL_TIME(TransferId, CallTime) ->
                    ct:pal("ASDASDASDASD"),
                    transfers_test_utils:assert_transfer_state(
                        TestNode, TransferId, ExpTransfer#{
                            finish_time => fun(FinishTime) ->
                                FinishTime > CallTime - 10 andalso FinishTime =< CallTime
                            end
                        },
                        1
                    )
            after 5000 ->
                ct:pal("Expected callback call never occured"),
                ?assert(false)
            end;
        false ->
            ok
    end.


%% @private
create_verify_transfer_env_fun(replication, Node, UserId, SrcProvider, DstProvider, Config) ->
    create_verify_replication_env_fun(Node, UserId, SrcProvider, DstProvider, Config);
create_verify_transfer_env_fun(eviction, Node, UserId, SrcProvider, DstProvider, Config) ->
    create_verify_eviction_env_fun(Node, UserId, SrcProvider, DstProvider, Config);
create_verify_transfer_env_fun(migration, Node, UserId, SrcProvider, DstProvider, Config) ->
    create_verify_migration_env_fun(Node, UserId, SrcProvider, DstProvider, Config).


%% @private
create_verify_replication_env_fun(Node, UserId, SrcProvider, DstProvider, Config) ->
    SessId = ?SESS_ID(UserId, Node, Config),

    fun
        (expected_failure, #api_test_ctx{env = #{files_to_transfer := FilesToTransfer, other_files := OtherFiles}}) ->
            Files = FilesToTransfer ++ OtherFiles,
            assert_distribution(Node, SessId, Files, [{SrcProvider, ?BYTES_NUM}]),
            true;
        (expected_success, #api_test_ctx{env = #{files_to_transfer := FilesToTransfer, other_files := OtherFiles}}) ->
            assert_distribution(Node, SessId, OtherFiles, [{SrcProvider, ?BYTES_NUM}]),
            assert_distribution(
                Node, SessId, FilesToTransfer,
                [{SrcProvider, ?BYTES_NUM}, {DstProvider, ?BYTES_NUM}]
            ),
            true
    end.


%% @private
create_verify_eviction_env_fun(Node, UserId, SrcProvider, DstProvider, Config) ->
    SessId = ?SESS_ID(UserId, Node, Config),

    fun
        (expected_failure, #api_test_ctx{env = #{files_to_transfer := FilesToTransfer, other_files := OtherFiles}}) ->
            assert_distribution(
                Node, SessId, OtherFiles ++ FilesToTransfer,
                [{SrcProvider, ?BYTES_NUM}, {DstProvider, ?BYTES_NUM}]
            ),
            true;
        (expected_success, #api_test_ctx{env = #{files_to_transfer := FilesToTransfer, other_files := OtherFiles}}) ->
            assert_distribution(
                Node, SessId, OtherFiles,
                [{SrcProvider, ?BYTES_NUM}, {DstProvider, ?BYTES_NUM}]
            ),
            assert_distribution(
                Node, SessId, FilesToTransfer,
                [{SrcProvider, 0}, {DstProvider, ?BYTES_NUM}]
            ),
            true
    end.


%% @private
create_verify_migration_env_fun(Node, UserId, SrcProvider, DstProvider, Config) ->
    SessId = ?SESS_ID(UserId, Node, Config),

    fun
        (expected_failure, #api_test_ctx{env = #{files_to_transfer := FilesToTransfer, other_files := OtherFiles}}) ->
            Files = FilesToTransfer ++ OtherFiles,
            assert_distribution(Node, SessId, Files, [{SrcProvider, ?BYTES_NUM}]),
            true;
        (expected_success, #api_test_ctx{env = #{files_to_transfer := FilesToTransfer, other_files := OtherFiles}}) ->
            assert_distribution(Node, SessId, OtherFiles, [{SrcProvider, ?BYTES_NUM}]),
            assert_distribution(
                Node, SessId, FilesToTransfer,
                [{SrcProvider, 0}, {DstProvider, ?BYTES_NUM}]
            ),
            true
    end.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        lists:foreach(fun(Worker) ->
            % TODO VFS-6251
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(1)), % TODO - change to 2 seconds
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off),
            test_utils:set_env(Worker, ?APP_NAME, public_block_size_treshold, 0),
            test_utils:set_env(Worker, ?APP_NAME, public_block_percent_treshold, 0)
        end, ?config(op_worker_nodes, NewConfig2)),
        application:start(ssl),
        hackney:start(),
        NewConfig3 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig2, "env_desc.json"),
            NewConfig2
        ),
        initializer:mock_auth_manager(NewConfig3, _CheckIfUserIsSupported = true),
        application:start(ssl),
        hackney:start(),
        start_http_server(),
        NewConfig3
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(Config) ->
    stop_http_server(),
    hackney:stop(),
    application:stop(ssl),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:teardown_storage(Config).


init_per_testcase(_Case, Config) ->
    initializer:mock_share_logic(Config),
    ct:timetrap({minutes, 10}),

    % TODO better loading modules from /test_distributed
    code:add_pathz("/home/cyfrinet/Desktop/develop/op-worker/test_distributed"),
    ct:pal("QWEASD:~p", [code:load_file(transfers_test_utils)]),

    % For http server to send msg about transfer callback call it is necessary
    % to register test process (every test is carried by different process)
    case whereis(?TEST_PROCESS) of
        undefined -> ok;
        _ -> unregister(?TEST_PROCESS)
    end,
    register(?TEST_PROCESS, self()),

    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% HTTP server used for transfer callback
%%%===================================================================


start_http_server() ->
    inets:start(),
    {ok, _} = inets:start(httpd, [
        {port, ?HTTP_SERVER_PORT},
        {server_name, "httpd_test"},
        {server_root, "/tmp"},
        {document_root, "/tmp"},
        {modules, [?MODULE]}
    ]).


stop_http_server() ->
    inets:stop().


do(#mod{method = "POST", request_uri = ?ENDED_TRANSFERS_PATH, entity_body = Body}) ->
    #{<<"transferId">> := TransferId} = json_utils:decode(Body),
    CallTime = time_utils:system_time_millis() div 1000,
    ?TEST_PROCESS ! ?CALLBACK_CALL_TIME(TransferId, CallTime),

    done.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
get_callback_url() ->
    {ok, IpAddressBin} = ip_utils:to_binary(initializer:local_ip_v4()),
    PortBin = integer_to_binary(?HTTP_SERVER_PORT),

    <<"http://", IpAddressBin/binary, ":" , PortBin/binary, ?ENDED_TRANSFERS_PATH>>.


%% @private
set_space_privileges(Nodes, SpaceId, UserId, Privileges) ->
    initializer:testmaster_mock_space_user_privileges(
        Nodes, SpaceId, UserId, Privileges
    ).


%% @private
create_file(Node, SessId, ParentPath) ->
    FilePath = filename:join([ParentPath, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(
        <<"file">>, Node, SessId, FilePath, 8#777
    ),
    api_test_utils:fill_file_with_dummy_data(Node, SessId, FileGuid, ?BYTES_NUM),
    FileGuid.


% Wait for metadata sync between providers and if requested transfer is `eviction`
% copy files to DstNode beforehand (eviction doesn't work if data replicas don't
% exist on other providers)
sync_files_between_nodes(eviction, SrcNode, SrcNodeSessId, DstNode, DstNodeSessId, Files) ->
    lists:foreach(fun(Guid) ->
        ExpContent = api_test_utils:read_file(SrcNode, SrcNodeSessId, Guid, ?BYTES_NUM),
        % Read file on DstNode to force rtransfer
        api_test_utils:wait_for_file_sync(DstNode, DstNodeSessId, Guid),
        ?assertMatch(
            ExpContent,
            api_test_utils:read_file(DstNode, DstNodeSessId, Guid, ?BYTES_NUM),
            ?ATTEMPTS
        )
    end, Files),
    % Wait until file_distribution contains entries for both nodes
    % Otherwise some of them could be omitted from eviction (if data
    % replicas don't exist on other providers it is skipped).
    assert_distribution(
        SrcNode, SrcNodeSessId, Files, [{SrcNode, ?BYTES_NUM}, {DstNode, ?BYTES_NUM}]
    );
sync_files_between_nodes(_TransferType, _SrcNode, _SrcNodeSessId, DstNode, DstNodeSessId, Files) ->
    lists:foreach(fun(Guid) ->
        api_test_utils:wait_for_file_sync(DstNode, DstNodeSessId, Guid)
    end, Files).


%% @private
create_view(TransferType, SrcNode, DstNode) ->
    XattrName = transfers_test_utils:random_job_name(?FUNCTION_NAME),
    ViewName = transfers_test_utils:random_view_name(?FUNCTION_NAME),
    MapFunction = transfers_test_utils:test_map_function(XattrName),
    case TransferType of
        replication ->
            transfers_test_utils:create_view(
                DstNode, ?SPACE_2, ViewName, MapFunction, [],
                [transfers_test_utils:provider_id(DstNode)]
            );
        eviction ->
            transfers_test_utils:create_view(
                SrcNode, ?SPACE_2, ViewName, MapFunction, [],
                [transfers_test_utils:provider_id(SrcNode)]
            );
        migration ->
            transfers_test_utils:create_view(
                DstNode, ?SPACE_2, ViewName, MapFunction, [],
                [transfers_test_utils:provider_id(SrcNode), transfers_test_utils:provider_id(DstNode)]
            )
    end,

    {ViewName, #xattr{name = XattrName, value = 1}}.


%% @private
assert_distribution(Node, SessId, Files, ExpSizePerProvider) ->
    ExpDistribution = lists:sort(lists:map(fun({Provider, ExpSize}) ->
        #{
            <<"blocks">> => case ExpSize of
                0 -> [];
                _ -> [[0, ExpSize]]
            end,
            <<"providerId">> => transfers_test_utils:provider_id(Provider),
            <<"totalBlocksSize">> => ExpSize
        }
    end, ExpSizePerProvider)),

    FetchDistributionFun = fun(Guid) ->
        {ok, Distribution} = lfm_proxy:get_file_distribution(Node, SessId, {guid, Guid}),
        lists:sort(Distribution)
    end,

    lists:foreach(fun(FileGuid) ->
        ?assertEqual(ExpDistribution, FetchDistributionFun(FileGuid), ?ATTEMPTS)
    end, Files).


%% @private
replace_placeholder_value(Key, Value, Data) ->
    case maps:get(Key, Data, undefined) of
        ?PLACEHOLDER ->
            Data#{Key => Value};
        _ ->
            Data
    end.
