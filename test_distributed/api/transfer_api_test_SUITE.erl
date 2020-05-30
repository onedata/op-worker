%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning transfer create basic API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_api_test_SUITE).
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

    create_view_replication/1,
    create_view_eviction/1,
    create_view_migration/1
]).

all() ->
    ?ALL([
        create_file_replication,
        create_file_eviction,

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

-define(BYTES_NUM, 20).

-define(HTTP_SERVER_PORT, 8080).
-define(TRANSFER_MONITORING_PATH, "/transfer_monitoring").

-define(TEST_PROCESS, test_process).
-define(CALLBACK_CALL_TIME(__TIME), {callback_call_time, __TIME}).


%%%===================================================================
%%% File transfer test functions
%%%===================================================================


create_file_replication(Config) ->
    [P2, _P1] = ?config(op_worker_nodes, Config),

    Callback = get_callback_url(),
    RequiredPrivs = [?SPACE_SCHEDULE_REPLICATION],

    TransferDataSpec = #data_spec{
        required = [
            <<"type">>, <<"dataSourceType">>,
            <<"replicatingProviderId">>,
            <<"fileId">>
        ],
        optional = [<<"callback">>],
        correct_values = #{
            <<"type">> => [<<"replication">>],
            <<"dataSourceType">> => [<<"file">>],
            <<"replicatingProviderId">> => [?GET_DOMAIN_BIN(P2)],
            <<"fileId">> => [?PLACEHOLDER],
            <<"callback">> => [Callback]
        },
        bad_values = ?TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES ++ [
            {<<"replicatingProviderId">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"replicatingProviderId">>)},
            {<<"replicatingProviderId">>, <<"NonExistingProvider">>, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"NonExistingProvider">>)},
            {<<"callback">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"callback">>)}
        ]
    },
    ReplicaDataSpec = #data_spec{
        required = [<<"provider_id">>],
        optional = [<<"url">>],
        correct_values = #{
            <<"provider_id">> => [?GET_DOMAIN_BIN(P2)],
            <<"url">> => [Callback]
        },
        bad_values = [
            {<<"provider_id">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"provider_id">>)}},
            {<<"provider_id">>, <<"NonExistingProvider">>, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"NonExistingProvider">>)},
            {<<"url">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"url">>)}}
        ]
    },

    create_file_transfer(Config, replication, TransferDataSpec, ReplicaDataSpec, RequiredPrivs).


create_file_eviction(Config) ->
    [_P2, P1] = ?config(op_worker_nodes, Config),

    Callback = get_callback_url(),
    RequiredPrivs = [?SPACE_SCHEDULE_EVICTION],

    TransferDataSpec = #data_spec{
        required = [
            <<"type">>, <<"dataSourceType">>,
            <<"evictingProviderId">>,
            <<"fileId">>
        ],
        optional = [<<"callback">>],
        correct_values = #{
            <<"type">> => [<<"eviction">>],
            <<"dataSourceType">> => [<<"file">>],
            <<"evictingProviderId">> => [?GET_DOMAIN_BIN(P1)],
            <<"fileId">> => [?PLACEHOLDER],
            <<"callback">> => [Callback]
        },
        bad_values = ?TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES ++ [
            {<<"evictingProviderId">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"evictingProviderId">>)},
            {<<"evictingProviderId">>, <<"NonExistingProvider">>, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"NonExistingProvider">>)},
            {<<"callback">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"callback">>)}
        ]
    },
    ReplicaDataSpec = #data_spec{
        required = [<<"provider_id">>],
        correct_values = #{
            <<"provider_id">> => [?GET_DOMAIN_BIN(P1)]
        },
        bad_values = [
            {<<"provider_id">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"provider_id">>)}},
            {<<"provider_id">>, <<"NonExistingProvider">>, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"NonExistingProvider">>)}
        ]
    },

    create_file_transfer(Config, eviction, TransferDataSpec, ReplicaDataSpec, RequiredPrivs).


create_file_transfer(Config, Type, TransferDataSpec, ReplicaDataSpec, RequiredPrivs) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),

    set_space_privileges(Providers, ?SPACE_2, ?USER_IN_SPACE_2, privileges:space_admin() -- RequiredPrivs),
    set_space_privileges(Providers, ?SPACE_2, ?USER_IN_BOTH_SPACES, RequiredPrivs),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_TRANSFER_SCENARIOS(Config),
            setup_fun = create_setup_file_replication_env(Type, P1, P2, ?USER_IN_SPACE_2, Config),
            verify_fun = create_verify_transfer_env(Type, P2, ?USER_IN_SPACE_2, P1, P2, Config),
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Transfer (~p) file using gs transfer api", [Type]),
                    type = gs,
                    prepare_args_fun = create_prepare_transfer_create_instance_gs_args_fun(private),
                    validate_result_fun = fun validate_transfer_gs_call_result/2
                }
            ],
            data_spec = add_file_id_bad_values(TransferDataSpec, ?SPACE_2, undefined)
        },

        %% TEST DEPRECATED REPLICAS ENDPOINTS

        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_TRANSFER_SCENARIOS(Config),
            setup_fun = create_setup_file_replication_env(Type, P1, P2, ?USER_IN_SPACE_2, Config),
            verify_fun = create_verify_transfer_env(Type, P2, ?USER_IN_SPACE_2, P1, P2, Config),
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
            data_spec = api_test_utils:add_bad_file_id_and_path_error_values(
                ReplicaDataSpec, ?SPACE_2, undefined
            )
        }
    ])).


create_setup_file_replication_env(TransferType, SrcNode, DstNode, UserId, Config) ->
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
                fill_file_with_dummy_data(SrcNode, SessId1, RootFileGuid, ?BYTES_NUM),
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


%%%===================================================================
%%% View transfer test functions
%%%===================================================================


create_view_replication(Config) ->
    [P2, _P1] = ?config(op_worker_nodes, Config),

    Callback = get_callback_url(),
    RequiredPrivs = [?SPACE_SCHEDULE_REPLICATION, ?SPACE_QUERY_VIEWS],

    TransferDataSpec = #data_spec{
        required = [
            <<"type">>, <<"dataSourceType">>,
            <<"replicatingProviderId">>,
            <<"spaceId">>, <<"viewName">>
        ],
        optional = [<<"callback">>],
        correct_values = #{
            <<"type">> => [<<"replication">>],
            <<"dataSourceType">> => [<<"view">>],
            <<"replicatingProviderId">> => [?GET_DOMAIN_BIN(P2)],
            <<"spaceId">> => [?SPACE_2],
            <<"viewName">> => [?PLACEHOLDER],
            <<"callback">> => [Callback]
        },
        bad_values = ?TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES ++ [
            {<<"replicatingProviderId">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"replicatingProviderId">>)},
            {<<"replicatingProviderId">>, <<"NonExistingProvider">>, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"NonExistingProvider">>)},
            {<<"callback">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"callback">>)}
        ]
    },

    ReplicaDataSpec = #data_spec{
        required = [<<"provider_id">>, <<"space_id">>],
        optional = [<<"url">>],
        correct_values = #{
            <<"provider_id">> => [?GET_DOMAIN_BIN(P2)],
            <<"space_id">> => [?SPACE_2],
            <<"url">> => [Callback]
        },
        bad_values = [
            {<<"provider_id">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"provider_id">>)}},
            {<<"provider_id">>, <<"NonExistingProvider">>, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"NonExistingProvider">>)},
            {<<"url">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"url">>)}}
        ]
    },

    create_view_transfer(Config, replication, TransferDataSpec, ReplicaDataSpec, RequiredPrivs).


create_view_eviction(Config) ->
    [_P2, P1] = ?config(op_worker_nodes, Config),

    Callback = get_callback_url(),
    RequiredPrivs = [?SPACE_SCHEDULE_EVICTION, ?SPACE_QUERY_VIEWS],

    TransferDataSpec = #data_spec{
        required = [
            <<"type">>, <<"dataSourceType">>,
            <<"evictingProviderId">>,
            <<"spaceId">>, <<"viewName">>
        ],
        optional = [<<"callback">>],
        correct_values = #{
            <<"type">> => [<<"eviction">>],
            <<"dataSourceType">> => [<<"view">>],
            <<"evictingProviderId">> => [?GET_DOMAIN_BIN(P1)],
            <<"spaceId">> => [?SPACE_2],
            <<"viewName">> => [?PLACEHOLDER],
            <<"callback">> => [Callback]
        },
        bad_values = ?TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES ++ [
            {<<"evictingProviderId">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"evictingProviderId">>)},
            {<<"evictingProviderId">>, <<"NonExistingProvider">>, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"NonExistingProvider">>)},
            {<<"callback">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"callback">>)}
        ]
    },

    ReplicaDataSpec = #data_spec{
        required = [<<"provider_id">>, <<"space_id">>],
        correct_values = #{
            <<"provider_id">> => [?GET_DOMAIN_BIN(P1)],
            <<"space_id">> => [?SPACE_2]
        },
        bad_values = [
            {<<"provider_id">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"provider_id">>)}},
            {<<"provider_id">>, <<"NonExistingProvider">>, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"NonExistingProvider">>)}
        ]
    },

    create_view_transfer(Config, eviction, TransferDataSpec, ReplicaDataSpec, RequiredPrivs).


create_view_migration(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),

    Callback = get_callback_url(),
    RequiredPrivs = [
        ?SPACE_SCHEDULE_REPLICATION, ?SPACE_SCHEDULE_EVICTION,
        ?SPACE_QUERY_VIEWS
    ],

    TransferDataSpec = #data_spec{
        required = [
            <<"type">>, <<"dataSourceType">>,
            <<"evictingProviderId">>,
            <<"replicatingProviderId">>,
            <<"spaceId">>, <<"viewName">>
        ],
        optional = [<<"callback">>],
        correct_values = #{
            <<"type">> => [<<"migration">>],
            <<"dataSourceType">> => [<<"view">>],
            <<"replicatingProviderId">> => [?GET_DOMAIN_BIN(P2)],
            <<"evictingProviderId">> => [?GET_DOMAIN_BIN(P1)],
            <<"spaceId">> => [?SPACE_2],
            <<"viewName">> => [?PLACEHOLDER],
            <<"callback">> => [Callback]
        },
        bad_values = ?TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES ++ [
            {<<"replicatingProviderId">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"replicatingProviderId">>)},
            {<<"replicatingProviderId">>, <<"NonExistingProvider">>, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"NonExistingProvider">>)},
            {<<"evictingProviderId">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"evictingProviderId">>)},
            {<<"evictingProviderId">>, <<"NonExistingProvider">>, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"NonExistingProvider">>)},
            {<<"callback">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"callback">>)}
        ]
    },

    ReplicaDataSpec = #data_spec{
        required = [<<"provider_id">>, <<"migration_provider_id">>, <<"space_id">>],
        correct_values = #{
            <<"migration_provider_id">> => [?GET_DOMAIN_BIN(P2)],
            <<"provider_id">> => [?GET_DOMAIN_BIN(P1)],
            <<"space_id">> => [?SPACE_2]
        },
        bad_values = [
            {<<"migration_provider_id">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"migration_provider_id">>)}},
            {<<"migration_provider_id">>, <<"NonExistingProvider">>, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"NonExistingProvider">>)},
            {<<"provider_id">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"provider_id">>)}},
            {<<"provider_id">>, <<"NonExistingProvider">>, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"NonExistingProvider">>)}
        ]
    },

    create_view_transfer(Config, migration, TransferDataSpec, ReplicaDataSpec, RequiredPrivs).


%% @private
create_view_transfer(Config, Type, TransferDataSpec, ReplicaDataSpec, RequiredPrivs) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),

    set_space_privileges(Providers, ?SPACE_2, ?USER_IN_SPACE_2, privileges:space_admin() -- RequiredPrivs),
    set_space_privileges(Providers, ?SPACE_2, ?USER_IN_BOTH_SPACES, RequiredPrivs),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_TRANSFER_SCENARIOS(Config),
            setup_fun = create_setup_view_transfer_env(Type, P1, P2, ?USER_IN_SPACE_2, Config),
            verify_fun = create_verify_transfer_env(Type, P2, ?USER_IN_SPACE_2, P1, P2, Config),
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Transfer (~p) view using gs transfer api", [Type]),
                    type = gs,
                    prepare_args_fun = create_prepare_transfer_create_instance_gs_args_fun(private),
                    validate_result_fun = fun validate_transfer_gs_call_result/2
                }
            ],
            data_spec = TransferDataSpec
        },

        %% TEST DEPRECATED REPLICAS ENDPOINTS

        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_TRANSFER_SCENARIOS(Config),
            setup_fun = create_setup_view_transfer_env(Type, P1, P2, ?USER_IN_SPACE_2, Config),
            verify_fun = create_verify_transfer_env(Type, P2, ?USER_IN_SPACE_2, P1, P2, Config),
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
            data_spec = ReplicaDataSpec
        }
    ])).


%% @private
create_setup_view_transfer_env(TransferType, SrcNode, DstNode, UserId, Config) ->
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


%%%===================================================================
%%% Common transfer helper functions
%%%===================================================================


%% @private
-spec add_file_id_bad_values(undefined | data_spec(), od_space:id(), undefined | od_share:id()) ->
    data_spec().
add_file_id_bad_values(DataSpec, SpaceId, ShareId) ->
    {ok, DummyObjectId} = file_id:guid_to_objectid(<<"DummyGuid">>),

    NonExistentFileAndSpaceGuid = file_id:pack_share_guid(<<"InvalidUuid">>, ?NOT_SUPPORTED_SPACE_ID, ShareId),
    {ok, NonExistentFileAndSpaceObjectId} = file_id:guid_to_objectid(NonExistentFileAndSpaceGuid),

    NonExistentFileGuid = file_id:pack_share_guid(<<"InvalidUuid">>, SpaceId, ShareId),
    {ok, NonExistentFileObjectId} = file_id:guid_to_objectid(NonExistentFileGuid),

    BadFileIdValues = [
        {<<"fileId">>, <<"InvalidObjectId">>, ?ERROR_BAD_VALUE_IDENTIFIER(<<"fileId">>)},
        {<<"fileId">>, DummyObjectId, ?ERROR_BAD_VALUE_IDENTIFIER(<<"fileId">>)},
        {<<"fileId">>, NonExistentFileAndSpaceObjectId, ?ERROR_FORBIDDEN},
        {<<"fileId">>, NonExistentFileObjectId, ?ERROR_POSIX(?ENOENT)}
    ],
    case DataSpec of
        undefined ->
            #data_spec{bad_values = BadFileIdValues};
        #data_spec{bad_values = BadValues} ->
            DataSpec#data_spec{bad_values = BadFileIdValues ++ BadValues}
    end.


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
        {InvalidId, Data2} = case maps:take(bad_id, ensure_defined(Data0, #{})) of
            {BadId, Data1} -> {BadId, Data1};
            error -> {undefined, Data0}
        end,
        RestPath = case Env of
            #{root_file_path := FilePath} when Scenario =:= rest_with_file_path  ->
                <<"replicas", (ensure_defined(InvalidId, FilePath))/binary>>;
            #{root_file_cdmi_id := FileObjectId} ->
                <<"replicas-id/", (ensure_defined(InvalidId, FileObjectId))/binary>>;
            #{view_name := ViewName} ->
                <<"replicas-view/", (ensure_defined(InvalidId, ViewName))/binary>>
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
        {GriId, Data3} = case maps:take(bad_id, ensure_defined(Data0, #{})) of
            {BadId, Data1} when map_size(Data1) == 0 -> {BadId, undefined};
            {BadId, Data1} -> {BadId, Data1};
            error -> {ValidId, Data0}
        end,
        #gs_args{
            operation = Operation,
            gri = #gri{type = op_replica, id = GriId, aspect = Aspect, scope = Scope},
            data = Data3
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
                ?CALLBACK_CALL_TIME(CallTime) ->
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
create_verify_transfer_env(replication, Node, UserId, SrcProvider, DstProvider, Config) ->
    create_verify_replication_env(Node, UserId, SrcProvider, DstProvider, Config);
create_verify_transfer_env(eviction, Node, UserId, SrcProvider, DstProvider, Config) ->
    create_verify_eviction_env(Node, UserId, SrcProvider, DstProvider, Config);
create_verify_transfer_env(migration, Node, UserId, SrcProvider, DstProvider, Config) ->
    create_verify_migration_env(Node, UserId, SrcProvider, DstProvider, Config).


%% @private
create_verify_replication_env(Node, UserId, SrcProvider, DstProvider, Config) ->
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
create_verify_eviction_env(Node, UserId, SrcProvider, DstProvider, Config) ->
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
create_verify_migration_env(Node, UserId, SrcProvider, DstProvider, Config) ->
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

    % TODO better loading modules from /test_distributes
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


do(#mod{method = "GET", request_uri = ?TRANSFER_MONITORING_PATH}) ->
    CallTime = time_utils:system_time_millis() div 1000,
    ?TEST_PROCESS ! ?CALLBACK_CALL_TIME(CallTime),

    done.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
get_callback_url() ->
    {ok, IpAddressBin} = ip_utils:to_binary(initializer:local_ip_v4()),
    PortBin = integer_to_binary(?HTTP_SERVER_PORT),

    <<"http://", IpAddressBin/binary, ":" , PortBin/binary, ?TRANSFER_MONITORING_PATH>>.


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
    fill_file_with_dummy_data(Node, SessId, FileGuid, ?BYTES_NUM),
    FileGuid.


%% @private
fill_file_with_dummy_data(Node, SessId, FileGuid, BytesNum) ->
    Content = crypto:strong_rand_bytes(BytesNum),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, {guid, FileGuid}, write)),
    ?assertMatch({ok, _}, lfm_proxy:write(Node, Handle, 0, Content)),
    ?assertMatch(ok, lfm_proxy:close(Node, Handle)),
    Content.


% Wait for metadata sync between providers and if requested transfer is `eviction`
% copy files to DstNode beforehand (eviction doesn't work if data replicas don't
% exist on other providers)
sync_files_between_nodes(eviction, SrcNode, SrcNodeSessId, DstNode, DstNodeSessId, Files) ->
    lists:foreach(fun(Guid) ->
        ExpContent = read_file(SrcNode, SrcNodeSessId, Guid),
        % Read file on DstNode to force rtransfer
        api_test_utils:wait_for_file_sync(DstNode, DstNodeSessId, Guid),
        ?assertMatch(ExpContent, read_file(DstNode, DstNodeSessId, Guid), ?ATTEMPTS)
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
read_file(Node, SessId, FileGuid) ->
    {ok, ReadHandle} = lfm_proxy:open(Node, SessId, {guid, FileGuid}, read),
    {ok, Content} = lfm_proxy:read(Node, ReadHandle, 0, ?BYTES_NUM),
    ok = lfm_proxy:close(Node, ReadHandle),
    Content.


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


%% @private
ensure_defined(undefined, DefaultValue) -> DefaultValue;
ensure_defined(Value, _DefaultValue) -> Value.
