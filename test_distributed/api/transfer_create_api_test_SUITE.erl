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
-include("transfer_api_test_utils.hrl").
-include("transfers_test_mechanism.hrl").
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

-define(CALLBACK_TRANSFER_ERRORS, [{<<"callback">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"callback">>)}]).

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

    % Shared file will be used to assert that shared file transfer will be forbidden
    % (it will be added to '#data_spec.bad_values')
    FileGuid = transfer_api_test_utils:create_file(P1, SessIdP1, filename:join(["/", ?SPACE_2])),
    {ok, ShareId} = lfm_proxy:create_share(P1, SessIdP1, {guid, FileGuid}, <<"share">>),
    file_test_utils:await_sync(P2, FileGuid),

    RequiredPrivs = create_file_transfer_required_privs(Type),
    set_space_privileges(Providers, ?SPACE_2, ?USER_IN_SPACE_2, privileges:space_admin() -- RequiredPrivs),
    set_space_privileges(Providers, ?SPACE_2, ?USER_IN_BOTH_SPACES, RequiredPrivs),

    MemRef = api_test_memory:init(),
    SetupFun = transfer_api_test_utils:build_create_file_transfer_setup_fun(
        Type, MemRef, P1, P2, ?USER_IN_SPACE_2, Config
    ),
    VerifyFun = transfer_api_test_utils:build_create_transfer_verify_fun(
        Type, MemRef, P2, ?USER_IN_SPACE_2, P1, P2, Config
    ),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_TRANSFER_SCENARIOS(Config),
            setup_fun = SetupFun,
            verify_fun = VerifyFun,
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Transfer (~p) view using /transfers rest endpoint", [Type]),
                    type = rest,
                    prepare_args_fun = build_create_transfer_prepare_rest_args_fun(MemRef),
                    validate_result_fun = build_create_transfer_validate_rest_call_result_fun(MemRef)
                },
                #scenario_template{
                    name = str_utils:format("Transfer (~p) file using gs transfer api", [Type]),
                    type = gs,
                    prepare_args_fun = build_create_transfer_prepare_gs_args_fun(MemRef, private),
                    validate_result_fun = build_create_transfer_validate_gs_call_result_fun(MemRef)
                }
            ],
            data_spec = api_test_utils:add_cdmi_id_errors_for_operations_not_available_in_share_mode(
                FileGuid, ?SPACE_2, ShareId,
                build_op_transfer_spec(Type, <<"file">>, P1, P2)
            )
        },

        %% TEST DEPRECATED REPLICAS ENDPOINTS

        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_TRANSFER_SCENARIOS(Config),
            setup_fun = SetupFun,
            verify_fun = VerifyFun,
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Transfer (~p) file using /replicas/ rest endpoint", [Type]),
                    type = rest_with_file_path,
                    prepare_args_fun = build_replica_prepare_rest_args_fun(MemRef, Type),
                    validate_result_fun = build_create_transfer_validate_rest_call_result_fun(MemRef)
                },
                #scenario_template{
                    name = str_utils:format("Transfer (~p) file using /replicas-id/ rest endpoint", [Type]),
                    type = rest,
                    prepare_args_fun = build_replica_prepare_rest_args_fun(MemRef, Type),
                    validate_result_fun = build_create_transfer_validate_rest_call_result_fun(MemRef)
                },
                #scenario_template{
                    name = str_utils:format("Transfer (~p) file using op_replica gs api", [Type]),
                    type = gs,
                    prepare_args_fun = build_replica_prepare_gs_args_fun(MemRef, Type, private),
                    validate_result_fun = build_create_transfer_validate_gs_call_result_fun(MemRef)
                }
            ],
            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                FileGuid, ShareId, build_op_replica_spec(Type, <<"file">>, P1, P2)
            )
        }
    ])).


%% @private
create_file_transfer_required_privs(replication) ->
    [?SPACE_SCHEDULE_REPLICATION];
create_file_transfer_required_privs(eviction) ->
    [?SPACE_SCHEDULE_EVICTION];
create_file_transfer_required_privs(migration) ->
    [?SPACE_SCHEDULE_REPLICATION, ?SPACE_SCHEDULE_EVICTION].


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

    RequiredPrivs = create_view_transfer_required_privs(Type),
    set_space_privileges(Providers, ?SPACE_2, ?USER_IN_SPACE_2, privileges:space_admin() -- RequiredPrivs),
    set_space_privileges(Providers, ?SPACE_2, ?USER_IN_BOTH_SPACES, RequiredPrivs),

    MemRef = api_test_memory:init(),
    SetupFun = transfer_api_test_utils:build_create_view_transfer_setup_fun(
        Type, MemRef, P1, P2, ?USER_IN_SPACE_2, Config
    ),
    VerifyFun = transfer_api_test_utils:build_create_transfer_verify_fun(
        Type, MemRef, P2, ?USER_IN_SPACE_2, P1, P2, Config
    ),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_TRANSFER_SCENARIOS(Config),
            setup_fun = SetupFun,
            verify_fun = VerifyFun,
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Transfer (~p) view using /transfers rest endpoint", [Type]),
                    type = rest,
                    prepare_args_fun = build_create_transfer_prepare_rest_args_fun(MemRef),
                    validate_result_fun = build_create_transfer_validate_rest_call_result_fun(MemRef)
                },
                #scenario_template{
                    name = str_utils:format("Transfer (~p) view using gs transfer gs api", [Type]),
                    type = gs,
                    prepare_args_fun = build_create_transfer_prepare_gs_args_fun(MemRef, private),
                    validate_result_fun = build_create_transfer_validate_gs_call_result_fun(MemRef)
                }
            ],
            data_spec = build_op_transfer_spec(Type, <<"view">>, P1, P2)
        },

        %% TEST DEPRECATED REPLICAS ENDPOINTS

        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_TRANSFER_SCENARIOS(Config),
            setup_fun = SetupFun,
            verify_fun = VerifyFun,
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Transfer (~p) view using /replicas-view/ rest endpoint", [Type]),
                    type = rest,
                    prepare_args_fun = build_replica_prepare_rest_args_fun(MemRef, Type),
                    validate_result_fun = build_create_transfer_validate_rest_call_result_fun(MemRef)
                },
                #scenario_template{
                    name = str_utils:format("Transfer (~p) view using op_replica gs api", [Type]),
                    type = gs,
                    prepare_args_fun = build_replica_prepare_gs_args_fun(MemRef, Type, private),
                    validate_result_fun = build_create_transfer_validate_gs_call_result_fun(MemRef)
                }
            ],
            data_spec = build_op_replica_spec(Type, <<"view">>, P1, P2)
        }
    ])).


%% @private
create_view_transfer_required_privs(replication) ->
    [?SPACE_SCHEDULE_REPLICATION, ?SPACE_QUERY_VIEWS];
create_view_transfer_required_privs(eviction) ->
    [?SPACE_SCHEDULE_EVICTION, ?SPACE_QUERY_VIEWS];
create_view_transfer_required_privs(migration) ->
    [?SPACE_SCHEDULE_REPLICATION, ?SPACE_SCHEDULE_EVICTION, ?SPACE_QUERY_VIEWS].


%%%===================================================================
%%% Common transfer helper functions
%%%===================================================================



%% @private
build_op_replica_spec(replication, DataSourceType, _SrcNode, DstNode) ->
    {Required, Optional, CorrectValues, BadValues} = get_data_source_dependent_data_spec_aspects(
        op_replica, DataSourceType
    ),
    #data_spec{
        required = [<<"provider_id">> | Required],
        optional = [<<"url">> | Optional],
        correct_values = CorrectValues#{
            <<"provider_id">> => [?GET_DOMAIN_BIN(DstNode)],
            <<"url">> => [get_callback_url()]
        },
        bad_values = lists:flatten([
            ?PROVIDER_ID_REPLICA_ERRORS(<<"provider_id">>),
            [{<<"url">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"url">>)}}],
            BadValues
        ])
    };
build_op_replica_spec(eviction, DataSourceType, SrcNode, _DstNode) ->
    {Required, Optional, CorrectValues, BadValues} = get_data_source_dependent_data_spec_aspects(
        op_replica, DataSourceType
    ),
    #data_spec{
        required = [<<"provider_id">> | Required],
        optional =  Optional,
        correct_values = CorrectValues#{
            <<"provider_id">> => [?GET_DOMAIN_BIN(SrcNode)]
        },
        bad_values = lists:flatten([
            ?PROVIDER_ID_REPLICA_ERRORS(<<"provider_id">>),
            BadValues
        ])
    };
build_op_replica_spec(migration, DataSourceType, SrcNode, DstNode) ->
    {Required, Optional, CorrectValues, BadValues} = get_data_source_dependent_data_spec_aspects(
        op_replica, DataSourceType
    ),
    #data_spec{
        required = [<<"provider_id">>, <<"migration_provider_id">> | Required],
        optional =  Optional,
        correct_values = CorrectValues#{
            <<"provider_id">> => [?GET_DOMAIN_BIN(SrcNode)],
            <<"migration_provider_id">> => [?GET_DOMAIN_BIN(DstNode)]
        },
        bad_values = lists:flatten([
            ?PROVIDER_ID_REPLICA_ERRORS(<<"provider_id">>),
            ?PROVIDER_ID_REPLICA_ERRORS(<<"migration_provider_id">>),
            BadValues
        ])
    }.


%% @private
build_op_transfer_spec(replication, DataSourceType, _SrcNode, DstNode) ->
    {Required, Optional, CorrectValues, BadValues} = get_data_source_dependent_data_spec_aspects(
        op_transfer, DataSourceType
    ),
    #data_spec{
        required = [
            <<"type">>, <<"dataSourceType">>, <<"replicatingProviderId">>
            | Required
        ],
        optional = [<<"callback">> | Optional],
        correct_values = CorrectValues#{
            <<"type">> => [<<"replication">>],
            <<"dataSourceType">> => [DataSourceType],
            <<"replicatingProviderId">> => [?GET_DOMAIN_BIN(DstNode)],
            <<"callback">> => [get_callback_url()]
        },
        bad_values = lists:flatten([
            ?TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES,
            ?PROVIDER_ID_TRANSFER_ERRORS(<<"replicatingProviderId">>),
            ?CALLBACK_TRANSFER_ERRORS,
            BadValues
        ])
    };
build_op_transfer_spec(eviction, DataSourceType, SrcNode, _DstNode) ->
    {Required, Optional, CorrectValues, BadValues} = get_data_source_dependent_data_spec_aspects(
        op_transfer, DataSourceType
    ),
    #data_spec{
        required = [
            <<"type">>, <<"dataSourceType">>, <<"evictingProviderId">>
            | Required
        ],
        optional = [<<"callback">> | Optional],
        correct_values = CorrectValues#{
            <<"type">> => [<<"eviction">>],
            <<"dataSourceType">> => [DataSourceType],
            <<"evictingProviderId">> => [?GET_DOMAIN_BIN(SrcNode)],
            <<"callback">> => [get_callback_url()]
        },
        bad_values = lists:flatten([
            ?TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES,
            ?PROVIDER_ID_TRANSFER_ERRORS(<<"evictingProviderId">>),
            ?CALLBACK_TRANSFER_ERRORS,
            BadValues
        ])
    };
build_op_transfer_spec(migration, DataSourceType, SrcNode, DstNode) ->
    {Required, Optional, CorrectValues, BadValues} = get_data_source_dependent_data_spec_aspects(
        op_transfer, DataSourceType
    ),
    #data_spec{
        required = [
            <<"type">>, <<"dataSourceType">>,
            <<"replicatingProviderId">>, <<"evictingProviderId">>
            | Required
        ],
        optional = [<<"callback">> | Optional],
        correct_values = CorrectValues#{
            <<"type">> => [<<"migration">>],
            <<"dataSourceType">> => [DataSourceType],
            <<"replicatingProviderId">> => [?GET_DOMAIN_BIN(DstNode)],
            <<"evictingProviderId">> => [?GET_DOMAIN_BIN(SrcNode)],
            <<"callback">> => [get_callback_url()]
        },
        bad_values = lists:flatten([
            ?TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES,
            ?PROVIDER_ID_TRANSFER_ERRORS(<<"replicatingProviderId">>),
            ?PROVIDER_ID_TRANSFER_ERRORS(<<"evictingProviderId">>),
            ?CALLBACK_TRANSFER_ERRORS,
            BadValues
        ])
    }.


%% @private
-spec get_data_source_dependent_data_spec_aspects(
    Middleware :: op_transfer | op_replica, DataSourceType :: binary()
) -> {
    RequiredParams :: [binary()],
    OptionalParams :: [binary()],
    CorrectValues :: #{Key :: binary() => Values :: [term()]},
    BadValues :: [{Key :: binary(), Value :: term(), errors:error()}]
}.
get_data_source_dependent_data_spec_aspects(op_transfer, <<"file">>) ->
    {[<<"fileId">>], [], #{<<"fileId">> => [?PLACEHOLDER]}, []};
get_data_source_dependent_data_spec_aspects(op_transfer, <<"view">>) ->
    RequiredParams = [<<"spaceId">>, <<"viewName">>],
    OptionalParams = [<<"queryViewParams">>],
    CorrectValues = #{
        <<"spaceId">> => [?SPACE_2],
        <<"viewName">> => [?PLACEHOLDER],
        % Below value will not affect test (view has only up to 5 files) and checks only
        % that server accepts it - view transfers with query options should have distinct suite
        <<"queryViewParams">> => [#{<<"limit">> => 100}]
    },
    BadValues = [
        {<<"spaceId">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"spaceId">>)},
        {<<"spaceId">>, <<"NonExistingSpace">>, ?ERROR_FORBIDDEN},

        {<<"viewName">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"viewName">>)},
        {<<"viewName">>, <<"NonExistingView">>, {error_fun, fun(#api_test_ctx{node = Node}) ->
            ?ERROR_VIEW_NOT_EXISTS_ON(?GET_DOMAIN_BIN(Node))
        end}},

        {<<"queryViewParams">>, #{<<"bbox">> => 123}, ?ERROR_BAD_DATA(<<"bbox">>)},
        {<<"queryViewParams">>, #{<<"descending">> => <<"ascending">>}, ?ERROR_BAD_VALUE_BOOLEAN(<<"descending">>)},
        {<<"queryViewParams">>, #{<<"limit">> => <<"inf">>}, ?ERROR_BAD_VALUE_INTEGER(<<"limit">>)},
        {<<"queryViewParams">>, #{<<"limit">> => 0}, ?ERROR_BAD_VALUE_TOO_LOW(<<"limit">>, 1)},
        {<<"queryViewParams">>, #{<<"stale">> => <<"fresh">>},
            ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"stale">>, [<<"ok">>, <<"update_after">>, <<"false">>])}
    ],
    {RequiredParams, OptionalParams, CorrectValues, BadValues};
get_data_source_dependent_data_spec_aspects(op_replica, <<"file">>) ->
    {[], [], #{}, []};
get_data_source_dependent_data_spec_aspects(op_replica, <<"view">>) ->
    RequiredParams = [<<"space_id">>],
    OptionalParams = [<<"limit">>],
    CorrectValues = #{
        <<"space_id">> => [?SPACE_2],
        % Below value will not affect test (view has only up to 5 files) and checks only
        % that server accepts it - view transfers with query options should have distinct suite
        <<"limit">> => [100]
    },
    BadValues = [
        {<<"space_id">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"space_id">>)}},
        {<<"space_id">>, <<"NonExistingSpace">>, ?ERROR_FORBIDDEN},

        {bad_id, <<"NonExistingView">>, {error_fun, fun(#api_test_ctx{node = Node}) ->
            ?ERROR_VIEW_NOT_EXISTS_ON(?GET_DOMAIN_BIN(Node))
        end}},

        {<<"bbox">>, <<"bbox">>, {rest, ?ERROR_BAD_DATA(<<"bbox">>)}},
        {<<"bbox">>, 123, {gs, ?ERROR_BAD_VALUE_BINARY(<<"bbox">>)}},
        {<<"descending">>, <<"ascending">>, ?ERROR_BAD_VALUE_BOOLEAN(<<"descending">>)},
        {<<"limit">>, <<"inf">>, ?ERROR_BAD_VALUE_INTEGER(<<"limit">>)},
        {<<"limit">>, 0, ?ERROR_BAD_VALUE_TOO_LOW(<<"limit">>, 1)},
        {<<"stale">>, <<"fresh">>,
            ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"stale">>, [<<"ok">>, <<"update_after">>, <<"false">>])}
    ],
    {RequiredParams, OptionalParams, CorrectValues, BadValues}.


%% @private
build_create_transfer_prepare_rest_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}) ->
        TransferDetails = api_test_memory:get(MemRef, transfer_details),

        #rest_args{
            method = post,
            path = <<"transfers">>,
            headers = #{<<"content-type">> => <<"application/json">>},
            body = json_utils:encode(substitute_transfer_data_source(TransferDetails, Data))
        }
    end.


%% @private
build_create_transfer_prepare_gs_args_fun(MemRef, Scope) ->
    fun(#api_test_ctx{data = Data}) ->
        TransferDetails = api_test_memory:get(MemRef, transfer_details),

        #gs_args{
            operation = create,
            gri = #gri{type = op_transfer, aspect = instance, scope = Scope},
            data = substitute_transfer_data_source(TransferDetails, Data)
        }
    end.


%% @private
substitute_transfer_data_source(Env, Data) ->
    case Env of
        #{root_file_cdmi_id := FileObjectId} ->
            replace_placeholder_value(<<"fileId">>, FileObjectId, Data);
        #{view_name := ViewName} ->
            replace_placeholder_value(<<"viewName">>, ViewName, Data)
    end.


%% @private
build_replica_prepare_rest_args_fun(MemRef, Type) ->
    Method = case Type of
        replication -> post;
        _ -> delete
    end,

    fun(#api_test_ctx{scenario_type = Scenario, node = Node, data = Data0}) ->
        case should_skip_replica_testcase(Type, Data0) of
            true ->
                skip;
            false ->
                ProviderId = transfers_test_utils:provider_id(Node),
                TransferDetails = api_test_memory:get(MemRef, transfer_details),

                Data1 = utils:ensure_defined(Data0, #{}),
                {InvalidId, Data2} = api_test_utils:maybe_substitute_bad_id(undefined, Data1),
                RestPath = case TransferDetails of
                    #{root_file_path := FilePath} when Scenario =:= rest_with_file_path  ->
                        <<"replicas", (utils:ensure_defined(InvalidId, FilePath))/binary>>;
                    #{root_file_cdmi_id := FileObjectId} ->
                        <<"replicas-id/", (utils:ensure_defined(InvalidId, FileObjectId))/binary>>;
                    #{view_name := ViewName} ->
                        <<"replicas-view/", (utils:ensure_defined(InvalidId, ViewName))/binary>>
                end,
                {Body, Data4} = case maps:take(<<"url">>, Data2) of
                    {Url, Data3} ->
                        {json_utils:encode(#{<<"url">> => Url}), Data3};
                    error ->
                        {<<>>, Data2}
                end,
                % In case of op_replica api 'provider_id' is optional - if it is not present
                % then by default provider receiving request will substitute its id
                Data5 = case maps:get(<<"provider_id">>, Data4) == ProviderId of
                    true -> maps:remove(<<"provider_id">>, Data4);
                    false -> Data4
                end,

                #rest_args{
                    method = Method,
                    path = http_utils:append_url_parameters(RestPath, Data5),
                    headers = #{<<"content-type">> => <<"application/json">>},
                    body = Body
                }
        end
    end.


%% @private
build_replica_prepare_gs_args_fun(MemRef, Type, Scope) ->
    Operation = case Type of
        replication -> create;
        _ -> delete
    end,

    fun(#api_test_ctx{node = Node, data = Data0}) ->
        case should_skip_replica_testcase(Type, Data0) of
            true ->
                skip;
            false ->
                ProviderId = transfers_test_utils:provider_id(Node),
                TransferDetails = api_test_memory:get(MemRef, transfer_details),

                {ValidId, Aspect} = case TransferDetails of
                    #{root_file_guid := FileGuid} ->
                        {FileGuid, instance};
                    #{view_name := ViewName} when Operation == create ->
                        {ViewName, replicate_by_view};
                    #{view_name := ViewName} when Operation == delete ->
                        {ViewName, evict_by_view}
                end,
                {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(ValidId, Data0),

                % In case of op_replica api if 'provider_id' is optional - if it is not present
                % then by default provider receiving request will substitute its id
                Data2 = case maps:get(<<"provider_id">>, Data1, undefined) == ProviderId of
                    true -> maps:remove(<<"provider_id">>, Data1);
                    false -> Data1
                end,
                #gs_args{
                    operation = Operation,
                    gri = #gri{type = op_replica, id = GriId, aspect = Aspect, scope = Scope},
                    data = Data2
                }
        end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Neither 'provider_id' nor 'migration_provider_id' fields are required to
%% create transfer using deprecated replicas endpoints. Nonetheless they were
%% added to required in data_spec due to following reasons:
%% - 'provider_id' - if it is not present provider which received request will
%%                   substitute its id, which not always will be the one expected
%%                   by tests.
%% - 'migration_provider_id` - if it is not present eviction will be performed
%%                             instead of expected migration.
%% Because those fields were added to 'required' in data_spec test cases with
%% them will be run for run_missing_required_data_test_cases scenario but will
%% not return expected errors (missing required data) and so they must be skipped.
%% @end
%%--------------------------------------------------------------------
should_skip_replica_testcase(Type, Data) ->
    IsProviderIdMissing = not maps:is_key(<<"provider_id">>, Data),
    IsProviderIdMissing or case Type of
        migration -> not maps:is_key(<<"migration_provider_id">>, Data);
        _ -> false
    end.


%% @private
build_create_transfer_validate_rest_call_result_fun(MemRef) ->
    fun(#api_test_ctx{node = Node} = TestCtx, Result) ->
        {ok, _, Headers, Body} = ?assertMatch(
            {ok, ?HTTP_201_CREATED, #{<<"Location">> := _}, #{<<"transferId">> := _}},
            Result
        ),
        TransferId = maps:get(<<"transferId">>, Body),

        ExpLocation = rpc:call(Node, oneprovider, get_rest_endpoint, [
            string:trim(filename:join([<<"/">>, <<"transfers">>, TransferId]), leading, [$/])
        ]),
        ?assertEqual(ExpLocation, maps:get(<<"Location">>, Headers)),

        build_create_transfer_validate_call_result(MemRef, TransferId, TestCtx)
    end.


%% @private
build_create_transfer_validate_gs_call_result_fun(MemRef) ->
    fun(TestCtx, Result) ->
        {ok, #{<<"transferId">> := TransferId}} = ?assertMatch({ok, _}, Result),
        build_create_transfer_validate_call_result(MemRef, TransferId, TestCtx)
    end.


%% @private
build_create_transfer_validate_call_result(MemRef, TransferId, #api_test_ctx{
    node = TestNode,
    client = ?USER(UserId),
    data = Data
}) ->
    #{exp_transfer := ExpTransferStats} = api_test_memory:get(MemRef, transfer_details),

    ExpTransfer = ExpTransferStats#{
        user_id => UserId,
        scheduling_provider => transfers_test_utils:provider_id(TestNode),
        failed_files => 0
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
            test_utils:set_env(Worker, ?APP_NAME, public_block_percent_treshold, 0),
            test_utils:set_env(Worker, ?APP_NAME, rerun_transfers, false)
        end, ?config(op_worker_nodes, NewConfig2)),
        ssl:start(),
        hackney:start(),
        NewConfig3 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig2, "env_desc.json"),
            NewConfig2
        ),
        initializer:mock_auth_manager(NewConfig3, _CheckIfUserIsSupported = true),
        ssl:start(),
        hackney:start(),
        start_http_server(),
        NewConfig3
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(Config) ->
    stop_http_server(),
    hackney:stop(),
    ssl:stop(),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:teardown_storage(Config).


init_per_testcase(_Case, Config) ->
    initializer:mock_share_logic(Config),
    ct:timetrap({minutes, 30}),

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
    CallTime = global_clock:timestamp_seconds(),
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
replace_placeholder_value(Key, Value, Data) ->
    case maps:get(Key, Data, undefined) of
        ?PLACEHOLDER ->
            Data#{Key => Value};
        _ ->
            Data
    end.
