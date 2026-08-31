%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning transfer API (REST + gs):
%%% creation, status retrieval, cancellation and rerun.
%%%
%%% The transfer type semantics (replication/eviction/migration) are
%%% covered by the transfer machinery suites, which drive the internal
%%% API directly - the tests here cover the layers above it (parameter
%%% sanitization, authorization, REST/gs plumbing), where the only
%%% type-dependent aspects are the required provider id parameters and
%%% the required space privileges. Each test case therefore runs a
%%% single transfer type - the one exercising the richest slice of
%%% those aspects - instead of iterating over all of them.
%%% @end
%%%-------------------------------------------------------------------
-module(api_transfer_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_test_runner.hrl").
-include("middleware/middleware.hrl").
-include("modules/datastore/transfer.hrl").
-include("onenv_test_utils.hrl").
-include("transfer_test.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/onedata_file.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/assertions.hrl").
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
    create_file_transfer/1,
    create_view_transfer/1,

    get_file_transfer_status/1,
    get_view_transfer_status/1,
    get_rerun_transfer_status/1,

    cancel_transfer/1,
    rerun_transfer/1
]).

%% TODO VFS-10259 add scheduling privilege checks for eviction and migration
%% of a file the scheduling user cannot access
all() -> [
    create_file_transfer,
    create_view_transfer,

    get_file_transfer_status,
    get_view_transfer_status,
    get_rerun_transfer_status,

    cancel_transfer,
    rerun_transfer
].


% Client conventions (see client_spec/0):
% - user3 owns the transferred files but is stripped of (some of) the privileges
%   required for the tested operation - forbidden despite being the owner;
% - user4 holds them - correct client;
% - user2 (space owner) is the setup client, exempt from privilege checks
%   (schedules the transfers the cancel/rerun tests operate on, so that
%   the forbidden client hits the non-creator authorization path).
-define(SPACE_SELECTOR, space_krk_par).
-define(FILES_OWNER_FORBIDDEN_CLIENT, user3).
-define(CORRECT_CLIENT, user4).

-define(VIEW_XATTR_VALUE, 1).

-define(TEST_TRANSFER_TYPES, [<<"replication">>, <<"eviction">>, <<"migration">>]).
-define(DATA_SOURCE_TYPES, [<<"file">>, <<"view">>]).

% Parameters having below value were not assigned proper value in data_spec()
% definition and should be given one by `prepare_arg_fun`
-define(PLACEHOLDER, placeholder).

-define(TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES, [
    {<<"type">>, 100, {gs, ?ERR_BAD_VALUE_STRING(<<"type">>)}},
    {<<"type">>, <<"transfer">>, ?ERR_BAD_VALUE_NOT_ALLOWED(<<"type">>, ?TEST_TRANSFER_TYPES)},
    {<<"dataSourceType">>, 100, {gs, ?ERR_BAD_VALUE_STRING(<<"dataSourceType">>)}},
    {<<"dataSourceType">>, <<"data">>, ?ERR_BAD_VALUE_NOT_ALLOWED(<<"dataSourceType">>, ?DATA_SOURCE_TYPES)}
]).

-define(PROVIDER_ID_TRANSFER_ERRORS(__KEY, __SPACE_ID), [
    {__KEY, 100, ?ERR_BAD_VALUE_STRING(__KEY)},
    {__KEY, <<"NonExistingProvider">>, ?ERR_SPACE_NOT_SUPPORTED_BY(__SPACE_ID, <<"NonExistingProvider">>)}
]).

-define(CALLBACK_TRANSFER_ERRORS, [{<<"callback">>, 100, ?ERR_BAD_VALUE_STRING(<<"callback">>)}]).

% The callback http server runs on the op-worker node of below provider -
% unlike the CT master, provider pods are always reachable from other pods
% regardless of the network topology between the local one-env deployment
% and the CT docker (e.g. on minikube the CT master docker bridge address
% is not routable from pods). Callback receipt is recorded in the node's
% node_cache and polled by the test via rpc.
-define(HTTP_SERVER_PROVIDER_SELECTOR, krakow).
-define(HTTP_SERVER_PORT, 18080).
-define(HTTP_SERVER_START_ATTEMPTS, 100).
-define(HTTP_SERVER_START_RETRY_INTERVAL_MS, 100).
-define(ENDED_TRANSFERS_PATH, "/ended_transfers").

-define(CALLBACK_CALL_TIME_KEY(__TRANSFER_ID), {transfer_callback_call_time, __TRANSFER_ID}).

-define(PROVIDER_GRI_ID(__PROVIDER_ID), gri:serialize(#gri{
    type = op_provider,
    id = __PROVIDER_ID,
    aspect = instance,
    scope = protected
})).

% Per-invocation transfer data (target files, ids, expected stats) stashed in
% api_test_memory under the transfer_details key by the setup/scheduling helpers.
-type transfer_details() :: #{atom() => term()}.


%%%===================================================================
%%% Create transfer test functions
%%%===================================================================


%% Migration is the create data spec superset: it requires both
%% replicatingProviderId and evictingProviderId (with their bad values)
%% and both scheduling privileges.
create_file_transfer(_Config) ->
    CaseName = ?FUNCTION_NAME,
    Type = migration,
    TestSuiteCtx = build_suite_ctx(Type),
    set_up_test_users_privileges(
        TestSuiteCtx,
        [?SPACE_SCHEDULE_REPLICATION, ?SPACE_SCHEDULE_EVICTION],
        [?SPACE_SCHEDULE_EVICTION]
    ),

    % Shared file will be used to assert that shared file transfer will be forbidden
    % (it will be added to '#data_spec.bad_values'). It is created by user2 -
    % the space owner - as creating shares requires privileges the test users
    % are stripped of above.
    #object{guid = SharedFileGuid, shares = [ShareId]} = onenv_file_test_utils:create_and_sync_file_tree(
        user2, ?SPACE_SELECTOR, #file_spec{
            name = str_utils:format_bin("~ts_shared_file_~ts", [CaseName, str_utils:rand_hex(6)]),
            shares = [#share_spec{}]
        }
    ),
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),

    MemRef = api_test_memory:init(),
    SetupFun = build_create_file_transfer_setup_fun(
        TestSuiteCtx, MemRef, CaseName
    ),
    VerifyFun = build_create_transfer_verify_fun(TestSuiteCtx, MemRef),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [krakow, paris],
            client_spec = client_spec(),
            setup_fun = SetupFun,
            verify_fun = VerifyFun,
            % every successful invocation runs a full transfer - exercising
            % each correct data set on one randomly chosen endpoint (instead
            % of on all of them) keeps the suite duration sane
            randomly_select_scenarios = true,
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Transfer (~tp) file using /transfers rest endpoint", [Type]),
                    type = rest,
                    prepare_args_fun = build_create_transfer_prepare_rest_args_fun(MemRef),
                    validate_result_fun = build_create_transfer_validate_rest_call_result_fun(TestSuiteCtx, MemRef)
                },
                #scenario_template{
                    name = str_utils:format("Transfer (~tp) file using gs transfer api", [Type]),
                    type = gs,
                    prepare_args_fun = build_create_transfer_prepare_gs_args_fun(MemRef, private),
                    validate_result_fun = build_create_transfer_validate_gs_call_result_fun(TestSuiteCtx, MemRef)
                }
            ],
            data_spec = api_test_utils:replace_enoent_with_error_not_found_in_error_expectations(
                api_test_utils:add_cdmi_id_errors_for_operations_not_available_in_share_mode(
                    SharedFileGuid, SpaceId, ShareId,
                    build_op_transfer_spec(TestSuiteCtx, <<"file">>)
                )
            )
        }
    ])).


%% Replication suffices for the view variant - the view-specific data
%% spec aspects (spaceId, viewName, queryViewParams) and the
%% ?SPACE_QUERY_VIEWS privilege do not depend on the transfer type.
create_view_transfer(_Config) ->
    CaseName = ?FUNCTION_NAME,
    Type = replication,
    TestSuiteCtx = build_suite_ctx(Type),
    set_up_test_users_privileges(
        TestSuiteCtx,
        [?SPACE_SCHEDULE_REPLICATION, ?SPACE_QUERY_VIEWS],
        [?SPACE_QUERY_VIEWS]
    ),

    MemRef = api_test_memory:init(),
    SetupFun = build_create_view_transfer_setup_fun(
        TestSuiteCtx, MemRef, CaseName
    ),
    VerifyFun = build_create_transfer_verify_fun(TestSuiteCtx, MemRef),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [krakow, paris],
            client_spec = client_spec(),
            setup_fun = SetupFun,
            verify_fun = VerifyFun,
            % see the file transfer suite_spec above
            randomly_select_scenarios = true,
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Transfer (~tp) view using /transfers rest endpoint", [Type]),
                    type = rest,
                    prepare_args_fun = build_create_transfer_prepare_rest_args_fun(MemRef),
                    validate_result_fun = build_create_transfer_validate_rest_call_result_fun(TestSuiteCtx, MemRef)
                },
                #scenario_template{
                    name = str_utils:format("Transfer (~tp) view using gs transfer api", [Type]),
                    type = gs,
                    prepare_args_fun = build_create_transfer_prepare_gs_args_fun(MemRef, private),
                    validate_result_fun = build_create_transfer_validate_gs_call_result_fun(TestSuiteCtx, MemRef)
                }
            ],
            data_spec = build_op_transfer_spec(TestSuiteCtx, <<"view">>)
        }
    ])).


%% @private
build_op_transfer_spec(TestSuiteCtx, DataSourceType) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),
    {ReplicatingProviderId, EvictingProviderId} =
        get_transfer_target_provider_ids(TestSuiteCtx),
    {Required, Optional, CorrectValues, BadValues} = get_data_source_dependent_data_spec_aspects(
        DataSourceType, SpaceId
    ),
    {TypeSpecificRequired, TypeSpecificCorrectValues, TypeSpecificBadValues} =
        get_type_dependent_data_spec_aspects(
            TestSuiteCtx#transfer_test_suite_ctx.transfer_type,
            SpaceId, ReplicatingProviderId, EvictingProviderId
        ),
    #data_spec{
        required = [<<"type">>, <<"dataSourceType">>] ++ TypeSpecificRequired ++ Required,
        optional = [<<"callback">> | Optional],
        correct_values = maps:merge(CorrectValues#{
            <<"type">> => [atom_to_binary(TestSuiteCtx#transfer_test_suite_ctx.transfer_type, utf8)],
            <<"dataSourceType">> => [DataSourceType],
            <<"callback">> => [get_callback_url()]
        }, TypeSpecificCorrectValues),
        bad_values = lists:flatten([
            ?TYPE_AND_DATA_SOURCE_TYPE_BAD_VALUES,
            TypeSpecificBadValues,
            ?CALLBACK_TRANSFER_ERRORS,
            BadValues
        ])
    }.


%% @private
get_type_dependent_data_spec_aspects(replication, SpaceId, ReplicatingProviderId, _EvictingProviderId) ->
    {
        [<<"replicatingProviderId">>],
        #{<<"replicatingProviderId">> => [ReplicatingProviderId]},
        ?PROVIDER_ID_TRANSFER_ERRORS(<<"replicatingProviderId">>, SpaceId)
    };
get_type_dependent_data_spec_aspects(migration, SpaceId, ReplicatingProviderId, EvictingProviderId) ->
    {
        [<<"replicatingProviderId">>, <<"evictingProviderId">>],
        #{
            <<"replicatingProviderId">> => [ReplicatingProviderId],
            <<"evictingProviderId">> => [EvictingProviderId]
        },
        ?PROVIDER_ID_TRANSFER_ERRORS(<<"replicatingProviderId">>, SpaceId)
            ++ ?PROVIDER_ID_TRANSFER_ERRORS(<<"evictingProviderId">>, SpaceId)
    }.


%% @private
get_data_source_dependent_data_spec_aspects(<<"file">>, _SpaceId) ->
    {[<<"fileId">>], [], #{<<"fileId">> => [?PLACEHOLDER]}, []};
get_data_source_dependent_data_spec_aspects(<<"view">>, SpaceId) ->
    RequiredParams = [<<"spaceId">>, <<"viewName">>],
    OptionalParams = [<<"queryViewParams">>],
    CorrectValues = #{
        <<"spaceId">> => [SpaceId],
        <<"viewName">> => [?PLACEHOLDER],
        % Below value will not affect test (view has only up to 5 files) and checks only
        % that server accepts it - view transfers with query options should have distinct suite
        <<"queryViewParams">> => [#{<<"limit">> => 100}]
    },
    BadValues = [
        {<<"spaceId">>, 100, ?ERR_BAD_VALUE_STRING(<<"spaceId">>)},
        {<<"spaceId">>, <<"NonExistingSpace">>, ?ERR_FORBIDDEN},

        {<<"viewName">>, 100, ?ERR_BAD_VALUE_STRING(<<"viewName">>)},
        {<<"viewName">>, <<"NonExistingView">>, {error_fun, fun(#api_test_ctx{node = Node}) ->
            ?ERR_VIEW_NOT_EXISTS_ON(opw_test_rpc:get_provider_id(Node))
        end}},

        {<<"queryViewParams">>, #{<<"bbox">> => 123}, ?ERR_BAD_DATA(<<"bbox">>, undefined)},
        {<<"queryViewParams">>, #{<<"descending">> => <<"ascending">>}, ?ERR_BAD_VALUE_BOOLEAN(<<"descending">>)},
        {<<"queryViewParams">>, #{<<"limit">> => <<"inf">>}, ?ERR_BAD_VALUE_INTEGER(<<"limit">>)},
        {<<"queryViewParams">>, #{<<"limit">> => 0}, ?ERR_BAD_VALUE_TOO_LOW(<<"limit">>, 1)},
        {<<"queryViewParams">>, #{<<"stale">> => <<"fresh">>},
            ?ERR_BAD_VALUE_NOT_ALLOWED(<<"stale">>, [<<"ok">>, <<"update_after">>, <<"false">>])}
    ],
    {RequiredParams, OptionalParams, CorrectValues, BadValues}.


%% @private
build_create_transfer_prepare_rest_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}) ->
        TransferDetails = get_transfer_details(MemRef),

        #rest_args{
            method = post,
            path = <<"transfers">>,
            headers = #{?HDR_CONTENT_TYPE => <<"application/json">>},
            body = json_utils:encode(substitute_transfer_data_source(TransferDetails, Data))
        }
    end.


%% @private
build_create_transfer_prepare_gs_args_fun(MemRef, Scope) ->
    fun(#api_test_ctx{data = Data}) ->
        TransferDetails = get_transfer_details(MemRef),

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
build_create_transfer_validate_rest_call_result_fun(TestSuiteCtx, MemRef) ->
    fun(#api_test_ctx{node = Node} = TestCtx, Result) ->
        {ok, _, Headers, Body} = ?assertMatch(
            {ok, ?HTTP_201_CREATED, #{?HDR_LOCATION := _}, #{<<"transferId">> := _}},
            Result
        ),
        TransferId = maps:get(<<"transferId">>, Body),

        ExpLocation = api_test_utils:build_rest_url(Node, [<<"transfers">>, TransferId]),
        ?assertEqual(ExpLocation, maps:get(?HDR_LOCATION, Headers)),

        validate_created_transfer(TestSuiteCtx, MemRef, TransferId, TestCtx)
    end.


%% @private
build_create_transfer_validate_gs_call_result_fun(TestSuiteCtx, MemRef) ->
    fun(TestCtx, Result) ->
        {ok, #{<<"gri">> := GRI}} = ?assertMatch({ok, _}, Result),
        #gri{id = TransferId} = gri:deserialize(GRI),
        validate_created_transfer(TestSuiteCtx, MemRef, TransferId, TestCtx)
    end.


%% @private
validate_created_transfer(TestSuiteCtx, MemRef, TransferId, #api_test_ctx{data = Data} = TestCtx) ->
    % Await transfer end and assert proper transfer stats
    await_transfer_ended(TestSuiteCtx, MemRef, TransferId, TestCtx),

    % If callback/url was supplied await feedback about transfer end
    % and assert that it came right after transfer ended - satisfy predicate:
    % CallTime - 10 < #transfer.finish_time <= CallTime
    case maps:is_key(<<"callback">>, Data) orelse maps:is_key(<<"url">>, Data) of
        true ->
            #api_test_ctx{node = TestNode} = TestCtx,
            CallTime = await_callback_call(TransferId),
            {ok, #document{value = #transfer{finish_time = FinishTime}}} = ?assertMatch(
                {ok, _}, opw_test_rpc:call(TestNode, transfer, get, [TransferId])
            ),
            ?assert(FinishTime > CallTime - 10 andalso FinishTime =< CallTime);
        false ->
            ok
    end.


%% @private
await_callback_call(TransferId) ->
    {called, CallTime} = ?assertMatch({called, _}, opw_test_rpc:call(
        ?HTTP_SERVER_PROVIDER_SELECTOR, node_cache, get,
        [?CALLBACK_CALL_TIME_KEY(TransferId), undefined]
    ), ?ATTEMPTS),
    CallTime.


%%%===================================================================
%%% Get transfer status test functions
%%%===================================================================


%% Migration covers the richest get response: both replication and
%% eviction statuses evolve and the replication histograms are present.
get_file_transfer_status(_Config) ->
    get_transfer_status_test_base(?FUNCTION_NAME, migration, file).


%% Eviction covers the response branches migration does not: skipped
%% replication status and empty histograms.
get_view_transfer_status(_Config) ->
    get_transfer_status_test_base(?FUNCTION_NAME, eviction, view).


%% @private
get_transfer_status_test_base(CaseName, TransferType, DataSourceType) ->
    TestSuiteCtx = build_suite_ctx(TransferType),
    set_up_test_users_privileges(
        TestSuiteCtx, [?SPACE_VIEW_TRANSFERS], [?SPACE_VIEW_TRANSFERS]
    ),

    % The transfer is gated (held mid-flight) so that its ongoing state can be
    % observed before it is released to run to completion.
    transfer_test_utils:mock_gated_file_processing(TestSuiteCtx),
    try
        MemRef = api_test_memory:init(),
        case DataSourceType of
            file -> start_file_transfer(TestSuiteCtx, MemRef, CaseName);
            view -> start_view_transfer(TestSuiteCtx, MemRef, CaseName)
        end,
        transfer_test_utils:await_gated_file_processing_job(),
        Env = get_transfer_details(MemRef),

        run_get_transfer_status_tests(TransferType, DataSourceType, Env, ongoing),

        transfer_test_utils:grant_file_processing_permits(TestSuiteCtx, all),
        await_transfer_end(TestSuiteCtx, maps:get(transfer_id, Env)),

        run_get_transfer_status_tests(TransferType, DataSourceType, Env, ended)
    after
        transfer_test_utils:unmock_gated_file_processing(TestSuiteCtx)
    end.


%% @private
run_get_transfer_status_tests(TransferType, DataSourceType, Env, ExpState) ->
    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [krakow, paris],
            client_spec = client_spec(),
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Get transfer (~tp) status using rest endpoint", [TransferType]),
                    type = rest,
                    prepare_args_fun = build_get_transfer_status_prepare_rest_args_fun(Env),
                    validate_result_fun = build_get_transfer_status_validate_rest_call_result_fun(
                        TransferType, DataSourceType, ExpState, Env
                    )
                },
                #scenario_template{
                    name = str_utils:format("Get transfer (~tp) status using gs transfer api", [TransferType]),
                    type = gs,
                    prepare_args_fun = build_get_transfer_status_prepare_gs_args_fun(Env),
                    validate_result_fun = build_get_transfer_status_validate_gs_call_result_fun(DataSourceType, ExpState, Env)
                }
            ],
            data_spec = #data_spec{bad_values = [
                {bad_id, <<"NonExistentTransfer">>, ?ERROR_NOT_FOUND}
            ]}
        }
    ])).


%% @private
build_get_transfer_status_prepare_rest_args_fun(#{transfer_id := TransferId}) ->
    fun(#api_test_ctx{data = Data}) ->
        {Id, _} = api_test_utils:maybe_substitute_bad_id(TransferId, Data),

        #rest_args{
            method = get,
            path = <<"transfers/", Id/binary>>
        }
    end.


%% @private
build_get_transfer_status_prepare_gs_args_fun(#{transfer_id := TransferId}) ->
    fun(#api_test_ctx{data = Data}) ->
        {Id, _} = api_test_utils:maybe_substitute_bad_id(TransferId, Data),

        #gs_args{
            operation = get,
            gri = #gri{type = op_transfer, id = Id, aspect = instance}
        }
    end.


%% @private
build_get_transfer_status_validate_rest_call_result_fun(TransferType, DataSourceType, ExpState, Env) ->
    AlwaysPresentFields = [
        <<"type">>, <<"dataSourceType">>,
        <<"userId">>, <<"rerunId">>, <<"spaceId">>, <<"callback">>,

        <<"replicatingProviderId">>, <<"evictingProviderId">>,
        <<"transferStatus">>, <<"replicationStatus">>, <<"evictionStatus">>,

        <<"effectiveJobStatus">>, <<"effectiveJobTransferId">>,

        <<"filesToProcess">>, <<"filesProcessed">>,
        <<"filesReplicated">>, <<"bytesReplicated">>, <<"filesEvicted">>,
        <<"filesFailed">>,

        <<"scheduleTime">>, <<"startTime">>, <<"finishTime">>,
        <<"lastUpdate">>, <<"minHist">>, <<"hrHist">>, <<"dyHist">>, <<"mthHist">>
    ],
    AllFields = case DataSourceType of
        file -> [<<"fileId">>, <<"filePath">> | AlwaysPresentFields];
        view -> [<<"viewName">>, <<"queryViewParams">> | AlwaysPresentFields]
    end,
    AllFieldsSorted = lists:sort(AllFields),

    fun(#api_test_ctx{node = TestNode}, Result) ->
        {ok, _, _, Body} = ?assertMatch({ok, ?HTTP_200_OK, _, _}, Result),
        ?assertMatch(AllFieldsSorted, lists:sort(maps:keys(Body))),

        assert_proper_constant_fields_in_get_status_rest_response(TransferType, DataSourceType, Env, Body),
        assert_proper_status_in_get_status_rest_response(ExpState, Env, Body),
        assert_proper_file_stats_in_get_status_rest_response(ExpState, Env, Body),
        assert_proper_histograms_in_get_status_rest_response(TransferType, ExpState, Env, Body),

        CreationTime = maps:get(creation_time, Env),
        % provider clock, not the CT master's - see the creation_time note
        % in start_transfer/6
        Now = opw_test_rpc:call(TestNode, global_clock, timestamp_seconds, []),
        assert_proper_times_in_get_status_rest_response(ExpState, CreationTime, Now, Body)
    end.


%% @private
assert_proper_constant_fields_in_get_status_rest_response(TransferType, DataSourceType, Env, Data) ->
    #{
        replicating_provider := ReplicatingProvider,
        evicting_provider := EvictingProvider
    } = maps:get(exp_transfer, Env),

    BasicConstantFields = #{
        <<"userId">> => maps:get(user_id, Env),
        <<"rerunId">> => null,
        <<"effectiveJobTransferId">> => maps:get(transfer_id, Env),
        <<"spaceId">> => maps:get(space_id, Env),
        <<"callback">> => maps:get(callback, Env, null),

        <<"type">> => atom_to_binary(TransferType, utf8),
        <<"replicatingProviderId">> => utils:undefined_to_null(ReplicatingProvider),
        <<"evictingProviderId">> => utils:undefined_to_null(EvictingProvider),

        <<"dataSourceType">> => atom_to_binary(DataSourceType, utf8)
    },
    DataSourceDependentConstantFields = case DataSourceType of
        file ->
            #{
                <<"fileId">> => maps:get(root_file_cdmi_id, Env),
                <<"filePath">> => maps:get(root_file_path, Env)
            };
        view ->
            #{
                <<"viewName">> => maps:get(view_name, Env),
                <<"queryViewParams">> => maps:get(query_view_params, Env)
            }
    end,
    ExpConstantFields = maps:merge(BasicConstantFields, DataSourceDependentConstantFields),

    ?assertEqual(ExpConstantFields, maps:with(maps:keys(ExpConstantFields), Data)).


assert_proper_status_in_get_status_rest_response(ExpState, Env, #{
    <<"transferStatus">> := TransferStatus,
    <<"effectiveJobStatus">> := EffJobStatus,
    <<"replicationStatus">> := ReplicationStatus,
    <<"evictionStatus">> := EvictionStatus
}) ->
    #{
        replication_status := ExpReplicationStatus,
        eviction_status := ExpEvictionStatus
    } = maps:get(exp_transfer, Env),

    % Without rerun EffJobStatus should always equal TransferStatus
    ?assertEqual(TransferStatus, EffJobStatus),

    case ExpState of
        ongoing ->
            ?assertNotEqual(TransferStatus, <<"completed">>),
            case ExpReplicationStatus of
                skipped -> ?assertEqual(ReplicationStatus, <<"skipped">>);
                _ -> ?assertNotEqual(ReplicationStatus, atom_to_binary(ExpReplicationStatus, utf8))
            end,
            case ExpEvictionStatus of
                skipped -> ?assertEqual(EvictionStatus, <<"skipped">>);
                _ -> ?assertNotEqual(EvictionStatus, atom_to_binary(ExpEvictionStatus, utf8))
            end;
        ended ->
            ?assertEqual(TransferStatus, <<"completed">>),
            ?assertEqual(ReplicationStatus, atom_to_binary(ExpReplicationStatus, utf8)),
            ?assertEqual(EvictionStatus, atom_to_binary(ExpEvictionStatus, utf8))
    end.


%% @private
assert_proper_file_stats_in_get_status_rest_response(ExpState, Env, #{
    <<"filesToProcess">> := FilesToProcess,
    <<"filesProcessed">> := FilesProcessed,
    <<"filesReplicated">> := FilesReplicated,
    <<"bytesReplicated">> := BytesReplicated,
    <<"filesEvicted">> := FilesEvicted,
    <<"filesFailed">> := FailedFiles
}) ->
    #{
        files_to_process := ExpFilesToProcess,
        files_processed := ExpFilesProcessed,
        files_replicated := ExpFilesReplicated,
        bytes_replicated := ExpBytesReplicated,
        files_evicted := ExpFilesEvicted
    } = maps:get(exp_transfer, Env),

    CompareFun = case ExpState of
        ongoing -> fun(X, Y) -> X =< Y end;
        ended -> fun(X, Y) -> X == Y end
    end,

    ?assertEqual(FailedFiles, 0),
    ?assert(CompareFun(FilesToProcess, ExpFilesToProcess)),
    ?assert(CompareFun(FilesProcessed, ExpFilesProcessed)),
    ?assert(CompareFun(FilesReplicated, ExpFilesReplicated)),
    ?assert(CompareFun(BytesReplicated, ExpBytesReplicated)),
    ?assert(CompareFun(FilesEvicted, ExpFilesEvicted)).


%% @private
assert_proper_histograms_in_get_status_rest_response(eviction, _, _, Data) ->
    ?assertEqual(
        #{<<"minHist">> => #{}, <<"hrHist">> => #{}, <<"dyHist">> => #{}, <<"mthHist">> => #{}},
        maps:with([<<"minHist">>, <<"hrHist">>, <<"dyHist">>, <<"mthHist">>], Data)
    );
assert_proper_histograms_in_get_status_rest_response(_TransferType, ExpState, Env, Data) ->
    SrcProviderId = maps:get(src_provider_id, Env),
    #{bytes_replicated := BytesReplicated} = maps:get(exp_transfer, Env),

    ?assert(lists:all(fun({Key, ExpLen}) ->
        case maps:to_list(maps:get(Key, Data)) of
            [] ->
                true;
            [{SrcProviderId, Hist}] ->
                HasProperLen = length(Hist) == ExpLen,
                HasProperValue = case ExpState of
                    ongoing -> lists:sum(Hist) =< BytesReplicated;
                    ended -> lists:sum(Hist) == BytesReplicated
                end,
                HasProperLen andalso HasProperValue;
            _ ->
                false
        end
    end, [
        {<<"minHist">>, ?MIN_HIST_LENGTH},
        {<<"hrHist">>, ?HOUR_HIST_LENGTH},
        {<<"dyHist">>, ?DAY_HIST_LENGTH},
        {<<"mthHist">>, ?MONTH_HIST_LENGTH}
    ])).


%% @private
assert_proper_times_in_get_status_rest_response(ongoing, CreationTime, _Now, #{
    <<"scheduleTime">> := ScheduleTime,
    <<"startTime">> := StartTime,
    <<"finishTime">> := FinishTime
}) ->
    ?assert(CreationTime =< ScheduleTime),
    ?assert(0 == StartTime orelse ScheduleTime =< StartTime),
    ?assert(0 == FinishTime);
assert_proper_times_in_get_status_rest_response(ended, CreationTime, Now, #{
    <<"scheduleTime">> := ScheduleTime,
    <<"startTime">> := StartTime,
    <<"finishTime">> := FinishTime
}) ->
    ?assert(CreationTime =< ScheduleTime),
    ?assert(ScheduleTime =< StartTime),
    ?assert(StartTime =< FinishTime),
    ?assert(FinishTime =< Now).


%% @private
build_get_transfer_status_validate_gs_call_result_fun(DataSourceType, ExpState, #{
    user_id := UserId,
    creation_time := CreationTime,
    transfer_id := TransferId,
    exp_transfer := #{
        replicating_provider := ReplicatingProvider,
        evicting_provider := EvictingProvider
    }
} = Env) ->

    {ExpDataSourceType, DataSourceId, DataSourceName, QueryViewParams} = case DataSourceType of
        file ->
            FileType = maps:get(root_file_type, Env),
            FileGuid = maps:get(root_file_guid, Env),
            FilePath = maps:get(root_file_path, Env),
            {FileType, FileGuid, FilePath, #{}};
        view ->
            ViewName = maps:get(view_name, Env),
            ViewId = maps:get(view_id, Env),
            {<<"view">>, ViewId, ViewName, maps:get(query_view_params, Env)}
    end,
    ConstantValues = #{
        <<"gri">> => gri:serialize(#gri{type = op_transfer, id = TransferId, aspect = instance}),
        <<"revision">> => 1,

        <<"userId">> => UserId,
        <<"type">> => case {ReplicatingProvider, EvictingProvider} of
            {_, undefined} -> <<"replication">>;
            {undefined, _} -> <<"eviction">>;
            {_, _} -> <<"migration">>
        end,
        <<"replicatingProvider">> => case ReplicatingProvider of
            undefined -> null;
            _ -> ?PROVIDER_GRI_ID(ReplicatingProvider)
        end,
        <<"evictingProvider">> => case EvictingProvider of
            undefined -> null;
            _ -> ?PROVIDER_GRI_ID(EvictingProvider)
        end,
        <<"dataSourceType">> => ExpDataSourceType,
        <<"dataSourceId">> => DataSourceId,
        <<"dataSourceName">> => DataSourceName,
        <<"queryParams">> => QueryViewParams
    },

    ConstantFields = maps:keys(ConstantValues),
    OtherFields = [<<"isOngoing">>, <<"startTime">>, <<"scheduleTime">>, <<"finishTime">>],
    AllFields = lists:sort(ConstantFields ++ OtherFields),

    fun(#api_test_ctx{node = TestNode}, Result) ->
        {ok, Transfer} = ?assertMatch({ok, _}, Result),

        ?assertMatch(AllFields, lists:sort(maps:keys(Transfer))),
        ?assertEqual(ConstantValues, maps:with(ConstantFields, Transfer)),

        IsOngoing = maps:get(<<"isOngoing">>, Transfer),
        ScheduleTime = maps:get(<<"scheduleTime">>, Transfer),
        StartTime = maps:get(<<"startTime">>, Transfer),
        FinishTime = maps:get(<<"finishTime">>, Transfer),

        case ExpState of
            ongoing ->
                ?assert(IsOngoing),
                ?assert(CreationTime =< ScheduleTime),
                ?assert(0 == StartTime orelse ScheduleTime =< StartTime),
                ?assertEqual(null, FinishTime);
            ended ->
                % provider clock, not the CT master's - see the creation_time
                % note in start_transfer/6
                Now = opw_test_rpc:call(TestNode, global_clock, timestamp_seconds, []),

                ?assert(not IsOngoing),
                ?assert(CreationTime =< ScheduleTime),
                ?assert(ScheduleTime =< StartTime),
                ?assert(StartTime =< FinishTime),
                ?assert(FinishTime =< Now)
        end
    end.


%%%===================================================================
%%% Rerun transfer status test
%%%===================================================================


get_rerun_transfer_status(_Config) ->
    get_rerun_transfer_status_test_base(?FUNCTION_NAME).


%% @private
get_rerun_transfer_status_test_base(CaseName) ->
    TransferType = replication,
    % the stable mid-flight state of a gated replication - see
    % await_replication_enqueued/2
    ExpOngoingEffStatus = <<"enqueued">>,

    TestSuiteCtx = build_suite_ctx(TransferType),
    set_up_test_users_privileges(
        TestSuiteCtx, [?SPACE_VIEW_TRANSFERS], [?SPACE_VIEW_TRANSFERS]
    ),

    % A single-file transfer is used so that exactly one gated file job is
    % released per stage (letting each rerun be observed in its ongoing state).
    transfer_test_utils:mock_gated_file_processing(TestSuiteCtx),
    try
        MemRef = api_test_memory:init(),
        start_single_file_transfer(TestSuiteCtx, MemRef, CaseName),
        Env = get_transfer_details(MemRef),
        TransferId = maps:get(transfer_id, Env),

        transfer_test_utils:await_gated_file_processing_job(),
        complete_gated_transfer(TestSuiteCtx, TransferId),
        get_rerun_transfer_status_tests(TransferType, Env, null, TransferId, <<"completed">>),

        RerunId1 = transfer_test_utils:rerun_transfer(TestSuiteCtx, user3, TransferId),
        % rerun links are written on the rerunning provider - both providers
        % serve the status requests, so await the link sync before asserting
        % rerunId/effectiveJobTransferId derived by following the chain
        transfer_test_utils:await_transfer_rerun_id(TestSuiteCtx, TransferId, RerunId1),
        transfer_test_utils:await_gated_file_processing_job(),
        await_replication_enqueued(TestSuiteCtx, RerunId1),
        get_rerun_transfer_status_tests(TransferType, Env, RerunId1, RerunId1, ExpOngoingEffStatus),
        complete_gated_transfer(TestSuiteCtx, RerunId1),
        get_rerun_transfer_status_tests(TransferType, Env, RerunId1, RerunId1, <<"completed">>),

        RerunId2 = transfer_test_utils:rerun_transfer(TestSuiteCtx, user3, RerunId1),
        transfer_test_utils:await_transfer_rerun_id(TestSuiteCtx, RerunId1, RerunId2),
        transfer_test_utils:await_gated_file_processing_job(),
        await_replication_enqueued(TestSuiteCtx, RerunId2),
        get_rerun_transfer_status_tests(TransferType, Env, RerunId1, RerunId2, ExpOngoingEffStatus),
        complete_gated_transfer(TestSuiteCtx, RerunId2),
        get_rerun_transfer_status_tests(TransferType, Env, RerunId1, RerunId2, <<"completed">>)
    after
        transfer_test_utils:unmock_gated_file_processing(TestSuiteCtx)
    end.


%% @private
%% Releases the single already-parked-and-awaited file job of the transfer
%% and waits for it to end.
complete_gated_transfer(TestSuiteCtx, TransferId) ->
    transfer_test_utils:grant_file_processing_permits(TestSuiteCtx, 1),
    await_transfer_end(TestSuiteCtx, TransferId).


%% @private
get_rerun_transfer_status_tests(TransferType, Env, RerunId, EffTransferId, ExpEffStatus) ->
    VerifyResultFun = fun(_TestCtx, Result) ->
        {ok, _, _, Body} = ?assertMatch({ok, ?HTTP_200_OK, _, _}, Result),

        ?assertEqual(RerunId, maps:get(<<"rerunId">>, Body)),
        ?assertEqual(EffTransferId, maps:get(<<"effectiveJobTransferId">>, Body)),
        ?assertEqual(ExpEffStatus, maps:get(<<"effectiveJobStatus">>, Body))
    end,

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [krakow, paris],
            client_spec = client_spec(),
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Get transfer (~tp) rerun status using rest endpoint", [TransferType]),
                    type = rest,
                    prepare_args_fun = build_get_transfer_status_prepare_rest_args_fun(Env),
                    validate_result_fun = VerifyResultFun
                }
            ],
            data_spec = #data_spec{bad_values = [
                {bad_id, <<"NonExistentTransfer">>, ?ERROR_NOT_FOUND}
            ]}
        }
    ])).


%%%===================================================================
%%% Cancel transfer test functions
%%%===================================================================


%% Migration requires both cancel privileges of a non-creator client.
%% The cancelled transfers are scheduled by user2 (the space owner) so
%% that the forbidden client exercises the non-creator authorization
%% path; the creator path (no privileges needed) is checked separately.
cancel_transfer(_Config) ->
    CaseName = ?FUNCTION_NAME,
    TestSuiteCtx = (build_suite_ctx(migration))#transfer_test_suite_ctx{
        user_selector = user2
    },
    set_up_test_users_privileges(
        TestSuiteCtx,
        [?SPACE_CANCEL_REPLICATION, ?SPACE_CANCEL_EVICTION],
        [?SPACE_CANCEL_EVICTION]
    ),

    transfer_test_utils:mock_gated_file_processing(TestSuiteCtx),
    try
        % a cancellation attempt on an already ended transfer must fail -
        % prepare one by releasing its single gated replication job
        EndedMemRef = api_test_memory:init(),
        start_single_file_transfer(TestSuiteCtx, EndedMemRef, CaseName),
        transfer_test_utils:await_gated_file_processing_job(),
        #{transfer_id := EndedTransferId} = get_transfer_details(EndedMemRef),
        complete_gated_transfer(TestSuiteCtx, EndedTransferId),

        MemRef = api_test_memory:init(),

        ?assert(onenv_api_test_runner:run_tests([
            #suite_spec{
                target_nodes = [krakow, paris],
                client_spec = client_spec(),
                setup_fun = build_cancel_transfer_setup_fun(TestSuiteCtx, MemRef, CaseName),
                verify_fun = build_cancel_transfer_verify_fun(TestSuiteCtx, MemRef),
                scenario_templates = [
                    #scenario_template{
                        name = "Cancel transfer using DELETE /transfers/:tid rest endpoint",
                        type = rest,
                        prepare_args_fun = build_cancel_transfer_prepare_rest_args_fun(MemRef),
                        validate_result_fun = fun(_TestCtx, Result) ->
                            {ok, _, _, Body} = ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, Result),
                            ?assertEqual(#{}, Body)
                        end
                    },
                    #scenario_template{
                        name = "Cancel transfer using gs transfer api",
                        type = gs,
                        prepare_args_fun = build_cancel_transfer_prepare_gs_args_fun(MemRef),
                        validate_result_fun = fun(_TestCtx, Result) ->
                            ?assertEqual(ok, Result)
                        end
                    }
                ],
                data_spec = #data_spec{bad_values = [
                    {bad_id, <<"NonExistentTransfer">>, ?ERROR_NOT_FOUND},
                    {bad_id, EndedTransferId, ?ERR_TRANSFER_ALREADY_ENDED}
                ]}
            }
        ])),

        % the creator may cancel their own transfer without holding any
        % cancel privileges - user3 (stripped of ?SPACE_CANCEL_EVICTION
        % above) schedules a migration and cancels it nonetheless
        CreatorTestSuiteCtx = TestSuiteCtx#transfer_test_suite_ctx{user_selector = user3},
        CreatorMemRef = api_test_memory:init(),
        start_single_file_transfer(CreatorTestSuiteCtx, CreatorMemRef, CaseName),
        transfer_test_utils:await_gated_file_processing_job(),
        #{transfer_id := CreatorTransferId} = get_transfer_details(CreatorMemRef),

        ?assertEqual(ok, cancel_transfer_via_middleware(TestSuiteCtx, user3, CreatorTransferId)),
        transfer_test_utils:grant_file_processing_permits(TestSuiteCtx, 1),
        await_transfer_cancelled(TestSuiteCtx, CreatorTransferId)

        % a possibly leftover ongoing transfer of the client matrix (its last
        % invocation may have been a rejected one) is released and run to
        % completion by the gate unmock below
    after
        transfer_test_utils:unmock_gated_file_processing(TestSuiteCtx)
    end.


%% @private
build_cancel_transfer_setup_fun(TestSuiteCtx, MemRef, CaseName) ->
    fun() ->
        % only a successful cancellation consumes the current transfer -
        % a still ongoing one is reused by the next invocation
        IsOngoing = case api_test_memory:get(MemRef, transfer_details, undefined) of
            undefined ->
                false;
            #{transfer_id := TransferId} ->
                is_transfer_ongoing(
                    TestSuiteCtx#transfer_test_suite_ctx.creation_provider_selector,
                    TransferId
                )
        end,
        case IsOngoing of
            true ->
                ok;
            false ->
                start_single_file_transfer(TestSuiteCtx, MemRef, CaseName),
                transfer_test_utils:await_gated_file_processing_job()
        end
    end.


%% @private
build_cancel_transfer_verify_fun(TestSuiteCtx, MemRef) ->
    fun(ExpTestResult, #api_test_ctx{node = TestNode}) ->
        #{transfer_id := TransferId} = get_transfer_details(MemRef),
        case ExpTestResult of
            expected_failure ->
                % a rejected request must leave the transfer running
                ?assert(is_transfer_ongoing(TestNode, TransferId));
            expected_success ->
                % release the single parked replication job (a cancelled
                % migration never starts its eviction phase) and await the
                % cancellation outcome
                transfer_test_utils:grant_file_processing_permits(TestSuiteCtx, 1),
                await_transfer_cancelled(TestSuiteCtx, TransferId)
        end,
        true
    end.


%% @private
build_cancel_transfer_prepare_rest_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}) ->
        #{transfer_id := TransferId} = get_transfer_details(MemRef),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(TransferId, Data),

        #rest_args{
            method = delete,
            path = <<"transfers/", Id/binary>>
        }
    end.


%% @private
build_cancel_transfer_prepare_gs_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}) ->
        #{transfer_id := TransferId} = get_transfer_details(MemRef),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(TransferId, Data),

        #gs_args{
            operation = delete,
            gri = #gri{type = op_transfer, id = Id, aspect = cancel, scope = private}
        }
    end.


%%%===================================================================
%%% Rerun transfer test functions
%%%===================================================================


%% The rerun privilege requirements mirror the create scheduling ones
%% (exercised per type there) - replication suffices; what is
%% rerun-specific is the ended-transfer precondition and the rerun
%% linkage. The rerun targets are scheduled by user2 (the space owner) -
%% unlike for cancellation there is no creator exception and the
%% forbidden client is a plain non-creator space member.
rerun_transfer(_Config) ->
    CaseName = ?FUNCTION_NAME,
    TestSuiteCtx = (build_suite_ctx(replication))#transfer_test_suite_ctx{
        user_selector = user2
    },
    set_up_test_users_privileges(
        TestSuiteCtx, [?SPACE_SCHEDULE_REPLICATION], [?SPACE_SCHEDULE_REPLICATION]
    ),

    MemRef = api_test_memory:init(),
    % the rerun chain root - scheduled ungated and run to completion
    % (a rerun may only target an ended transfer)
    start_single_file_transfer(TestSuiteCtx, MemRef, CaseName),
    #{transfer_id := TransferId} = get_transfer_details(MemRef),
    await_transfer_end(TestSuiteCtx, TransferId),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [krakow, paris],
            client_spec = client_spec(),
            verify_fun = build_rerun_transfer_verify_fun(MemRef),
            scenario_templates = [
                #scenario_template{
                    name = "Rerun transfer using POST /transfers/:tid/rerun rest endpoint",
                    type = rest,
                    prepare_args_fun = build_rerun_transfer_prepare_rest_args_fun(MemRef),
                    validate_result_fun = build_rerun_transfer_validate_rest_call_result_fun(
                        TestSuiteCtx, MemRef
                    )
                },
                #scenario_template{
                    name = "Rerun transfer using gs transfer api",
                    type = gs,
                    prepare_args_fun = build_rerun_transfer_prepare_gs_args_fun(MemRef),
                    validate_result_fun = build_rerun_transfer_validate_gs_call_result_fun(
                        TestSuiteCtx, MemRef
                    )
                }
            ],
            data_spec = #data_spec{bad_values = [
                {bad_id, <<"NonExistentTransfer">>, ?ERROR_NOT_FOUND}
            ]}
        }
    ])),

    % rerunning a still ongoing transfer must fail
    transfer_test_utils:mock_gated_file_processing(TestSuiteCtx),
    try
        OngoingMemRef = api_test_memory:init(),
        start_single_file_transfer(TestSuiteCtx, OngoingMemRef, CaseName),
        transfer_test_utils:await_gated_file_processing_job(),
        #{transfer_id := OngoingTransferId} = get_transfer_details(OngoingMemRef),

        ?assertMatch(?ERR_TRANSFER_NOT_ENDED, rerun_transfer_via_middleware(
            TestSuiteCtx, user4, OngoingTransferId
        ))

        % the parked transfer is released and run to completion by the gate
        % unmock below
    after
        transfer_test_utils:unmock_gated_file_processing(TestSuiteCtx)
    end.


%% @private
build_rerun_transfer_prepare_rest_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}) ->
        #{transfer_id := TransferId} = get_transfer_details(MemRef),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(TransferId, Data),

        #rest_args{
            method = post,
            path = <<"transfers/", Id/binary, "/rerun">>
        }
    end.


%% @private
build_rerun_transfer_prepare_gs_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}) ->
        #{transfer_id := TransferId} = get_transfer_details(MemRef),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(TransferId, Data),

        #gs_args{
            operation = create,
            gri = #gri{type = op_transfer, id = Id, aspect = rerun, scope = private}
        }
    end.


%% @private
build_rerun_transfer_validate_rest_call_result_fun(TestSuiteCtx, MemRef) ->
    fun(#api_test_ctx{node = TestNode}, Result) ->
        {ok, _, Headers, Body} = ?assertMatch(
            {ok, ?HTTP_201_CREATED, #{?HDR_LOCATION := _}, #{<<"transferId">> := _}},
            Result
        ),
        NewTransferId = maps:get(<<"transferId">>, Body),

        ExpLocation = api_test_utils:build_rest_url(TestNode, [<<"transfers">>, NewTransferId]),
        ?assertEqual(ExpLocation, maps:get(?HDR_LOCATION, Headers)),

        validate_rerun_transfer(TestSuiteCtx, MemRef, NewTransferId)
    end.


%% @private
build_rerun_transfer_validate_gs_call_result_fun(TestSuiteCtx, MemRef) ->
    fun(_TestCtx, Result) ->
        {ok, #{<<"transferId">> := NewTransferId}} = ?assertMatch(
            {ok, #{<<"transferId">> := _}}, Result
        ),
        validate_rerun_transfer(TestSuiteCtx, MemRef, NewTransferId)
    end.


%% @private
validate_rerun_transfer(TestSuiteCtx, MemRef, NewTransferId) ->
    Details = #{transfer_id := RerunTargetId} = get_transfer_details(MemRef),

    % the rerun link is written by the rerunning provider - await it (and the
    % new transfer doc, polled on both providers by the transfer end await)
    % before targeting the new transfer
    transfer_test_utils:await_transfer_rerun_id(TestSuiteCtx, RerunTargetId, NewTransferId),
    await_transfer_end(TestSuiteCtx, NewTransferId),

    % subsequent invocations rerun the newest transfer of the chain
    api_test_memory:set(MemRef, transfer_details, Details#{transfer_id => NewTransferId}).


%% @private
build_rerun_transfer_verify_fun(MemRef) ->
    fun(ExpTestResult, #api_test_ctx{node = TestNode}) ->
        case ExpTestResult of
            expected_failure ->
                % a rejected rerun must leave the chain untouched - the
                % newest transfer of it has not been rerun
                #{transfer_id := TransferId} = get_transfer_details(MemRef),
                ?assertMatch(
                    {ok, #document{value = #transfer{rerun_id = undefined}}},
                    opw_test_rpc:call(TestNode, transfer, get, [TransferId])
                );
            expected_success ->
                % asserted by validate_result_fun
                ok
        end,
        true
    end.


%%%===================================================================
%%% Middleware request helpers
%%%===================================================================


%% @private
cancel_transfer_via_middleware(TestSuiteCtx, UserSelector, TransferId) ->
    call_middleware(TestSuiteCtx, UserSelector, #gri{
        type = op_transfer, id = TransferId, aspect = cancel
    }, delete).


%% @private
rerun_transfer_via_middleware(TestSuiteCtx, UserSelector, TransferId) ->
    call_middleware(TestSuiteCtx, UserSelector, #gri{
        type = op_transfer, id = TransferId, aspect = rerun
    }, create).


%% @private
call_middleware(#transfer_test_suite_ctx{
    creation_provider_selector = CreationProviderSelector
}, UserSelector, GRI, Operation) ->
    UserId = oct_background:get_user_id(UserSelector),
    SessionId = oct_background:get_user_session_id(UserSelector, CreationProviderSelector),

    opw_test_rpc:call(CreationProviderSelector, middleware, handle, [#op_req{
        auth = ?USER(UserId, SessionId),
        gri = GRI,
        operation = Operation
    }]).


%% @private
is_transfer_ongoing(NodeOrProviderSelector, TransferId) ->
    opw_test_rpc:call(NodeOrProviderSelector, fun() ->
        {ok, #document{value = Transfer}} = transfer:get(TransferId),
        transfer:is_ongoing(Transfer)
    end).


%%%===================================================================
%%% Transfer setup and scheduling helpers
%%%
%%% Transfer target providers follow the transfer_test_utils toolkit
%%% conventions: files are created on the creation provider (krakow) and,
%%% depending on the transfer type, the other provider (paris) is the
%%% replication target or the evicted one (eviction operates on the replicas
%%% set up by transfer_test_utils:ensure_initial_replicas/2; migration
%%% replicates to paris and evicts krakow).
%%%===================================================================


-spec build_suite_ctx(transfer_test_utils:transfer_type()) ->
    #transfer_test_suite_ctx{}.
build_suite_ctx(TransferType) ->
    #transfer_test_suite_ctx{
        transfer_type = TransferType,
        space_selector = ?SPACE_SELECTOR,
        user_selector = ?FILES_OWNER_FORBIDDEN_CLIENT,
        creation_provider_selector = krakow,
        other_provider_selector = paris
    }.


-spec client_spec() -> #client_spec{}.
client_spec() ->
    #client_spec{
        correct = [?CORRECT_CLIENT],
        unauthorized = [nobody],
        forbidden_not_in_space = [user1],
        % forbidden by lack of privileges (even though being owner of files)
        forbidden_in_space = [?FILES_OWNER_FORBIDDEN_CLIENT]
    }.


%% @private
%% Grants the given required privileges (on top of the plain member ones)
%% to the correct client and revokes the given ones from the forbidden
%% client (leaving it every other space privilege). Revoking just one of
%% several required privileges asserts the stronger property: lacking any
%% single one of them already forbids the tested operation.
-spec set_up_test_users_privileges(
    #transfer_test_suite_ctx{},
    CorrectClientPrivs :: [privileges:space_privilege()],
    ForbiddenClientRevokedPrivs :: [privileges:space_privilege()]
) ->
    ok.
set_up_test_users_privileges(#transfer_test_suite_ctx{
    space_selector = SpaceSelector
}, CorrectClientPrivs, ForbiddenClientRevokedPrivs) ->
    ozt_spaces:set_privileges(
        SpaceSelector, ?CORRECT_CLIENT,
        lists:usort(CorrectClientPrivs ++ privileges:space_member())
    ),
    ozt_spaces:set_privileges(
        SpaceSelector, ?FILES_OWNER_FORBIDDEN_CLIENT,
        privileges:space_admin() -- ForbiddenClientRevokedPrivs
    ).


%% @private
%% Returns the {ReplicatingProviderId, EvictingProviderId} pair matching
%% the transfer_test_utils toolkit target conventions for the given
%% transfer type ('undefined' = not taking part).
-spec get_transfer_target_provider_ids(#transfer_test_suite_ctx{}) ->
    {od_provider:id() | undefined, od_provider:id() | undefined}.
get_transfer_target_provider_ids(#transfer_test_suite_ctx{
    transfer_type = TransferType,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}) ->
    CreationProviderId = oct_background:get_provider_id(CreationProviderSelector),
    OtherProviderId = oct_background:get_provider_id(OtherProviderSelector),

    case TransferType of
        replication -> {OtherProviderId, undefined};
        eviction -> {undefined, OtherProviderId};
        migration -> {OtherProviderId, CreationProviderId}
    end.


%% @private
%% Builds a setup fun (run before every test invocation) creating a fresh
%% single-file transfer target along with a bystander file used to verify
%% that the transfer affects nothing beyond its target. A single regular
%% file suffices here - transferring whole directory trees is the transfer
%% machinery suites' concern, not the API's. The created files and the
%% target object id are stored in memory as transfer_details.
-spec build_create_file_transfer_setup_fun(
    #transfer_test_suite_ctx{},
    api_test_memory:mem_ref(),
    atom()
) ->
    onenv_api_test_runner:setup_fun().
build_create_file_transfer_setup_fun(TestSuiteCtx, MemRef, CaseName) ->
    fun() ->
        TargetObject = create_transfer_target_object(TestSuiteCtx, CaseName, <<"file">>),
        BystanderFileObject = create_bystander_file(TestSuiteCtx, CaseName),
        {ok, TargetObjectId} = file_id:guid_to_objectid(TargetObject#object.guid),

        api_test_memory:set(MemRef, transfer_details, #{
            root_file_guid => TargetObject#object.guid,
            root_file_cdmi_id => TargetObjectId,
            transfer_objects => [TargetObject],
            other_objects => [BystanderFileObject]
        })
    end.


%% @private
%% Builds a setup fun (run before every test invocation) creating a fresh
%% set of 1-5 files matched by a newly created view (via an xattr-driven
%% map function) along with a bystander file not matched by it. The view
%% is awaited to emit all the matched files on every provider evaluating
%% it for the suite's transfer type. The created files and the view name
%% are stored in memory as transfer_details.
-spec build_create_view_transfer_setup_fun(
    #transfer_test_suite_ctx{},
    api_test_memory:mem_ref(),
    atom()
) ->
    onenv_api_test_runner:setup_fun().
build_create_view_transfer_setup_fun(TestSuiteCtx, MemRef, CaseName) ->
    fun() ->
        RootDirObject = setup_file_tree_replicas(TestSuiteCtx, CaseName, rand:uniform(5)),
        FileObjects = RootDirObject#object.children,
        BystanderFileObject = create_bystander_file(TestSuiteCtx, CaseName),
        ViewName = create_view_matching_files(TestSuiteCtx, CaseName, FileObjects),

        api_test_memory:set(MemRef, transfer_details, #{
            view_name => ViewName,
            transfer_objects => FileObjects,
            other_objects => [BystanderFileObject]
        })
    end.


%% @private
%% Builds a verify fun asserting the post-invocation block distribution:
%% after a successful transfer the target files must match the suite's
%% transfer type outcome and the bystander files must be untouched; after
%% a failed (unauthorized/forbidden/bad data) one nothing may change.
-spec build_create_transfer_verify_fun(
    #transfer_test_suite_ctx{},
    api_test_memory:mem_ref()
) ->
    onenv_api_test_runner:verify_fun().
build_create_transfer_verify_fun(TestSuiteCtx, MemRef) ->
    fun(ExpTestResult, _ApiTestCtx) ->
        #{
            transfer_objects := TransferObjects,
            other_objects := OtherObjects
        } = api_test_memory:get(MemRef, transfer_details),

        case ExpTestResult of
            expected_failure ->
                transfer_test_utils:assert_initial_distribution(
                    TestSuiteCtx, TransferObjects ++ OtherObjects
                );
            expected_success ->
                transfer_test_utils:assert_distribution(TestSuiteCtx, TransferObjects),
                transfer_test_utils:assert_initial_distribution(TestSuiteCtx, OtherObjects)
        end,
        true
    end.


-spec get_transfer_details(api_test_memory:mem_ref()) -> transfer_details().
get_transfer_details(MemRef) ->
    api_test_memory:get(MemRef, transfer_details).


%% @private
%% Awaits the given transfer reaching the expected end state derived from
%% the transferred file objects stored in transfer_details, attributed to
%% the given user and scheduling node.
-spec await_transfer_ended(
    #transfer_test_suite_ctx{},
    api_test_memory:mem_ref(),
    transfer:id(),
    #api_test_ctx{}
) ->
    ok.
await_transfer_ended(TestSuiteCtx, MemRef, TransferId, #api_test_ctx{
    node = TestNode,
    client = ?USER(UserId)
}) ->
    #{transfer_objects := TransferObjects} = api_test_memory:get(MemRef, transfer_details),

    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, TransferObjects, #{
        user_id => UserId,
        scheduling_provider => opw_test_rpc:get_provider_id(TestNode)
    }).


%% @private
%% Creates a file (random regular file or a directory with 5 files) and
%% starts a transfer of it via the internal API (attributed to the suite
%% ctx user, who must hold the required scheduling privileges), recording
%% the transfer id along with the details needed to assert the returned
%% transfer status as transfer_details. If the caller has installed the
%% file processing gate (transfer_test_utils:mock_gated_file_processing/1)
%% beforehand, the transfer parks mid-flight until permits are granted.
-spec start_file_transfer(
    #transfer_test_suite_ctx{},
    api_test_memory:mem_ref(),
    atom()
) ->
    ok.
start_file_transfer(TestSuiteCtx, MemRef, CaseName) ->
    do_start_file_transfer(
        TestSuiteCtx, MemRef, CaseName, lists_utils:random_element([<<"file">>, <<"dir">>])
    ).


%% @private
%% Like start_file_transfer/3 but always transfers a single regular
%% file, so that the exact number of gated file jobs is known (needed
%% when granting permits in bounded batches, e.g. across reruns).
-spec start_single_file_transfer(
    #transfer_test_suite_ctx{},
    api_test_memory:mem_ref(),
    atom()
) ->
    ok.
start_single_file_transfer(TestSuiteCtx, MemRef, CaseName) ->
    do_start_file_transfer(TestSuiteCtx, MemRef, CaseName, <<"file">>).


%% @private
-spec do_start_file_transfer(
    #transfer_test_suite_ctx{},
    api_test_memory:mem_ref(),
    atom(),
    binary()
) ->
    ok.
do_start_file_transfer(TestSuiteCtx, MemRef, CaseName, RootFileType) ->
    TargetObject = create_transfer_target_object(TestSuiteCtx, CaseName, RootFileType),
    {ok, TargetObjectId} = file_id:guid_to_objectid(TargetObject#object.guid),

    FileObjects = collect_regular_files(TargetObject),
    BaseDetails = #{
        root_file_type => RootFileType,
        root_file_guid => TargetObject#object.guid,
        root_file_path => get_file_path(TestSuiteCtx, TargetObject#object.guid),
        root_file_cdmi_id => TargetObjectId
    },
    start_transfer(
        TestSuiteCtx, MemRef, file, FileObjects,
        #{<<"fileId">> => TargetObjectId}, BaseDetails
    ).


%% @private
%% Like start_file_transfer/3 but the transfer is scheduled by a freshly
%% created view matching 1-5 files (via an xattr-driven map function).
-spec start_view_transfer(
    #transfer_test_suite_ctx{},
    api_test_memory:mem_ref(),
    atom()
) ->
    ok.
start_view_transfer(TestSuiteCtx, MemRef, CaseName) ->
    RootDirObject = setup_file_tree_replicas(TestSuiteCtx, CaseName, rand:uniform(5)),
    FileObjects = RootDirObject#object.children,
    ViewName = create_view_matching_files(TestSuiteCtx, CaseName, FileObjects),

    SpaceId = oct_background:get_space_id(TestSuiteCtx#transfer_test_suite_ctx.space_selector),
    ViewId = get_view_id(TestSuiteCtx, ViewName, SpaceId),
    QueryViewParams = #{<<"descending">> => true},
    BaseDetails = #{
        view_name => ViewName,
        view_id => ViewId,
        query_view_params => QueryViewParams
    },
    start_transfer(
        TestSuiteCtx, MemRef, view, FileObjects,
        #{
            <<"spaceId">> => SpaceId,
            <<"viewName">> => ViewName,
            <<"queryViewParams">> => QueryViewParams
        },
        BaseDetails
    ).


%% @private
%% Awaits the given gated replication being visible with the enqueued
%% replication status on both providers. A fully gated replication never
%% becomes active - activation requires registered progress (processed
%% files, replicated bytes or a finished traverse), all withheld by the
%% gate - so enqueued is its stable mid-flight state. This does not apply
%% to gated evictions, which activate on enqueue already.
-spec await_replication_enqueued(#transfer_test_suite_ctx{}, transfer:id()) -> ok.
await_replication_enqueued(TestSuiteCtx, TransferId) ->
    await_transfer_state(TestSuiteCtx, TransferId, #{replication_status => enqueued}).


-spec await_transfer_end(#transfer_test_suite_ctx{}, transfer:id()) -> ok.
await_transfer_end(#transfer_test_suite_ctx{transfer_type = TransferType} = TestSuiteCtx, TransferId) ->
    ExpStatusFields = case TransferType of
        replication -> #{replication_status => completed};
        eviction -> #{eviction_status => completed};
        migration -> #{replication_status => completed, eviction_status => completed}
    end,
    await_transfer_state(TestSuiteCtx, TransferId, ExpStatusFields).


-spec await_transfer_cancelled(#transfer_test_suite_ctx{}, transfer:id()) -> ok.
await_transfer_cancelled(#transfer_test_suite_ctx{transfer_type = TransferType} = TestSuiteCtx, TransferId) ->
    ExpStatusFields = case TransferType of
        replication -> #{replication_status => cancelled};
        eviction -> #{eviction_status => cancelled};
        % a migration cancelled during its replication phase still ends with
        % both statuses cancelled - the eviction phase never starts
        migration -> #{replication_status => cancelled, eviction_status => cancelled}
    end,
    await_transfer_state(TestSuiteCtx, TransferId, ExpStatusFields).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, transfer_test_utils],
    opt:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "api_tests",
        envs = [
            {op_worker, op_worker, [
                {fuse_session_grace_period_seconds, 24 * 60 * 60},
                {provider_token_ttl_sec, 24 * 60 * 60},
                {dbsync_changes_broadcast_interval, 1000},
                {rerun_transfers, false}
            ]},
            {op_worker, cluster_worker, [
                {cache_to_disk_delay_ms, timer:seconds(1)},
                {cache_to_disk_force_delay_ms, timer:seconds(2)}
            ]}
        ],
        posthook = fun(NewConfig) ->
            start_http_server(),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    stop_http_server(),
    oct_background:end_per_suite().


init_per_testcase(Case, Config) ->
    ct:timetrap({minutes, 30}),
    NewConfig = lfm_proxy:init(Config),

    % the ctx transfer type is irrelevant for the cleanup below
    TestSuiteCtx = build_suite_ctx(replication),
    transfer_test_utils:remove_leftover_file_trees(TestSuiteCtx, Case),
    transfer_test_utils:remove_all_transfers(TestSuiteCtx),
    transfer_test_utils:remove_all_views(TestSuiteCtx),

    NewConfig.


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% HTTP server used for transfer callback
%%%===================================================================


start_http_server() ->
    ensure_inets_started_on_server_node(),
    % sweep leftover instances of previous runs against the same long-lived
    % deployment (an interrupted run never reaches the end_per_suite stop) -
    % the start below would otherwise fail with eaddrinuse
    stop_http_server(),

    % the sweep is synchronous but the stopped instance's listen socket is
    % released asynchronously (typically <15ms, tail of 100ms+ measured on
    % the pod) - retry over the transient eaddrinuse
    ?assertMatch({ok, _}, opw_test_rpc:call(?HTTP_SERVER_PROVIDER_SELECTOR, inets, start, [httpd, [
        {port, ?HTTP_SERVER_PORT},
        {server_name, "httpd_test"},
        {server_root, "/tmp"},
        {document_root, "/tmp"},
        {modules, [?MODULE]}
    ]]), ?HTTP_SERVER_START_ATTEMPTS, ?HTTP_SERVER_START_RETRY_INTERVAL_MS).


stop_http_server() ->
    ensure_inets_started_on_server_node(),
    lists:foreach(fun
        ({httpd, Pid}) ->
            ok = opw_test_rpc:call(?HTTP_SERVER_PROVIDER_SELECTOR, inets, stop, [httpd, Pid]);
        (_) ->
            ok
    end, opw_test_rpc:call(?HTTP_SERVER_PROVIDER_SELECTOR, inets, services, [])).


%% @private
ensure_inets_started_on_server_node() ->
    case opw_test_rpc:call(?HTTP_SERVER_PROVIDER_SELECTOR, inets, start, []) of
        ok -> ok;
        {error, {already_started, inets}} -> ok
    end.


%% NOTE: executed on the http server node
do(#mod{method = "POST", request_uri = ?ENDED_TRANSFERS_PATH, entity_body = Body}) ->
    #{<<"transferId">> := TransferId} = json_utils:decode(Body),
    CallTime = global_clock:timestamp_seconds(),
    node_cache:put(?CALLBACK_CALL_TIME_KEY(TransferId), {called, CallTime}),
    done.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec start_transfer(
    #transfer_test_suite_ctx{},
    api_test_memory:mem_ref(),
    file | view,
    [onenv_file_test_utils:object()],
    map(),
    map()
) ->
    ok.
start_transfer(TestSuiteCtx, MemRef, DataSourceType, FileObjects, DataSourceData, BaseDetails) ->
    Callback = <<"callback">>,
    % The transfer times the tests compare against (scheduleTime etc.) are
    % stamped by provider clocks, which the CT master's clock may be ahead
    % of by over a second - the reference time must come from the provider
    % that will stamp them
    CreationTime = opw_test_rpc:call(
        TestSuiteCtx#transfer_test_suite_ctx.creation_provider_selector,
        global_clock, timestamp_seconds, []
    ),
    TransferId = schedule_transfer_via_api(TestSuiteCtx, DataSourceType, DataSourceData, Callback),

    TotalBytes = lists:sum([byte_size(Content) || #object{content = Content} <- FileObjects]),
    ExpTransfer = build_exp_transfer_stats(TestSuiteCtx, length(FileObjects), TotalBytes),

    api_test_memory:set(MemRef, transfer_details, BaseDetails#{
        transfer_id => TransferId,
        creation_time => CreationTime,
        user_id => oct_background:get_user_id(TestSuiteCtx#transfer_test_suite_ctx.user_selector),
        callback => Callback,
        data_source_type => DataSourceType,
        space_id => oct_background:get_space_id(TestSuiteCtx#transfer_test_suite_ctx.space_selector),
        src_provider_id => oct_background:get_provider_id(
            TestSuiteCtx#transfer_test_suite_ctx.creation_provider_selector
        ),
        transfer_objects => FileObjects,
        exp_transfer => ExpTransfer
    }).


%% @private
-spec schedule_transfer_via_api(#transfer_test_suite_ctx{}, file | view, map(), binary()) ->
    transfer:id().
schedule_transfer_via_api(#transfer_test_suite_ctx{
    transfer_type = TransferType,
    space_selector = SpaceSelector,
    user_selector = UserSelector,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
} = TestSuiteCtx, DataSourceType, DataSourceData, Callback) ->
    SpaceId = oct_background:get_space_id(SpaceSelector),
    {ReplicatingProviderId, EvictingProviderId} = get_transfer_target_provider_ids(TestSuiteCtx),
    ProviderData = maps:filter(fun(_, V) -> V =/= undefined end, #{
        <<"replicatingProviderId">> => ReplicatingProviderId,
        <<"evictingProviderId">> => EvictingProviderId
    }),
    Data = maps:merge(DataSourceData, ProviderData#{
        <<"type">> => atom_to_binary(TransferType, utf8),
        <<"dataSourceType">> => atom_to_binary(DataSourceType, utf8),
        <<"spaceId">> => SpaceId,
        <<"callback">> => Callback
    }),

    UserId = oct_background:get_user_id(UserSelector),
    SessionId = oct_background:get_user_session_id(UserSelector, CreationProviderSelector),
    Req = #op_req{
        auth = ?USER(UserId, SessionId),
        gri = #gri{type = op_transfer, aspect = instance},
        operation = create,
        data = Data
    },
    {ok, resource, {#gri{id = TransferId}, _}} = ?assertMatch(
        {ok, _, _},
        opw_test_rpc:call(CreationProviderSelector, middleware, handle, [Req])
    ),
    % Wait for transfer doc sync with the other provider
    ?assertMatch({ok, _}, opw_test_rpc:call(OtherProviderSelector, transfer, get, [TransferId]), ?ATTEMPTS),
    TransferId.


%% @private
-spec build_exp_transfer_stats(#transfer_test_suite_ctx{}, non_neg_integer(), non_neg_integer()) ->
    map().
build_exp_transfer_stats(TestSuiteCtx, FilesCount, TotalBytes) ->
    {ReplicatingProviderId, EvictingProviderId} = get_transfer_target_provider_ids(TestSuiteCtx),
    case TestSuiteCtx#transfer_test_suite_ctx.transfer_type of
        replication ->
            #{
                replication_status => completed, eviction_status => skipped,
                replicating_provider => ReplicatingProviderId, evicting_provider => undefined,
                files_to_process => FilesCount, files_processed => FilesCount,
                files_replicated => FilesCount, bytes_replicated => TotalBytes, files_evicted => 0
            };
        eviction ->
            #{
                replication_status => skipped, eviction_status => completed,
                replicating_provider => undefined, evicting_provider => EvictingProviderId,
                files_to_process => FilesCount, files_processed => FilesCount,
                files_replicated => 0, bytes_replicated => 0, files_evicted => FilesCount
            };
        migration ->
            #{
                replication_status => completed, eviction_status => completed,
                replicating_provider => ReplicatingProviderId, evicting_provider => EvictingProviderId,
                files_to_process => 2 * FilesCount, files_processed => 2 * FilesCount,
                files_replicated => FilesCount, bytes_replicated => TotalBytes, files_evicted => FilesCount
            }
    end.


%% @private
-spec await_transfer_state(#transfer_test_suite_ctx{}, transfer:id(), map()) -> ok.
await_transfer_state(#transfer_test_suite_ctx{
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}, TransferId, ExpStatusFields) ->
    lists:foreach(fun(ProviderSelector) ->
        ?assertEqual(ExpStatusFields, case opw_test_rpc:call(
            ProviderSelector, transfer, get, [TransferId]
        ) of
            {ok, #document{value = Transfer}} ->
                maps:map(fun
                    (replication_status, _) -> Transfer#transfer.replication_status;
                    (eviction_status, _) -> Transfer#transfer.eviction_status
                end, ExpStatusFields);
            {error, _} = Error ->
                Error
        end, ?ATTEMPTS)
    end, [CreationProviderSelector, OtherProviderSelector]).


%% @private
-spec get_view_id(#transfer_test_suite_ctx{}, index:name(), od_space:id()) -> binary().
get_view_id(#transfer_test_suite_ctx{creation_provider_selector = CreationProviderSelector}, ViewName, SpaceId) ->
    {ok, ViewId} = ?assertMatch({ok, _}, opw_test_rpc:call(
        CreationProviderSelector, view_links, get_view_id, [ViewName, SpaceId]
    )),
    ViewId.


%% @private
-spec get_file_path(#transfer_test_suite_ctx{}, file_id:file_guid()) -> file_meta:path().
get_file_path(#transfer_test_suite_ctx{
    user_selector = UserSelector,
    creation_provider_selector = CreationProviderSelector
}, FileGuid) ->
    Node = oct_background:get_random_provider_node(CreationProviderSelector),
    SessionId = oct_background:get_user_session_id(UserSelector, CreationProviderSelector),
    {ok, FilePath} = ?assertMatch({ok, _}, lfm_proxy:get_file_path(Node, SessionId, FileGuid)),
    FilePath.


%% @private
-spec collect_regular_files(onenv_file_test_utils:object()) -> [onenv_file_test_utils:object()].
collect_regular_files(#object{type = ?REGULAR_FILE_TYPE} = FileObject) ->
    [FileObject];
collect_regular_files(#object{type = ?DIRECTORY_TYPE, children = Children}) ->
    lists:flatmap(fun collect_regular_files/1, Children).


%% @private
-spec setup_file_tree_replicas(#transfer_test_suite_ctx{}, atom(), pos_integer()) ->
    onenv_file_test_utils:object().
setup_file_tree_replicas(TestSuiteCtx, CaseName, FilesCount) ->
    RootDirObject = transfer_test_utils:create_file_tree(
        TestSuiteCtx, CaseName, #dir_spec{children = [
            #file_spec{content = ?RAND_CONTENT()} || _ <- lists:seq(1, FilesCount)
        ]}
    ),
    transfer_test_utils:ensure_initial_replicas(TestSuiteCtx, RootDirObject),
    RootDirObject.


%% @private
%% The transfer target is either the single regular file of a fresh tree or
%% the whole 5-file tree root directory, depending on the given root file type.
-spec create_transfer_target_object(#transfer_test_suite_ctx{}, atom(), binary()) ->
    onenv_file_test_utils:object().
create_transfer_target_object(TestSuiteCtx, CaseName, <<"file">>) ->
    RootDirObject = setup_file_tree_replicas(TestSuiteCtx, CaseName, 1),
    hd(RootDirObject#object.children);
create_transfer_target_object(TestSuiteCtx, CaseName, <<"dir">>) ->
    setup_file_tree_replicas(TestSuiteCtx, CaseName, 5).


%% @private
%% Creates a file not taking part in the transfer, used to verify that the
%% transfer affects nothing beyond its target.
-spec create_bystander_file(#transfer_test_suite_ctx{}, atom()) ->
    onenv_file_test_utils:object().
create_bystander_file(TestSuiteCtx, CaseName) ->
    RootDirObject = setup_file_tree_replicas(TestSuiteCtx, CaseName, 1),
    hd(RootDirObject#object.children).


%% @private
%% Creates a view matching exactly the given files (via an xattr set on each
%% of them and an xattr-driven map function) and awaits it emitting all of
%% them.
-spec create_view_matching_files(
    #transfer_test_suite_ctx{},
    atom(),
    [onenv_file_test_utils:object()]
) ->
    index:name().
create_view_matching_files(TestSuiteCtx, CaseName, FileObjects) ->
    XattrName = transfer_test_utils:rand_xattr_name(CaseName),
    lists:foreach(fun(#object{guid = FileGuid}) ->
        set_xattr(TestSuiteCtx, FileGuid, XattrName, ?VIEW_XATTR_VALUE)
    end, FileObjects),

    ViewName = transfer_test_utils:rand_view_name(CaseName),
    transfer_test_utils:create_view(
        TestSuiteCtx, ViewName,
        transfer_test_utils:gen_view_map_function(XattrName), undefined, []
    ),
    ExpObjectIds = lists:map(fun(#object{guid = FileGuid}) ->
        {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
        ObjectId
    end, FileObjects),
    transfer_test_utils:await_view_query_result(
        TestSuiteCtx, ViewName, [{key, ?VIEW_XATTR_VALUE}, {stale, false}], ExpObjectIds
    ),
    ViewName.


%% @private
%% Sets the xattr on the other provider - the one the views are evaluated
%% on - so that view emissions do not wait for metadata dbsync.
-spec set_xattr(#transfer_test_suite_ctx{}, file_id:file_guid(), binary(), term()) ->
    ok.
set_xattr(#transfer_test_suite_ctx{
    other_provider_selector = OtherProviderSelector
}, FileGuid, XattrName, XattrValue) ->
    OtherNode = oct_background:get_random_provider_node(OtherProviderSelector),
    file_test_utils:set_xattr(OtherNode, FileGuid, XattrName, XattrValue).


%% @private
get_callback_url() ->
    ServerNodeIp = opw_test_rpc:call(
        ?HTTP_SERVER_PROVIDER_SELECTOR, initializer, local_ip_v4, []
    ),
    {ok, IpAddressBin} = ip_utils:to_binary(ServerNodeIp),
    PortBin = integer_to_binary(?HTTP_SERVER_PORT),

    <<"http://", IpAddressBin/binary, ":" , PortBin/binary, ?ENDED_TRANSFERS_PATH>>.


%% @private
replace_placeholder_value(Key, Value, Data) ->
    case maps:get(Key, Data, undefined) of
        ?PLACEHOLDER ->
            Data#{Key => Value};
        _ ->
            Data
    end.
