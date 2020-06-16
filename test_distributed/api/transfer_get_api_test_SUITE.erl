%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning transfer get API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_get_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_test_runner.hrl").
-include("transfer_api_test_utils.hrl").
-include("../transfers_test_mechanism.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    get_file_replication_status/1,
    get_file_eviction_status/1,
    get_file_migration_status/1,

    get_view_replication_status/1,
    get_view_eviction_status/1,
    get_view_migration_status/1
]).

all() ->
    ?ALL([
        get_file_replication_status,
        get_file_eviction_status,
        get_file_migration_status,

        get_view_replication_status,
        get_view_eviction_status,
        get_view_migration_status
    ]).


-define(PROVIDER_GRI_ID(__PROVIDER_ID), gri:serialize(#gri{
    type = op_provider,
    id = __PROVIDER_ID,
    aspect = instance,
    scope = protected
})).

-define(TRANSFER_PROLONGATION_TIME, 2).


%%%===================================================================
%%% Test functions
%%%===================================================================


get_file_replication_status(Config) ->
    get_transfer_status_test_base(Config, replication, file).


get_file_eviction_status(Config) ->
    get_transfer_status_test_base(Config, eviction, file).


get_file_migration_status(Config) ->
    get_transfer_status_test_base(Config, migration, file).


get_view_replication_status(Config) ->
    get_transfer_status_test_base(Config, replication, view).


get_view_eviction_status(Config) ->
    get_transfer_status_test_base(Config, eviction, view).


get_view_migration_status(Config) ->
    get_transfer_status_test_base(Config, migration, view).


%% @private
get_transfer_status_test_base(Config, TransferType, DataSourceType) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),

    RequiredPrivs = [?SPACE_VIEW_TRANSFERS],
    set_space_privileges(Providers, ?SPACE_2, ?USER_IN_SPACE_2, privileges:space_admin() -- RequiredPrivs),
    set_space_privileges(Providers, ?SPACE_2, ?USER_IN_BOTH_SPACES, RequiredPrivs),

    % Start transfer and check returned stats for ongoing transfer (transfer is prolonged via mock)
    EnvRef = api_test_utils:init_env(),
    SetupEnvFun = transfer_api_test_utils:create_setup_transfer_env_with_started_transfer_fun(
        TransferType, EnvRef, DataSourceType, P1, P2, ?USER_IN_SPACE_2, Config
    ),
    SetupEnvFun(),
    {ok, #{transfer_id := TransferId} = TransferDetails} = api_test_utils:get_env_var(
        EnvRef, transfer_details
    ),

    get_transfer_status_test_base(Config, TransferType, DataSourceType, TransferDetails, ongoing),
    transfer_api_test_utils:await_transfer_end(Providers, TransferId, TransferType),
    get_transfer_status_test_base(Config, TransferType, DataSourceType, TransferDetails, ended).


%% @private
get_transfer_status_test_base(Config, TransferType, DataSourceType, Env, ExpState) ->
    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_TRANSFER_SCENARIOS(Config),
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Get transfer (~p) status using rest endpoint", [TransferType]),
                    type = rest,
                    prepare_args_fun = create_prepare_transfer_get_status_rest_args_fun(Env),
                    validate_result_fun = create_validate_transfer_get_status_rest_call_result_fun(
                        TransferType, DataSourceType, ExpState, Env
                    )
                },
                #scenario_template{
                    name = str_utils:format("Get transfer (~p) status using gs transfer api", [TransferType]),
                    type = gs,
                    prepare_args_fun = create_prepare_transfer_get_status_gs_args_fun(Env),
                    validate_result_fun = create_validate_transfer_get_status_gs_call_result_fun(DataSourceType, ExpState, Env)
                }
            ],
            data_spec = get_transfer_status_spec()
        }
    ])).


%% @private
get_transfer_status_spec() ->
    #data_spec{bad_values = [
        {bad_id, <<"NonExistentTransfer">>, ?ERROR_NOT_FOUND}
    ]}.


%% @private
create_prepare_transfer_get_status_rest_args_fun(#{transfer_id := TransferId}) ->
    fun(#api_test_ctx{data = Data}) ->
        {Id, _} = api_test_utils:maybe_substitute_id(TransferId, Data),

        #rest_args{
            method = get,
            path = <<"transfers/", Id/binary>>
        }
    end.


%% @private
create_prepare_transfer_get_status_gs_args_fun(#{transfer_id := TransferId}) ->
    fun(#api_test_ctx{data = Data}) ->
        {Id, _} = api_test_utils:maybe_substitute_id(TransferId, Data),

        #gs_args{
            operation = get,
            gri = #gri{type = op_transfer, id = Id, aspect = instance}
        }
    end.


%% @private
create_validate_transfer_get_status_rest_call_result_fun(TransferType, DataSourceType, ExpState, Env) ->
    AlwaysPresentFields = [
        <<"type">>, <<"dataSourceType">>,
        <<"userId">>, <<"rerunId">>, <<"spaceId">>, <<"callback">>,

        <<"replicatingProviderId">>, <<"evictingProviderId">>,
        <<"transferStatus">>, <<"replicationStatus">>, <<"replicaEvictionStatus">>, <<"evictionStatus">>,

        <<"filesToProcess">>, <<"filesProcessed">>,
        <<"filesReplicated">>, <<"bytesReplicated">>,
        <<"fileReplicasEvicted">>, <<"filesEvicted">>,
        <<"failedFiles">>, <<"failesFailed">>,

        <<"scheduleTime">>, <<"startTime">>, <<"finishTime">>,
        <<"lastUpdate">>, <<"minHist">>, <<"hrHist">>, <<"dyHist">>, <<"mthHist">>
    ],
    AllFields = case DataSourceType of
        file -> [<<"fileId">>, <<"path">>, <<"filePath">> | AlwaysPresentFields];
        view -> [<<"viewName">>, <<"queryViewParams">> | AlwaysPresentFields]
    end,
    AllFieldsSorted = lists:sort(AllFields),

    fun(_TestCtx, Result) ->
        {ok, _, _, Body} = ?assertMatch({ok, ?HTTP_200_OK, _, _}, Result),
        ?assertMatch(AllFieldsSorted, lists:sort(maps:keys(Body))),

        assert_proper_constant_fields_in_get_status_rest_response(TransferType, DataSourceType, Env, Body),
        assert_proper_status_in_get_status_rest_response(ExpState, Env, Body),
        assert_proper_file_stats_in_get_status_rest_response(ExpState, Env, Body),
        assert_proper_histograms_in_get_status_rest_response(TransferType, ExpState, Env, Body),

        CreationTime = maps:get(creation_time, Env),
        Now = time_utils:system_time_millis() div 1000,
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
        <<"spaceId">> => ?SPACE_2,
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
                <<"path">> => maps:get(root_file_path, Env),
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
    <<"replicationStatus">> := ReplicationStatus,
    <<"replicaEvictionStatus">> := EvictionStatus,
    <<"evictionStatus">> := EvictionStatus
}) ->
    #{
        replication_status := ExpReplicationStatus,
        eviction_status := ExpEvictionStatus
    } = maps:get(exp_transfer, Env),

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
    <<"fileReplicasEvicted">> := FilesEvicted,
    <<"filesEvicted">> := FilesEvicted,
    <<"failedFiles">> := FailedFiles,
    <<"failesFailed">> := FailedFiles
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
assert_proper_histograms_in_get_status_rest_response(_TransferType, ExpState, #{
    src_node := SrcNode,
    exp_transfer := #{bytes_replicated := BytesReplicated}
}, Data) ->
    SrcProviderId = transfers_test_utils:provider_id(SrcNode),

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
create_validate_transfer_get_status_gs_call_result_fun(DataSourceType, ExpState, #{
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

    fun(_, Result) ->
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
                Now = time_utils:system_time_millis() div 1000,

                ?assert(not IsOngoing),
                ?assert(CreationTime =< ScheduleTime),
                ?assert(ScheduleTime =< StartTime),
                ?assert(StartTime =< FinishTime),
                ?assert(FinishTime =< Now)
        end
    end.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    api_test_utils:load_module_from_test_distributed_dir(Config, transfers_test_utils),

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
        application:start(ssl),
        hackney:start(),
        NewConfig3 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig2, "env_desc.json"),
            NewConfig2
        ),
        initializer:mock_auth_manager(NewConfig3, _CheckIfUserIsSupported = true),
        NewConfig3
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, transfers_test_utils]} | Config].


end_per_suite(Config) ->
    hackney:stop(),
    application:stop(ssl),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:teardown_storage(Config).


init_per_testcase(Case, Config) when
    Case =:= get_file_replication_status;
    Case =:= get_file_migration_status;
    Case =:= get_view_replication_status;
    Case =:= get_view_migration_status
->
    Providers = ?config(op_worker_nodes, Config),
    transfers_test_utils:mock_prolonged_replication(Providers, 0.5, ?TRANSFER_PROLONGATION_TIME),
    init_per_testcase(all, Config);

init_per_testcase(Case, Config) when
    Case =:= get_file_eviction_status;
    Case =:= get_view_eviction_status
->
    Providers = ?config(op_worker_nodes, Config),
    transfers_test_utils:mock_prolonged_replica_eviction(Providers, 0.5, ?TRANSFER_PROLONGATION_TIME),
    init_per_testcase(all, Config);

init_per_testcase(_Case, Config) ->
    initializer:mock_share_logic(Config),
    ct:timetrap({minutes, 30}),
    lfm_proxy:init(Config).


end_per_testcase(Case, Config) when
    Case =:= get_file_replication_status;
    Case =:= get_file_migration_status;
    Case =:= get_view_replication_status;
    Case =:= get_view_migration_status
->
    Providers = ?config(op_worker_nodes, Config),
    transfers_test_utils:unmock_prolonged_replication(Providers),
    end_per_testcase(all, Config);

end_per_testcase(Case, Config) when
    Case =:= get_file_eviction_status;
    Case =:= get_view_eviction_status
->
    Providers = ?config(op_worker_nodes, Config),
    transfers_test_utils:unmock_prolonged_replica_eviction(Providers),
    end_per_testcase(all, Config);

end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
set_space_privileges(Nodes, SpaceId, UserId, Privileges) ->
    initializer:testmaster_mock_space_user_privileges(
        Nodes, SpaceId, UserId, Privileges
    ).
