%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests for space unsupport.
%%% @end
%%%--------------------------------------------------------------------
-module(space_unsupport_test_SUITE).
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
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
    replicate_stage_test/1,
    replicate_stage_persistence_test/1,
    cleanup_traverse_stage_test/1,
    cleanup_traverse_stage_with_import_test/1,
    cleanup_traverse_stage_persistence_test/1,
    delete_synced_documents_stage_test/1,
    delete_local_documents_stage_test/1,
    overall_test/1
]).

% Exported for rpc calls
-export([get_keys/1]).

all() -> [
    replicate_stage_test,
    replicate_stage_persistence_test,
    cleanup_traverse_stage_test,
    cleanup_traverse_stage_with_import_test,
    cleanup_traverse_stage_persistence_test,
    delete_synced_documents_stage_test,
    delete_local_documents_stage_test,
    overall_test
].

-define(SPACE_ID, <<"space1">>).
-define(TASK_ID, <<"task_id">>).
-define(TEST_DATA, <<"test_data">>).
-define(ATTEMPTS, 60).

%%%===================================================================
%%% Test functions
%%%===================================================================

replicate_stage_test(Config) ->
    [Worker1, Worker2] = Workers = ?config(op_worker_nodes, Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_ID),
    StorageId = initializer:get_supporting_storage_id(Worker1, ?SPACE_ID),
    
    {{DirGuid, _}, {G1, _}, {G2, _}} = create_files_and_dirs(Worker1, SessId),
    StageJob = #space_unsupport_job{
        stage = replicate,
        space_id = ?SPACE_ID,
        storage_id = StorageId
    },
    
    Promise = rpc:async_call(Worker1, space_unsupport, do_slave_job, [StageJob, ?TASK_ID]),
    
    {ok, {EntriesMap, _}} = ?assertMatch({ok, {Map, _}} when map_size(Map) =/= 0, 
        lfm_proxy:get_effective_file_qos(Worker1, SessId(Worker1), {guid, SpaceGuid}), 
        ?ATTEMPTS),
    [QosEntryId] = maps:keys(EntriesMap),
    
    % check that space unsupport QoS entry cannot be deleted
    ?assertEqual({error, ?EACCES}, lfm_proxy:remove_qos_entry(Worker1, SessId(Worker1), QosEntryId)),
    ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(Worker1, SessId(Worker1), QosEntryId)),
    
    ok = rpc:yield(Promise),
    
    ?assertMatch(?ERROR_NOT_FOUND, lfm_proxy:get_qos_entry(Worker1, SessId(Worker1), QosEntryId)),
    
    Size = size(?TEST_DATA),
    check_distribution(Workers, SessId, [{Worker1, Size}, {Worker2, Size}], G1),
    check_distribution(Workers, SessId, [{Worker1, Size}, {Worker2, Size}], G2),
    
    ok = lfm_proxy:unlink(Worker1, SessId(Worker1), {guid, G1}),
    ok = lfm_proxy:unlink(Worker1, SessId(Worker1), {guid, G2}),
    ok = lfm_proxy:unlink(Worker1, SessId(Worker1), {guid, DirGuid}).


replicate_stage_persistence_test(Config) ->
    [Worker1, _Worker2] = ?config(op_worker_nodes, Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_ID),
    StorageId = initializer:get_supporting_storage_id(Worker1, ?SPACE_ID),
    
    % Create new QoS entry representing entry created before provider restart.
    % Running stage again with existing entry should not create new one, 
    % but wait for fulfillment of previous one.
    Expression = <<?QOS_ANY_STORAGE, "\\ storageId = ", StorageId/binary>>,
    {ok, QosEntryId} = create_qos_entry(Worker1, SessId, SpaceGuid, Expression),
    
    StageJob = #space_unsupport_job{
        stage = replicate,
        space_id = ?SPACE_ID,
        storage_id = StorageId,
        subtask_id = QosEntryId
    },
    
    test_utils:mock_new(Worker1, qos_entry, [passthrough]),
    ok = rpc:call(Worker1, space_unsupport, do_slave_job, [StageJob, ?TASK_ID]),
    % check that no additional entry was created
    test_utils:mock_assert_num_calls(Worker1, qos_entry, create, 5, 0, 1),
    test_utils:mock_assert_num_calls(Worker1, qos_entry, create, 7, 0, 1),
    test_utils:mock_unload(Worker1, [qos_entry]),
    
    ?assertMatch(?ERROR_NOT_FOUND, lfm_proxy:get_qos_entry(Worker1, SessId(Worker1), QosEntryId)).


cleanup_traverse_stage_test(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    
    Worker = select_provider_without_imported_storage(Workers),
    StorageId = initializer:get_supporting_storage_id(Worker, ?SPACE_ID),
    {{DirGuid, DirPath}, {G1, F1Path}, {G2, F2Path}} = create_files_and_dirs(Worker, SessId),
    
    % Relative paths to space dir. Empty binary represents space dir.
    AllPaths = [<<"">>, DirPath, F1Path, F2Path],
    AllPathsWithoutSpace = [DirPath, F1Path, F2Path],
    check_files_on_storage(Worker, AllPaths, true, false),
    
    StageJob = #space_unsupport_job{
        stage = cleanup_traverse,
        space_id = ?SPACE_ID,
        storage_id = StorageId
    },
    
    ok = rpc:call(Worker, space_unsupport, do_slave_job, [StageJob, ?TASK_ID]),
    rpc:call(Worker, unsupport_cleanup_traverse, delete_ended, [?SPACE_ID, StorageId]),
    check_files_on_storage(Worker, AllPathsWithoutSpace, false, false),
    assert_storage_cleaned_up(Worker, StorageId),
    check_distribution(Workers, SessId, [], G1),
    check_distribution(Workers, SessId, [], G2),
    
    ok = lfm_proxy:unlink(Worker, SessId(Worker), {guid, G1}),
    ok = lfm_proxy:unlink(Worker, SessId(Worker), {guid, G2}),
    ok = lfm_proxy:unlink(Worker, SessId(Worker), {guid, DirGuid}).


cleanup_traverse_stage_with_import_test(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    Worker = select_provider_with_imported_storage(Workers),
    StorageId = initializer:get_supporting_storage_id(Worker, ?SPACE_ID),
    
    {{DirGuid, DirPath}, {G1, F1Path}, {G2, F2Path}} = create_files_and_dirs(Worker, SessId),
    ok = rpc:call(Worker, storage_sync, configure_import, [?SPACE_ID, true, #{max_depth => 5, sync_acl => true}]),
    
    % Relative paths to space dir. Empty binary represents space dir.
    AllPaths = [<<"">>, DirPath, F1Path, F2Path],
    check_files_on_storage(Worker, AllPaths, true, true),
    
    StageJob = #space_unsupport_job{
        stage = cleanup_traverse,
        space_id = ?SPACE_ID,
        storage_id = StorageId
    },
    
    ok = rpc:call(Worker, space_unsupport, do_slave_job, [StageJob, ?TASK_ID]),
    % do not delete ended unsupport_cleanup_traverse - it will be deleted in `delete_local_documents_stage_test`
    
    check_files_on_storage(Worker, AllPaths, true, true),
    check_distribution(Workers, SessId, [], G1),
    check_distribution(Workers, SessId, [], G2),
    
    ok = lfm_proxy:unlink(Worker, SessId(Worker), {guid, G1}),
    ok = lfm_proxy:unlink(Worker, SessId(Worker), {guid, G2}),
    ok = lfm_proxy:unlink(Worker, SessId(Worker), {guid, DirGuid}).


cleanup_traverse_stage_persistence_test(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    Worker = select_provider_without_imported_storage(Workers),
    StorageId = initializer:get_supporting_storage_id(Worker, ?SPACE_ID),
    
    % Create new cleanup traverse representing traverse created before provider restart.
    % Running stage again with existing traverse should not create new one, 
    % but wait for previous one to finish.
    {ok, TaskId} = rpc:call(Worker, unsupport_cleanup_traverse, start, [?SPACE_ID, StorageId]),
    
    StageJob = #space_unsupport_job{
        stage = cleanup_traverse,
        space_id = ?SPACE_ID,
        storage_id = StorageId,
        subtask_id = TaskId
    },
    
    ok = rpc:call(Worker, space_unsupport, do_slave_job, [StageJob, ?TASK_ID]),
    rpc:call(Worker, unsupport_cleanup_traverse, delete_ended, [?SPACE_ID, StorageId]),
    % check that no additional traverse was started
    test_utils:mock_assert_num_calls(Worker, unsupport_cleanup_traverse, start, 1, 0, 1).


delete_synced_documents_stage_test(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    % TODO VFS-6135 before stopping dbsync during unsupport is implemented run this stage on the 
    % same worker as the `overall_test` is ran on. Otherwise running this stage may 
    % cause dbsync hooks to create documents for files, which were deleted long ago (e.g file_meta_posthooks), 
    % and these documents will never be deleted resulting in a failure of the aforementioned test.
    Worker1 = select_provider_without_imported_storage(Workers),
    Worker2 = select_provider_with_imported_storage(Workers),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    StorageId = initializer:get_supporting_storage_id(Worker1, ?SPACE_ID),
    
    % create synced documents on remote provider
    {{DirGuid,_}, {G1, _}, {G2, _}} = create_files_and_dirs(Worker2, SessId),
    {ok, _} = create_qos_entry(Worker2, SessId, DirGuid, <<"key=value">>),
    {ok, _} = create_replication(Worker2, SessId, G2, Worker1),
    {ok, _} = create_eviction(Worker2, SessId, G1, Worker1),
    ok = create_view(Worker2, ?SPACE_ID, Worker1),
    ok = create_custom_metadata(Worker2, SessId, G1),
    
    StageJob = #space_unsupport_job{
        stage = delete_synced_documents,
        space_id = ?SPACE_ID,
        storage_id = StorageId
    },
    
    % wait for documents to be dbsynced and saved in database
    timer:sleep(timer:seconds(60)),
    
    ok = rpc:call(Worker1, space_unsupport, do_slave_job, [StageJob, ?TASK_ID]),
    
    % wait for documents to expire 
    timer:sleep(timer:seconds(70)),
    
    assert_synced_documents_cleaned_up(Worker1, ?SPACE_ID),
    
    ok = lfm_proxy:unlink(Worker2, SessId(Worker2), {guid, G1}),
    ok = lfm_proxy:unlink(Worker2, SessId(Worker2), {guid, G2}),
    ok = lfm_proxy:unlink(Worker2, SessId(Worker2), {guid, DirGuid}).
    

delete_local_documents_stage_test(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    % select provider with imported storage because storage sync is being enabled later on
    Worker = select_provider_with_imported_storage(Workers),
    StorageId = initializer:get_supporting_storage_id(Worker, ?SPACE_ID),
    ok = rpc:call(Worker, storage_sync, configure_import, [?SPACE_ID, true, #{max_depth => 5, sync_acl => true}]),
    ok = rpc:call(Worker, storage_sync_worker, schedule_spaces_check, [0]),
    ok = rpc:call(Worker, file_popularity_api, enable, [?SPACE_ID]),
    ACConfig =  #{
        enabled => true,
        target => 0,
        threshold => 100
    },
    ok = rpc:call(Worker, autocleaning_api, configure, [?SPACE_ID, ACConfig]),
    
    ?assertMatch({ok, _}, rpc:call(Worker, space_strategies, get, [?SPACE_ID])),
    ?assertMatch({ok, _}, rpc:call(Worker, storage_sync_monitoring, get, [?SPACE_ID, StorageId]), 10),
    ?assertMatch({ok, _}, rpc:call(Worker, autocleaning, get, [?SPACE_ID])),
    ?assertMatch({ok, _}, rpc:call(Worker, file_popularity_config, get, [?SPACE_ID])),
    ?assertMatch(true, rpc:call(Worker, file_popularity_api, is_enabled, [?SPACE_ID])),
    % wait for storage sync to finish
    ?assertEqual(finished, rpc:call(Worker, storage_sync_monitoring, get_import_status, [?SPACE_ID, StorageId]), 30),
    
    StageJob = #space_unsupport_job{
        stage = delete_local_documents,
        space_id = ?SPACE_ID,
        storage_id = StorageId
    },
    ok = rpc:call(Worker, space_unsupport, do_slave_job, [StageJob, ?TASK_ID]),
    
    assert_local_documents_cleaned_up(Worker, ?SPACE_ID, StorageId).


overall_test(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    Worker = select_provider_without_imported_storage(Workers),
    StorageId = initializer:get_supporting_storage_id(Worker, ?SPACE_ID),
    
    create_files_and_dirs(Worker, SessId),
    ok = rpc:call(Worker, space_unsupport, run, [?SPACE_ID, StorageId]),
    
    % mocked in init_per_testcase
    receive task_finished -> ok end,
    
    % check that all stages have been executed
    lists:foreach(fun(Stage) ->
        test_utils:mock_assert_num_calls(
            Worker, space_unsupport, do_slave_job,
            [{space_unsupport_job, Stage, '_', ?SPACE_ID, StorageId, '_', '_'}, '_'],
            1, 1
        )
    end, space_unsupport:get_all_stages()),
    
    % check that stages have been executed in correct order 
    % (sending message mocked in init_per_testcase)
    lists:foldl(fun(Stage, CallNum) ->
        receive {Stage, Num} ->
            ct:pal("Stage: ~p - ~p", [Stage, Num]),
            ?assertEqual(CallNum, Num)
        end,
        CallNum + 1
    end, 0, space_unsupport:get_all_stages()),
    
    % wait for documents to expire 
    timer:sleep(timer:seconds(70)),

    assert_storage_cleaned_up(Worker, StorageId),
    assert_synced_documents_cleaned_up(Worker, ?SPACE_ID),
    assert_local_documents_cleaned_up(Worker).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        hackney:start(),
        application:start(ssl),
        initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), NewConfig)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, pool_utils]} | Config].


end_per_suite(Config) ->
    hackney:stop(),
    application:stop(ssl),
    initializer:clean_test_users_and_spaces_no_validate(Config).


init_per_testcase(cleanup_traverse_stage_persistence_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, unsupport_cleanup_traverse, [passthrough]),
    test_utils:mock_expect(Workers, unsupport_cleanup_traverse, start, fun(SpaceId, StorageId) ->
        % delay traverse start, so it does not finish before stage is even called
        timer:sleep(timer:seconds(5)), 
        meck:passthrough([SpaceId, StorageId])
    end),
    init_per_testcase(default, Config);
init_per_testcase(overall_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    Self = self(),
    test_utils:mock_new(Workers, space_unsupport, [passthrough]),
    test_utils:mock_expect(Workers, space_unsupport, do_slave_job,
        fun(#space_unsupport_job{stage = Stage} = Job, TaskId) ->
            Self ! {Stage, meck:num_calls(space_unsupport, do_slave_job, 2)},
            meck:passthrough([Job, TaskId])
        end),
    test_utils:mock_expect(Workers, space_unsupport, task_finished, 
        fun(TaskId, Pool) ->
            Self ! task_finished,
            meck:passthrough([TaskId, Pool])
        end),
    init_per_testcase(default, Config);
init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_ID),
    lists:foreach(fun(Worker) ->
        ?assertEqual({ok, []}, lfm_proxy:get_children(Worker, <<"0">>, {guid, SpaceGuid}, 0, 10), ?ATTEMPTS)
    end, Workers),
    ct:timetrap({minutes, 30}),
    lfm_proxy:init(Config).


end_per_testcase(cleanup_traverse_stage_persistence_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [unsupport_cleanup_traverse]),
    end_per_testcase(default, Config);
end_per_testcase(overall_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [space_unsupport]),
    end_per_testcase(default, Config);
end_per_testcase(_, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Assertion helper functions
%%%===================================================================

assert_storage_cleaned_up(Worker, StorageId) ->
    Path = storage_mount_point(Worker, StorageId),
    {ok, FileList} = rpc:call(Worker, file, list_dir_all, [Path]),
    ?assertEqual([], FileList).


assert_local_documents_cleaned_up(Worker, SpaceId, StorageId) ->
    ?assertEqual({error, not_found}, rpc:call(Worker, space_strategies, get, [SpaceId])),
    ?assertEqual({error, not_found}, rpc:call(Worker, storage_sync_monitoring, get, [SpaceId, StorageId])),
    ?assertEqual(undefined, rpc:call(Worker, autocleaning, get_config, [SpaceId])),
    ?assertEqual({error, not_found}, rpc:call(Worker, autocleaning, get, [SpaceId])),
    ?assertEqual(false, rpc:call(Worker, file_popularity_api, is_enabled, [SpaceId])),
    ?assertEqual({error, not_found}, rpc:call(Worker, file_popularity_config, get, [SpaceId])),
    ?assertMatch({ok, [], _}, rpc:call(Worker, traverse_task_list, list, [<<"unsupport_cleanup_traverse">>, ended])).


assert_local_documents_cleaned_up(Worker) ->
    AllModels = datastore_config_plugin:get_models(),
    ModelsToCheck = AllModels
        -- [storage_config, provider_auth, % Models not associated with space support
            file_meta, times, %% These documents without scope are related to user root dir,
                              %% which is not cleaned up during unsupport
            dbsync_state,
            file_local_blocks, %% @TODO VFS-6275 check after file_local_blocks cleanup is properly implemented
            luma_db % These documents are associated with storage, not with space support
        ],
    assert_documents_cleaned_up(Worker, <<>>, ModelsToCheck).


assert_synced_documents_cleaned_up(Worker, SpaceId) ->
    SyncedModels = lists:filter(fun(Model) ->
        case lists:member({get_ctx, 0}, Model:module_info(exports)) of
            false -> false;
            true ->
                case Model:get_ctx() of
                    #{sync_enabled := true} -> true;
                    _ -> false
                end
        end
    end, datastore_config_plugin:get_models()),
    %% @TODO VFS-6135 Add model transferred_file when stopping dbsync is implemented
    SyncedModels1 = SyncedModels -- [transferred_file],
    assert_documents_cleaned_up(Worker, SpaceId, SyncedModels1).


assert_documents_cleaned_up(Worker, Scope, Models) ->
    {PoolActiveEntries, _} = pool_utils:get_pools_entries_and_sizes(Worker, memory),
    ActiveMemoryKeys = lists:map(fun(Entry) ->
        element(2, Entry)
    end, lists:flatten(PoolActiveEntries)),
    
    ProviderId = ?GET_DOMAIN_BIN(Worker),
    ?assert(lists:foldl(fun(Model, AccOut) ->
        Keys = rpc:call(Worker, ?MODULE, get_keys, [Model]),
        
        Ctx = datastore_model_default:set_defaults(datastore_model_default:get_ctx(Model)),
        #{disc_driver := DiscDriver} = Ctx,
        Result = case DiscDriver of
            undefined -> 
                % ignore memory only models because there are no od_* models in tests as zone is mocked 
                % and other models are not related to space support
                [];
            _ ->
                ?assertEqual([], lists_utils:intersect(Keys, ActiveMemoryKeys)),
                #{disc_driver_ctx := DiscDriverCtx} = Ctx,
                rpc:call(Worker, DiscDriver, get, [DiscDriverCtx, Keys])
        end,
        AccOut and lists:foldl(
            fun (?ERROR_NOT_FOUND, AccIn) -> AccIn;
                ({ok, _, #document{scope = S}}, AccIn) when S =/= Scope -> AccIn;
                ({ok, _, #document{deleted = true}}, AccIn) -> AccIn;
                ({ok, _, #document{value = #links_forest{}}}, AccIn) -> AccIn; %% @TODO VFS-6278 Sometimes links_forest documents are not properly deleted
                ({ok, _, #document{mutators = [M]}}, AccIn) when M =/= ProviderId-> AccIn; %% @TODO VFS-6135 Remove when stopping dbsync during unsupport is implemented
                ({ok, _, Doc}, _) ->
                    ct:pal("Document not cleaned up in model ~p:~n ~p", [Model, Doc]),
                    false
            end, true, Result)
    end, true, Models)).


get_keys(Model) ->
    Ctx = datastore_model_default:set_defaults(datastore_model_default:get_ctx(Model)),
    #{memory_driver := MemoryDriver, memory_driver_ctx := MemoryDriverCtx} = Ctx,
    get_keys(MemoryDriver, MemoryDriverCtx).


get_keys(ets_driver, MemoryDriverCtx) ->
    lists:foldl(fun(#{table := Table}, AccOut) ->
        AccOut ++ lists:map(fun
            ({Key, _Doc}) -> Key
        end, ets:tab2list(Table))
    end, [], datastore_multiplier:get_names(MemoryDriverCtx));
get_keys(mnesia_driver, MemoryDriverCtx) ->
    lists:foldl(fun(#{table := Table}, AccOut) ->
        AccOut ++ mnesia:async_dirty(fun() ->
            mnesia:foldl(fun({entry, Key, _Doc}, Acc) -> [Key | Acc] end, [], Table)
        end)
    end, [], datastore_multiplier:get_names(MemoryDriverCtx)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-define(filename(Name, Num), <<Name/binary,(integer_to_binary(Num))/binary>>).

create_files_and_dirs(Worker, SessId) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_ID),
    Name = generator:gen_name(),
    {ok, DirGuid} = lfm_proxy:mkdir(Worker, SessId(Worker), SpaceGuid, ?filename(Name, 0), 8#775),
    {ok, {G1, H1}} = lfm_proxy:create_and_open(Worker, SessId(Worker), DirGuid, ?filename(Name, 1), 8#664),
    {ok, _} = lfm_proxy:write(Worker, H1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(Worker, H1),
    {ok, {G2, H2}} = lfm_proxy:create_and_open(Worker, SessId(Worker), SpaceGuid, ?filename(Name, 2), 8#664),
    {ok, _} = lfm_proxy:write(Worker, H2, 0, ?TEST_DATA),
    ok = lfm_proxy:close(Worker, H2),
    {{DirGuid, ?filename(Name, 0)}, {G1, filename:join([?filename(Name, 0), ?filename(Name, 1)])}, {G2, ?filename(Name, 2)}}.

create_qos_entry(Worker, SessId, FileGuid, Expression) ->
    lfm_proxy:add_qos_entry(Worker, SessId(Worker), {guid, FileGuid}, Expression, 1).

create_custom_metadata(Worker, SessId, FileGuid) ->
    lfm_proxy:set_metadata(Worker, SessId(Worker), {guid, FileGuid}, json,
        #{<<"key">> => <<"value">>}, []).

create_replication(Worker, SessId, FileGuid, TargetWorker) ->
    lfm_proxy:schedule_file_replication(Worker, SessId(Worker), {guid, FileGuid},
        ?GET_DOMAIN_BIN(TargetWorker)).

create_eviction(Worker, SessId, FileGuid, TargetWorker) ->
    lfm_proxy:schedule_file_replica_eviction(Worker, SessId(Worker), {guid, FileGuid},
        ?GET_DOMAIN_BIN(TargetWorker), undefined).

create_view(Worker, SpaceId, TargetWorker) ->
    ViewFunction =
        <<"function (id, type, meta, ctx) {
             if(type == 'custom_metadata'
                && meta['onedata_json']
                && meta['onedata_json']['meta']
                && meta['onedata_json']['meta']['color'])
             {
                 return [meta['onedata_json']['meta']['color'], id];
             }
             return null;
       }">>,
    ok = rpc:call(Worker, index, save, [SpaceId, <<"view_name">>, ViewFunction, undefined,
        [], false, [?GET_DOMAIN_BIN(TargetWorker)]]).

check_distribution(Workers, SessId, Desc, Guid) ->
    ExpectedDistribution = lists:map(fun({W, TotalBlocksSize}) ->
        #{
            <<"providerId">> => ?GET_DOMAIN_BIN(W), 
            <<"totalBlocksSize">> => TotalBlocksSize, 
            <<"blocks">> => [[0, TotalBlocksSize]]
        }
    end, Desc),
    ExpectedDistributionSorted = lists:sort(fun(#{<<"providerId">> := ProviderIdA}, #{<<"providerId">> := ProviderIdB}) ->
        ProviderIdA =< ProviderIdB
    end, ExpectedDistribution),
    lists:foreach(fun(Worker) ->
    ?assertEqual({ok, ExpectedDistributionSorted}, 
            lfm_proxy:get_file_distribution(Worker, SessId(Worker), {guid, Guid}), ?ATTEMPTS
        )
    end, Workers).

check_files_on_storage(Worker, FilesList, ShouldExist, IsImportedStorage) ->
    lists:foreach(fun(FileRelativePath) ->
        StoragePath = storage_file_path(Worker, ?SPACE_ID, FileRelativePath, IsImportedStorage),
        ?assertEqual(ShouldExist, check_exists_on_storage(Worker, StoragePath), ?ATTEMPTS)
    end, FilesList).

check_exists_on_storage(Worker, StorageFilePath) ->
    rpc:call(Worker, filelib, is_file, [StorageFilePath]).

storage_file_path(Worker, SpaceId, FilePath, true) ->
    SpaceMnt = get_space_mount_point(Worker, SpaceId),
    filename:join([SpaceMnt, FilePath]);
storage_file_path(Worker, SpaceId, FilePath, false) ->
    SpaceMnt = get_space_mount_point(Worker, SpaceId),
    filename:join([SpaceMnt, SpaceId, FilePath]).

get_space_mount_point(Worker, SpaceId) ->
    StorageId = initializer:get_supporting_storage_id(Worker, SpaceId),
    storage_mount_point(Worker, StorageId).

storage_mount_point(Worker, StorageId) ->
    Helper = rpc:call(Worker, storage, get_helper, [StorageId]),
    HelperArgs = helper:get_args(Helper),
    maps:get(<<"mountPoint">>, HelperArgs).

select_provider_with_imported_storage(Workers) ->
    select_provider_by_imported_storage_value(Workers, true).

select_provider_without_imported_storage(Workers) ->
    select_provider_by_imported_storage_value(Workers, false).

select_provider_by_imported_storage_value(Workers, Expected) ->
    [Worker | _] = lists:filter(fun(W) ->
        StorageId = initializer:get_supporting_storage_id(W, ?SPACE_ID),
        Expected == rpc:call(W, storage, is_imported, [StorageId])
    end, Workers),
    Worker.
