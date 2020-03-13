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
    cleanup_traverse_stage_persistence_test/1,
    cleanup_traverse_stage_test/1,
    delete_local_documents_stage_test/1,
    overall_test/1
]).

all() -> [
    replicate_stage_test,
    replicate_stage_persistence_test,
    cleanup_traverse_stage_persistence_test,
    cleanup_traverse_stage_test,
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
    
    {_Dir, {G1, _}, {G2, _}} = create_files_and_dirs(Config),
    StageJob = #space_unsupport_job{
        stage = replicate,
        space_id = ?SPACE_ID,
        storage_id = StorageId
    },
    
    Promise = rpc:async_call(Worker1, space_unsupport, do_slave_job, [StageJob, ?TASK_ID]),
    
    {ok, {[QosEntryId], _}} = ?assertMatch({ok, {[_], _}}, 
        lfm_proxy:get_effective_file_qos(Worker1, SessId(Worker1), {guid, SpaceGuid}), 
        ?ATTEMPTS),
    
    % check that space unsupport QoS entry cannot be deleted
    ?assertEqual({error, ?EACCES}, lfm_proxy:remove_qos_entry(Worker1, SessId(Worker1), QosEntryId)),
    ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(Worker1, SessId(Worker1), QosEntryId)),
    
    ok = rpc:yield(Promise),
    
    ?assertMatch(?ERROR_NOT_FOUND, lfm_proxy:get_qos_entry(Worker1, SessId(Worker1), QosEntryId)),
    
    Size = size(?TEST_DATA),
    check_distribution(Workers, SessId, [{Worker1, Size}, {Worker2, Size}], G1),
    check_distribution(Workers, SessId, [{Worker1, Size}, {Worker2, Size}], G2).


replicate_stage_persistence_test(Config) ->
    [Worker1, _Worker2] = ?config(op_worker_nodes, Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_ID),
    StorageId = initializer:get_supporting_storage_id(Worker1, ?SPACE_ID),
    
    create_files_and_dirs(Config),
    
    % Create new QoS entry representing entry created before provider restart.
    % Running stage again with existing entry should not create new one, 
    % but wait for fulfillment of previous one.
    {ok, QosEntryId} = lfm_proxy:add_qos_entry(Worker1, SessId(Worker1), {guid, SpaceGuid}, 
        <<?QOS_ANY_STORAGE/binary, " - storageId = ", StorageId/binary>>, 1),
    
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


cleanup_traverse_stage_persistence_test(Config) ->
    [Worker1, _Worker2] = ?config(op_worker_nodes, Config),
    StorageId = initializer:get_supporting_storage_id(Worker1, ?SPACE_ID),
    
    % Create new cleanup traverse representing traverse created before provider restart.
    % Running stage again with existing traverse should not create new one, 
    % but wait for previous one to finish.
    {ok, TaskId} = rpc:call(Worker1, unsupport_cleanup_traverse, start, [?SPACE_ID, StorageId]),
    
    StageJob = #space_unsupport_job{
        stage = cleanup_traverse,
        space_id = ?SPACE_ID,
        storage_id = StorageId,
        subtask_id = TaskId
    },
    
    ok = rpc:call(Worker1, space_unsupport, do_slave_job, [StageJob, ?TASK_ID]),
    % check that no additional traverse was started
    test_utils:mock_assert_num_calls(Worker1, unsupport_cleanup_traverse, start, 1, 0, 1),
    
    rpc:call(Worker1, unsupport_cleanup_traverse, delete_ended, [?SPACE_ID, StorageId]).
    

cleanup_traverse_stage_test(Config) ->
    [Worker1, _Worker2] = Workers = ?config(op_worker_nodes, Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    StorageId = initializer:get_supporting_storage_id(Worker1, ?SPACE_ID),
    
    {{_,DirPath}, {G1, F1Path}, {G2, F2Path}} = create_files_and_dirs(Config),
    
    % Relative paths to space dir. Empty binary represents space dir.
    AllPaths = [<<"">>, DirPath, F1Path, F2Path], 
    lists:foreach(fun(FileRelativePath) -> 
        StoragePath = storage_file_path(Worker1, ?SPACE_ID, FileRelativePath),
        ?assertEqual(true, check_exists_on_storage(Worker1, StoragePath))
    end, AllPaths),
    
    StageJob = #space_unsupport_job{
        stage = cleanup_traverse,
        space_id = ?SPACE_ID,
        storage_id = StorageId
    },
    
    ok = rpc:call(Worker1, space_unsupport, do_slave_job, [StageJob, ?TASK_ID]),
    
    lists:foreach(fun(FileRelativePath) ->
        StoragePath = storage_file_path(Worker1, ?SPACE_ID, FileRelativePath),
        ?assertEqual(false, check_exists_on_storage(Worker1, StoragePath))
    end, AllPaths),
    
    check_distribution(Workers, SessId, [], G1),
    check_distribution(Workers, SessId, [], G2).


delete_local_documents_stage_test(Config) ->
    [Worker1, _Worker2] = ?config(op_worker_nodes, Config),
    StorageId = initializer:get_supporting_storage_id(Worker1, ?SPACE_ID),
    ok = rpc:call(Worker1, storage_sync, configure_import, [?SPACE_ID, true, #{max_depth => 5, sync_acl => true}]),
    ok = rpc:call(Worker1, file_popularity_api, enable, [?SPACE_ID]),
    ACConfig =  #{
        enabled => true,
        target => 0,
        threshold => 100
    },
    ok = rpc:call(Worker1, autocleaning_api, configure, [?SPACE_ID, ACConfig]),
    ok = rpc:call(Worker1, storage_sync_worker, schedule_spaces_check, [0]),
    
    ?assertMatch({ok, _}, rpc:call(Worker1, space_strategies, get, [?SPACE_ID])),
    ?assertMatch({ok, _}, rpc:call(Worker1, storage_sync_monitoring, get, [?SPACE_ID, StorageId]), 10),
    ?assertMatch({ok, _}, rpc:call(Worker1, autocleaning, get, [?SPACE_ID])),
    ?assertMatch({ok, _}, rpc:call(Worker1, file_popularity_config, get, [?SPACE_ID])),
    ?assertMatch(true, rpc:call(Worker1, file_popularity_api, is_enabled, [?SPACE_ID])),
    ?assertMatch({ok, [_], _}, rpc:call(Worker1, traverse_task_list, list, [<<"unsupport_cleanup_traverse">>, ended]), 10),
    
    StageJob = #space_unsupport_job{
        stage = delete_local_documents,
        space_id = ?SPACE_ID,
        storage_id = StorageId
    },
    ok = rpc:call(Worker1, space_unsupport, do_slave_job, [StageJob, ?TASK_ID]),
    
    ?assertEqual({error, not_found}, rpc:call(Worker1, space_strategies, get, [?SPACE_ID])),
    ?assertEqual({error, not_found}, rpc:call(Worker1, storage_sync_monitoring, get, [?SPACE_ID, StorageId])),
    ?assertEqual(undefined, rpc:call(Worker1, autocleaning, get_config, [?SPACE_ID])),
    ?assertEqual({error, not_found}, rpc:call(Worker1, autocleaning, get, [?SPACE_ID])),
    ?assertEqual(false, rpc:call(Worker1, file_popularity_api, is_enabled, [?SPACE_ID])),
    ?assertEqual({error, not_found}, rpc:call(Worker1, file_popularity_config, get, [?SPACE_ID])),
    ?assertMatch({ok, [], _}, rpc:call(Worker1, traverse_task_list, list, [<<"unsupport_cleanup_traverse">>, ended])).


overall_test(Config) ->
    [Worker1, _Worker2] = ?config(op_worker_nodes, Config),
    StorageId = initializer:get_supporting_storage_id(Worker1, ?SPACE_ID),
    
    create_files_and_dirs(Config),
    ok = rpc:call(Worker1, space_unsupport, run, [?SPACE_ID, StorageId]),
    
    % mocked in init_per_testcase
    receive task_finished -> ok end,
    
    % check that all stages have been executed
    lists:foreach(fun(Stage) ->
        test_utils:mock_assert_num_calls(
            Worker1, space_unsupport, do_slave_job,
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
    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        hackney:start(),
        application:start(ssl),
        initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), NewConfig)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, qos_tests_utils]} | Config].


end_per_suite(Config) ->
    hackney:stop(),
    application:stop(ssl),
    initializer:clean_test_users_and_spaces_no_validate(Config).


init_per_testcase(cleanup_traverse_stage_persistence_test, Config) ->
    [Worker1, _Worker2] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker1, unsupport_cleanup_traverse, [passthrough]),
    test_utils:mock_expect(Worker1, unsupport_cleanup_traverse, start, fun(SpaceId, StorageId) ->
        % delay traverse start, so it does not finish before stage is even called
        timer:sleep(timer:seconds(5)), 
        meck:passthrough([SpaceId, StorageId])
    end),
    init_per_testcase(default, Config);
init_per_testcase(overall_test, Config) ->
    [Worker1, _Worker2] = ?config(op_worker_nodes, Config),
    Self = self(),
    test_utils:mock_new(Worker1, space_unsupport, [passthrough]),
    test_utils:mock_expect(Worker1, space_unsupport, do_slave_job,
        fun(#space_unsupport_job{stage = Stage} = Job, TaskId) ->
            Self ! {Stage, meck:num_calls(space_unsupport, do_slave_job, 2)},
            meck:passthrough([Job, TaskId])
        end),
    test_utils:mock_expect(Worker1, space_unsupport, task_finished, 
        fun(TaskId, Pool) ->
            Self ! task_finished,
            meck:passthrough([TaskId, Pool])
        end),
    init_per_testcase(default, Config);
init_per_testcase(_, Config) ->
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config).


end_per_testcase(cleanup_traverse_stage_persistence_test, Config) ->
    [Worker1, _Worker2] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker1, [unsupport_cleanup_traverse]),
    end_per_testcase(default, Config);
end_per_testcase(overall_test, Config) ->
    [Worker1, _Worker2] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker1, [space_unsupport]),
    end_per_testcase(default, Config);
end_per_testcase(_, Config) ->
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-define(filename(Name, Num), <<Name/binary,(integer_to_binary(Num))/binary>>).

create_files_and_dirs(Config) ->
    [Worker1, _Worker2] = ?config(op_worker_nodes, Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_ID),
    Name = generator:gen_name(),
    {ok, DirGuid} = lfm_proxy:mkdir(Worker1, SessId(Worker1), SpaceGuid, ?filename(Name, 0), 8#775),
    {ok, {G1, H1}} = lfm_proxy:create_and_open(Worker1, SessId(Worker1), DirGuid, ?filename(Name, 1), 8#664),
    {ok, _} = lfm_proxy:write(Worker1, H1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(Worker1, H1),
    {ok, {G2, H2}} = lfm_proxy:create_and_open(Worker1, SessId(Worker1), SpaceGuid, ?filename(Name, 2), 8#664),
    {ok, _} = lfm_proxy:write(Worker1, H2, 0, ?TEST_DATA),
    ok = lfm_proxy:close(Worker1, H2),
    {{DirGuid, ?filename(Name, 0)}, {G1, filename:join([?filename(Name, 0), ?filename(Name, 1)])}, {G2, ?filename(Name, 2)}}.

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

check_exists_on_storage(Worker, StorageFilePath) ->
    rpc:call(Worker, filelib, is_file, [StorageFilePath]).

storage_file_path(Worker, SpaceId, FilePath) ->
    SpaceMnt = get_space_mount_point(Worker, SpaceId),
    filename:join([SpaceMnt, SpaceId, FilePath]).

get_space_mount_point(Worker, SpaceId) ->
    StorageId = initializer:get_supporting_storage_id(Worker, SpaceId),
    storage_mount_point(Worker, StorageId).

storage_mount_point(Worker, StorageId) ->
    Helper = rpc:call(Worker, storage, get_helper, [StorageId]),
    HelperArgs = helper:get_args(Helper),
    maps:get(<<"mountPoint">>, HelperArgs).
