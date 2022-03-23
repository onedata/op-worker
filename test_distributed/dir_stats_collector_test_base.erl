%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of dir_stats_collector to be used with different environments
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collector_test_base).
-author("Michal Wrzeszcz").


-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/errors.hrl").


-export([basic_test/1, multiprovider_test/1,
    enabling_for_empty_space_test/1, enabling_for_not_empty_space_test/1, enabling_large_dirs_test/1,
    enabling_during_writing_test/1, race_with_file_adding_test/1, race_with_file_writing_test/1,
    race_with_subtree_adding_test/1, race_with_subtree_filling_with_data_test/1,
    race_with_file_adding_to_large_dir_test/1,
    multiple_status_change_test/1, adding_file_when_disabled_test/1]).
-export([init/1, init_and_enable_for_new_space/1, teardown/1]).
-export([verify_dir_on_provider_creating_files/3, delete_stats/2]).
% TODO VFS-9148 - extend tests


% For multiprovider test, one provider creates files and fills them with data,
% second reads some data and deletes files
-define(PROVIDER_CREATING_FILES_NODES_SELECTOR, workers1).
-define(PROVIDER_DELETING_FILES_NODES_SELECTOR, workers2).

-define(INITIAL_DIR_TOTAL_SIZE_ON_STORAGE(NodeSelector, BytesWritten),
    % initially the files are not replicated - all blocks are located on the creating provider
    case NodeSelector of
        ?PROVIDER_DELETING_FILES_NODES_SELECTOR -> 0;
        _ -> BytesWritten
    end
).
-define(TOTAL_SIZE_ON_STORAGE(Config, NodesSelector), 
    ?SIZE_ON_STORAGE((lfm_test_utils:get_user1_first_storage_id(Config, NodesSelector)))).

-define(ATTEMPTS, 60).

%%%===================================================================
%%% Test functions
%%%===================================================================

basic_test(Config) ->
    % TODO VFS-8835 - test rename
    enable(Config, new_space),
    create_initial_file_tree_and_fill_files(Config, op_worker_nodes, enabled),
    check_initial_dir_stats(Config, op_worker_nodes),
    check_update_times(Config, [op_worker_nodes]).


multiprovider_test(Config) ->
    enable(Config, new_space),
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    create_initial_file_tree_and_fill_files(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR, enabled),

    lists:foreach(fun(NodesSelector) ->
        check_initial_dir_stats(Config, NodesSelector)
    end, [?PROVIDER_CREATING_FILES_NODES_SELECTOR, ?PROVIDER_DELETING_FILES_NODES_SELECTOR]),

    check_update_times(Config, [?PROVIDER_CREATING_FILES_NODES_SELECTOR, ?PROVIDER_DELETING_FILES_NODES_SELECTOR]),

    % Read from file on PROVIDER_DELETING_FILES to check if size on storage is calculated properly
    read_from_file(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR, [1, 1, 1], [1], 10),
    check_dir_stats(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR, [1, 1, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 12,
        ?DIR_COUNT => 3,
        ?TOTAL_SIZE => 104,
        ?TOTAL_SIZE_ON_STORAGE(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR) => 10
    }),

    % Write 20 bytes to file on PROVIDER_DELETING_FILES to decrease the file's size on storage on PROVIDER_CREATING_FILES
    % The file ([1, 1, 1], [1]) was previously 30 bytes on storage on PROVIDER_CREATING_FILES
    % The total size on storage of directory ([1, 1, 1]) was previously 104 bytes on PROVIDER_CREATING_FILES
    % The total size on storage of space was previously 1334 bytes on PROVIDER_CREATING_FILES
    % Size on storage of directory and space should be 20 bytes on PROVIDER_DELETING_FILES and 20 bytes less than before
    % on PROVIDER_CREATING_FILES (20 bytes were invalidated)
    write_to_file(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR, [1, 1, 1], [1], 20),
    check_dir_stats(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR, [1, 1, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 12,
        ?DIR_COUNT => 3,
        ?TOTAL_SIZE => 104,
        ?TOTAL_SIZE_ON_STORAGE(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR) => 20
    }),
    check_dir_stats(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR, [1, 1, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 12,
        ?DIR_COUNT => 3,
        ?TOTAL_SIZE => 104,
        ?TOTAL_SIZE_ON_STORAGE(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR) => 84
    }),
    check_dir_stats(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 363,
        ?DIR_COUNT => 120,
        ?TOTAL_SIZE => 1334,
        ?TOTAL_SIZE_ON_STORAGE(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR) => 20
    }),
    check_dir_stats(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 363,
        ?DIR_COUNT => 120,
        ?TOTAL_SIZE => 1334,
        ?TOTAL_SIZE_ON_STORAGE(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR) => 1314
    }),

    % Check if deletions of files are counted properly
    [Worker2 | _] = ?config(?PROVIDER_DELETING_FILES_NODES_SELECTOR, Config),
    lfm_test_utils:clean_space([Worker2], SpaceId, 30),
    lists:foreach(fun(NodesSelector) ->
        check_dir_stats(Config, NodesSelector, SpaceGuid, #{
            ?REG_FILE_AND_LINK_COUNT => 0,
            ?DIR_COUNT => 0,
            ?TOTAL_SIZE => 0,
            ?TOTAL_SIZE_ON_STORAGE(Config, NodesSelector) => 0
        })
    end, [?PROVIDER_DELETING_FILES_NODES_SELECTOR, ?PROVIDER_CREATING_FILES_NODES_SELECTOR]).


enabling_for_empty_space_test(Config) ->
    enable(Config, existing_space),
    verify_collecting_status(Config, enabled),
    create_initial_file_tree_and_fill_files(Config, op_worker_nodes, initializing),
    check_initial_dir_stats(Config, op_worker_nodes),
    check_update_times(Config, [op_worker_nodes]).


enabling_for_not_empty_space_test(Config) ->
    create_initial_file_tree_and_fill_files(Config, op_worker_nodes, disabled),
    enable(Config, existing_space),
    verify_collecting_status(Config, enabled),
    check_initial_dir_stats(Config, op_worker_nodes),
    check_update_times(Config, [op_worker_nodes]).


enabling_large_dirs_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
    Structure = [{3, 2000}, {3, 300}],
    lfm_test_utils:create_files_tree(Worker, SessId, Structure, SpaceGuid),

    enable(Config, existing_space),
    verify_collecting_status(Config, enabled),

    check_dir_stats(Config, op_worker_nodes, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 2900,
        ?DIR_COUNT => 12,
        ?TOTAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE(Config, op_worker_nodes) => 0
    }).


enabling_during_writing_test(Config) ->
    create_initial_file_tree(Config, op_worker_nodes, disabled),
    enable(Config, existing_space),
    fill_files(Config, op_worker_nodes),
    check_initial_dir_stats(Config, op_worker_nodes),
    check_update_times(Config, [op_worker_nodes]).


race_with_file_adding_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
    OnSpaceChildrenListed = fun() ->
        lfm_test_utils:create_and_write_file(Worker, SessId, SpaceGuid, <<"test_raced_file">>, 0, {rand_content, 10})
    end,
    test_with_race_base(Config, SpaceGuid, OnSpaceChildrenListed, #{
        ?REG_FILE_AND_LINK_COUNT => 13,
        ?DIR_COUNT => 12,
        ?TOTAL_SIZE => 10,
        ?TOTAL_SIZE_ON_STORAGE(Config, op_worker_nodes) => 10
    }).


race_with_file_writing_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
    OnSpaceChildrenListed = fun() ->
        Guid = resolve_guid(Config, op_worker_nodes, [], [1]),
        lfm_test_utils:write_file(Worker, SessId, Guid, {rand_content, 10})
    end,
    test_with_race_base(Config, SpaceGuid, OnSpaceChildrenListed, #{
        ?REG_FILE_AND_LINK_COUNT => 12,
        ?DIR_COUNT => 12,
        ?TOTAL_SIZE => 10,
        ?TOTAL_SIZE_ON_STORAGE(Config, op_worker_nodes) => 10
    }).


race_with_subtree_adding_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    OnSpaceChildrenListed = fun() ->
        TestDirGuid = resolve_guid(Config, op_worker_nodes, [1], []),

        Guids = lists:map(fun(Seq) ->
            SeqBin = integer_to_binary(Seq),
            {ok, CreatedDirGuid} = ?assertMatch({ok, _},
                lfm_proxy:mkdir(Worker, SessId, TestDirGuid, <<"test_dir", SeqBin/binary>>, 8#777)),
            CreatedDirGuid
        end, lists:seq(1, 10)),

        lists:foreach(fun(Guid) ->
            timer:sleep(2000),
            ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, Guid, <<"test_dir">>, 8#777))
        end, Guids)
    end,
    test_with_race_base(Config, [1], OnSpaceChildrenListed, #{
        ?REG_FILE_AND_LINK_COUNT => 12,
        ?DIR_COUNT => 32,
        ?TOTAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE(Config, op_worker_nodes) => 0
    }).


race_with_subtree_filling_with_data_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    OnSpaceChildrenListed = fun() ->
        TestDirGuid = resolve_guid(Config, op_worker_nodes, [1, 1], []),

        lists:foreach(fun(Seq) ->
            SeqBin = integer_to_binary(Seq),
            timer:sleep(2000),
            lfm_test_utils:create_and_write_file(
                Worker, SessId, TestDirGuid, <<"test_file", SeqBin/binary>>, 0, {rand_content, 10})
        end, lists:seq(1, 10))
    end,
    test_with_race_base(Config, [1], OnSpaceChildrenListed, #{
        ?REG_FILE_AND_LINK_COUNT => 22,
        ?DIR_COUNT => 12,
        ?TOTAL_SIZE => 100,
        ?TOTAL_SIZE_ON_STORAGE(Config, op_worker_nodes) => 100
    }).


test_with_race_base(Config, TestDirIdentifier, OnSpaceChildrenListed, ExpectedSpaceStats) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),

    Structure = [{3, 3}, {3, 3}],
    lfm_test_utils:create_files_tree(Worker, SessId, Structure, SpaceGuid),

    TestDirGuid = case is_list(TestDirIdentifier) of
        true -> resolve_guid(Config, op_worker_nodes, TestDirIdentifier, []);
        _ -> TestDirIdentifier
    end,
    TestDirUuid = file_id:guid_to_uuid(TestDirGuid),

    Tag = mock_file_meta(Config, TestDirUuid, 1000),
    enable(Config, existing_space),
    execute_file_meta_list_hook(Tag, OnSpaceChildrenListed),

    check_dir_stats(Config, op_worker_nodes, SpaceGuid, ExpectedSpaceStats),
    verify_collecting_status(Config, enabled).


race_with_file_adding_to_large_dir_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
    Structure = [{3, 2000}, {3, 3}],
    lfm_test_utils:create_files_tree(Worker, SessId, Structure, SpaceGuid),

    Tag = mock_file_meta(Config, file_id:guid_to_uuid(SpaceGuid), 10),
    enable(Config, existing_space),
    OnSpaceChildrenListed = fun() ->
        lfm_test_utils:create_and_write_file(Worker, SessId, SpaceGuid, <<"test_raced_file">>, 0, {rand_content, 10})
    end,
    execute_file_meta_list_hook(Tag, OnSpaceChildrenListed),

    verify_collecting_status(Config, enabled),
    check_dir_stats(Config, op_worker_nodes, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 2010,
        ?DIR_COUNT => 12,
        ?TOTAL_SIZE => 10,
        ?TOTAL_SIZE_ON_STORAGE(Config, op_worker_nodes) => 10
    }).


multiple_status_change_test(Config) ->
    create_initial_file_tree_and_fill_files(Config, op_worker_nodes, disabled),

    enable(Config, existing_space),
    enable(Config, existing_space),
    enable(Config, existing_space),
    verify_collecting_status(Config, enabled),

    disable(Config),
    disable(Config),
    disable(Config),
    verify_collecting_status(Config, disabled),

    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    StatusChangesWithTimestamps = rpc:call(
        Worker, dir_stats_collector_config, get_collecting_status_change_timestamps, [SpaceId]),
    StatusChanges = lists:map(
        fun({StatusChangeDescription, _Timestamp}) -> StatusChangeDescription end, StatusChangesWithTimestamps),
    ExpectedStatusChanges = [disabled, collectors_stopping, enabled],
    ?assertEqual(ExpectedStatusChanges, StatusChanges),

    enable(Config, existing_space),
    disable(Config),
    verify_collecting_status(Config, disabled),

    enable(Config, existing_space),
    disable(Config),
    enable(Config, existing_space),
    disable(Config),
    enable(Config, existing_space),
    enable(Config, existing_space),
    verify_collecting_status(Config, enabled),

    disable(Config),
    enable(Config, existing_space),
    verify_collecting_status(Config, enabled),

    check_initial_dir_stats(Config, op_worker_nodes),
    check_update_times(Config, [op_worker_nodes]),

    {ok, EnablingTime} = ?assertMatch({ok, _}, rpc:call(Worker, dir_stats_collector_config, get_enabling_time, [SpaceId])),
    [{_, EnablingTime2} | _] = rpc:call(
        Worker, dir_stats_collector_config, get_collecting_status_change_timestamps, [SpaceId]),
    ?assertEqual(EnablingTime, EnablingTime2),

    disable(Config),
    enable(Config, existing_space),
    disable(Config),
    disable(Config),
    enable(Config, existing_space),
    disable(Config),
    verify_collecting_status(Config, disabled),

    StatusChangesWithTimestamps2 = rpc:call(
        Worker, dir_stats_collector_config, get_collecting_status_change_timestamps, [SpaceId]),
    [{_, LastChangeTime} | _] = StatusChangesWithTimestamps2,
    lists:foldl(fun({_, Timestamp}, TimestampToCompare) ->
        ?assert(TimestampToCompare >= Timestamp),
        Timestamp
    end, LastChangeTime, StatusChangesWithTimestamps2),

    ?assertEqual(?ERROR_FORBIDDEN, rpc:call(Worker, dir_stats_collector_config, get_enabling_time, [SpaceId])).


adding_file_when_disabled_test(Config) ->
    create_initial_file_tree_and_fill_files(Config, op_worker_nodes, disabled),
    enable(Config, existing_space),
    verify_collecting_status(Config, enabled),
    check_initial_dir_stats(Config, op_worker_nodes),

    disable(Config),
    verify_collecting_status(Config, disabled),
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
    lfm_test_utils:create_and_write_file(Worker, SessId, SpaceGuid, <<"test_file">>, 0, {rand_content, 10}),

    enable(Config, existing_space),
    verify_collecting_status(Config, enabled),
    check_dir_stats(Config, op_worker_nodes, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 364,
        ?DIR_COUNT => 120,
        ?TOTAL_SIZE => 1344,
        ?TOTAL_SIZE_ON_STORAGE(Config, op_worker_nodes) => 1344
    }),
    check_update_times(Config, [op_worker_nodes]).

%%%===================================================================
%%% Init and teardown
%%%===================================================================

init(Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    {ok, EnableDirStatsCollectorForNewSpaces} =
        test_utils:get_env(Worker, op_worker, enable_dir_stats_collector_for_new_spaces),
    test_utils:set_env(Workers, op_worker, enable_dir_stats_collector_for_new_spaces, true),

    {ok, MinimalSyncRequest} = test_utils:get_env(Worker, op_worker, minimal_sync_request),
    test_utils:set_env(Workers, op_worker, minimal_sync_request, 1),

    [{default_enable_dir_stats_collector_for_new_spaces, EnableDirStatsCollectorForNewSpaces},
        {default_minimal_sync_request, MinimalSyncRequest} | Config].


init_and_enable_for_new_space(Config) ->
    UpdatedConfig = init(Config),
    enable(UpdatedConfig, new_space),
    UpdatedConfig.


teardown(Config) ->
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),

    disable(Config),
    verify_collecting_status(Config, disabled),

    lists:foreach(fun(W) ->
        ?assertEqual(ok, rpc:call(W, dir_stats_collector_config, clean, [SpaceId])),
        delete_stats(W, SpaceGuid),
        lists:foreach(fun(Incarnation) ->
            % Clean traverse data (do not assert as not all tests use initialization traverses)
            rpc:call(W, traverse_task, delete_ended, [
                <<"dir_stats_collections_initialization_traverse">>,
                dir_stats_collections_initialization_traverse:gen_task_id(SpaceId, Incarnation)
            ])
        end, lists:seq(1, 10))
    end, initializer:get_different_domain_workers(Config)),

    Workers = ?config(op_worker_nodes, Config),
    lfm_test_utils:clean_space(Workers, SpaceId, 30),

    EnableDirStatsCollectorForNewSpaces = ?config(default_enable_dir_stats_collector_for_new_spaces, Config),
    test_utils:set_env(
        Workers, op_worker, enable_dir_stats_collector_for_new_spaces, EnableDirStatsCollectorForNewSpaces),

    MinimalSyncRequest = ?config(default_minimal_sync_request, Config),
    test_utils:set_env(Workers, op_worker, minimal_sync_request, MinimalSyncRequest),

    test_utils:mock_unload(Workers, file_meta).


%%%===================================================================
%%% Helper functions to be used in various suites to verify statistics
%%%===================================================================

verify_dir_on_provider_creating_files(Config, NodesSelector, Guid) ->
    [Worker | _] = ?config(NodesSelector, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),

    {ok, Children, _} = ?assertMatch({ok, _, _},
        lfm_proxy:get_children_attrs(Worker, SessId, ?FILE_REF(Guid), #{offset => 0, size => 100000})),

    StatsForEmptyDir = #{
        ?REG_FILE_AND_LINK_COUNT => 0,
        ?DIR_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE(Config, NodesSelector) => 0
    },
    Expectations = lists:foldl(fun
        (#file_attr{type = ?DIRECTORY_TYPE, guid = ChildGuid}, Acc) ->
            Acc2 = update_expectations_map(Acc, #{?DIR_COUNT => 1}),
            update_expectations_map(Acc2, verify_dir_on_provider_creating_files(Config, NodesSelector, ChildGuid));
        (#file_attr{size = ChildSize, mtime = ChildMTime, ctime = ChildCTime}, Acc) ->
            update_expectations_map(Acc, #{
                ?REG_FILE_AND_LINK_COUNT => 1,
                ?TOTAL_SIZE => ChildSize,
                ?TOTAL_SIZE_ON_STORAGE(Config, NodesSelector) => ChildSize,
                update_time => max(ChildMTime, ChildCTime)
            })
    end, StatsForEmptyDir, Children),

    check_dir_stats(Config, NodesSelector, Guid, maps:remove(update_time, Expectations)),

    {ok, #file_attr{mtime = MTime, ctime = CTime}} = ?assertMatch({ok, _},
        lfm_proxy:stat(Worker, SessId, ?FILE_REF(Guid))),
    CollectorTime = get_dir_update_time_stat(Worker, Guid),
    ?assert(CollectorTime >= max(MTime, CTime)),
    % Time for directory should not be earlier than time for any child
    ?assert(CollectorTime >= maps:get(update_time, Expectations, 0)),
    update_expectations_map(Expectations, #{update_time => CollectorTime}).


delete_stats(Worker, Guid) ->
    ?assertEqual(ok, rpc:call(Worker, dir_size_stats, delete_stats, [Guid])),
    ?assertEqual(ok, rpc:call(Worker, dir_update_time_stats, delete_stats, [Guid])).

%%%===================================================================
%%% Internal functions
%%%===================================================================

enable(Config, new_space) ->
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    lists:foreach(fun(W) ->
        ?assertEqual(ok, rpc:call(W, dir_stats_collector_config, init_for_empty_space, [SpaceId]))
    end, initializer:get_different_domain_workers(Config));

enable(Config, existing_space) ->
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    lists:foreach(fun(W) ->
        ?assertEqual(ok, rpc:call(W, dir_stats_collector_config, enable, [SpaceId]))
    end, initializer:get_different_domain_workers(Config)).


disable(Config) ->
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    lists:foreach(fun(W) ->
        ?assertEqual(ok, rpc:call(W, dir_stats_collector_config, disable, [SpaceId]))
    end, initializer:get_different_domain_workers(Config)).


verify_collecting_status(Config, ExpectedStatus) ->
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    lists:foreach(fun(W) ->
        ?assertEqual(ExpectedStatus,
            rpc:call(W, dir_stats_collector_config, get_extended_collecting_status, [SpaceId]), ?ATTEMPTS)
    end, initializer:get_different_domain_workers(Config)).


create_initial_file_tree_and_fill_files(Config, NodesSelector, CollectingStatus) ->
    create_initial_file_tree(Config, NodesSelector, CollectingStatus),
    fill_files(Config, NodesSelector).


create_initial_file_tree(Config, NodesSelector, CollectingStatus) ->
    [Worker | _] = ?config(NodesSelector, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),

    check_space_dir_values_map_and_time_series_collection(Config, NodesSelector, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 0,
        ?DIR_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE(Config, NodesSelector) => 0
    }, true, CollectingStatus),

    Structure = [{3, 3}, {3, 3}, {3, 3}, {3, 3}, {0, 3}],
    lfm_test_utils:create_files_tree(Worker, SessId, Structure, SpaceGuid).


fill_files(Config, NodesSelector) ->
    write_to_file(Config, NodesSelector, [1], [1], 10),
    write_to_file(Config, NodesSelector, [1, 1], [1], 20),
    write_to_file(Config, NodesSelector, [1, 1, 1], [1], 30),
    write_to_file(Config, NodesSelector, [1, 1, 1, 1], [1], 50),
    write_to_file(Config, NodesSelector, [1, 1, 1, 1], [2], 5),
    write_to_file(Config, NodesSelector, [1, 1, 1, 2], [1], 13),
    write_to_file(Config, NodesSelector, [1, 1, 1, 3], [1], 1),
    write_to_file(Config, NodesSelector, [1, 1, 1, 3], [2], 2),
    write_to_file(Config, NodesSelector, [1, 1, 1, 3], [3], 3),
    write_to_file(Config, NodesSelector, [1, 2, 3], [1], 200),
    write_to_file(Config, NodesSelector, [3, 2, 1], [2], 1000).


check_initial_dir_stats(Config, NodesSelector) ->
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),

    % all files in paths starting with dir 2 are empty
    check_dir_stats(Config, NodesSelector, [2, 1, 1, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 3, 
        ?DIR_COUNT => 0, 
        ?TOTAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE(Config, NodesSelector) => 0
    }),
    check_dir_stats(Config, NodesSelector, [2, 1, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 12,
        ?DIR_COUNT => 3,
        ?TOTAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE(Config, NodesSelector) => 0
    }),
    check_dir_stats(Config, NodesSelector, [2, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 39,
        ?DIR_COUNT => 12,
        ?TOTAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE(Config, NodesSelector) => 0
    }),
    check_dir_stats(Config, NodesSelector, [2], #{
        ?REG_FILE_AND_LINK_COUNT => 120,
        ?DIR_COUNT => 39,
        ?TOTAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE(Config, NodesSelector) => 0
    }),

    check_dir_stats(Config, NodesSelector, [1, 1, 1, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 3, 
        ?DIR_COUNT => 0, 
        ?TOTAL_SIZE => 55,
        ?TOTAL_SIZE_ON_STORAGE(Config, NodesSelector) => ?INITIAL_DIR_TOTAL_SIZE_ON_STORAGE(NodesSelector, 55)
    }),
    check_dir_stats(Config, NodesSelector, [1, 1, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 12,
        ?DIR_COUNT => 3,
        ?TOTAL_SIZE => 104,
        ?TOTAL_SIZE_ON_STORAGE(Config, NodesSelector) => ?INITIAL_DIR_TOTAL_SIZE_ON_STORAGE(NodesSelector, 104)
    }),
    check_dir_stats(Config, NodesSelector, [1, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 39,
        ?DIR_COUNT => 12,
        ?TOTAL_SIZE => 124,
        ?TOTAL_SIZE_ON_STORAGE(Config, NodesSelector) => ?INITIAL_DIR_TOTAL_SIZE_ON_STORAGE(NodesSelector, 124)
    }),
    check_dir_stats(Config, NodesSelector, [1], #{
        ?REG_FILE_AND_LINK_COUNT => 120,
        ?DIR_COUNT => 39,
        ?TOTAL_SIZE => 334,
        ?TOTAL_SIZE_ON_STORAGE(Config, NodesSelector) => ?INITIAL_DIR_TOTAL_SIZE_ON_STORAGE(NodesSelector, 334)
    }),

    % the space dir should have a sum of all statistics
    check_dir_stats(Config, NodesSelector, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 363,
        ?DIR_COUNT => 120,
        ?TOTAL_SIZE => 1334,
        ?TOTAL_SIZE_ON_STORAGE(Config, NodesSelector) => ?INITIAL_DIR_TOTAL_SIZE_ON_STORAGE(NodesSelector, 1334)
    }),
    check_space_dir_values_map_and_time_series_collection(Config, NodesSelector, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 363,
        ?DIR_COUNT => 120,
        ?TOTAL_SIZE => 1334,
        ?TOTAL_SIZE_ON_STORAGE(Config, NodesSelector) => ?INITIAL_DIR_TOTAL_SIZE_ON_STORAGE(NodesSelector, 1334)
    }, false, enabled).


check_update_times(Config, NodesSelectors) ->
    FileConstructorsToCheck = [
        {[1, 1, 1, 1], [1]},
        {[1, 1, 1, 1], []},
        {[1, 1, 1], []},
        {[1, 1], []},
        {[1], []}
    ],

    ?assertEqual(ok, check_update_times(Config, NodesSelectors, FileConstructorsToCheck), ?ATTEMPTS).


check_update_times(Config, NodesSelectors, FileConstructorsToCheck) ->
    try
        [UpdateTimesOnFirstProvider | _] = AllUpdateTimes = lists:map(fun(NodesSelector) ->
            lists:map(fun({DirConstructor, FileConstructor}) ->
                resolve_update_times_in_metadata_and_stats(Config, NodesSelector, DirConstructor, FileConstructor)
            end, FileConstructorsToCheck)
        end, NodesSelectors),

        % Check if times for all selectors ale equal
        ?assert(lists:all(fun(NodeUpdateTimes) -> NodeUpdateTimes =:= UpdateTimesOnFirstProvider end, AllUpdateTimes)),

        lists:foldl(fun
            ({MetadataTime, not_a_dir}, {MaxMetadataTime, LastCollectorTime}) ->
                {max(MaxMetadataTime, MetadataTime), LastCollectorTime};
            ({MetadataTime, CollectorTime}, {MaxMetadataTime, LastCollectorTime}) ->
                % Time for directory should not be earlier than time for any child
                NewMaxMetadataTime = max(MaxMetadataTime, MetadataTime),
                ?assert(CollectorTime >= NewMaxMetadataTime),
                ?assert(CollectorTime >= LastCollectorTime),
                {NewMaxMetadataTime, CollectorTime}
        end, {0, 0}, UpdateTimesOnFirstProvider),

        ok
    catch
        Error:Reason ->
            {Error, Reason}
    end.


check_space_dir_values_map_and_time_series_collection(
    Config, NodesSelector, SpaceGuid, _ExpectedMap, _IsCollectionEmpty, disabled = _CollectingStatus
) ->
    [Worker | _] = ?config(NodesSelector, Config),
    ?assertMatch({error, dir_stats_disabled_for_space},
        rpc:call(Worker, dir_size_stats, get_stats_and_time_series_collections, [SpaceGuid]));

check_space_dir_values_map_and_time_series_collection(
    Config, NodesSelector, SpaceGuid, ExpectedMap, IsCollectionEmpty, CollectingStatus
) ->
    Attempts = case CollectingStatus of
        enabled -> 1;
        initializing -> ?ATTEMPTS
    end,
    [Worker | _] = ?config(NodesSelector, Config),
    {ok, {CurrentValues, WindowsMap}} = ?assertMatch({ok, {_, _}},
        rpc:call(Worker, dir_size_stats, get_stats_and_time_series_collections, [SpaceGuid]), Attempts),

    ?assertEqual(ExpectedMap, CurrentValues),

    case {IsCollectionEmpty, CollectingStatus} of
        {true, enabled} ->
            ?assertEqual(lists:duplicate(16, []), maps:values(WindowsMap));
        {true, initializing} ->
            ?assertEqual(lists:duplicate(16, 0),
                lists:map(fun([{_Timestamp, Value}]) -> Value end, maps:values(WindowsMap)));
        {false, _} ->
            ?assertEqual(16, maps:size(WindowsMap))
    end.


check_dir_stats(Config, NodesSelector, Guid, ExpectedMap) when is_binary(Guid) ->
    [Worker | _] = ?config(NodesSelector, Config),
    ?assertEqual({ok, ExpectedMap}, rpc:call(Worker, dir_size_stats, get_stats, [Guid]), ?ATTEMPTS);

check_dir_stats(Config, NodesSelector, DirConstructor, ExpectedMap) ->
    Guid = resolve_guid(Config, NodesSelector, DirConstructor, []),
    check_dir_stats(Config, NodesSelector, Guid, ExpectedMap).


read_from_file(Config, NodesSelector, DirConstructor, FileConstructor, BytesCount) ->
    [Worker | _] = ?config(NodesSelector, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    Guid = resolve_guid(Config, NodesSelector, DirConstructor, FileConstructor),
    lfm_test_utils:read_file(Worker, SessId, Guid, BytesCount).


write_to_file(Config, NodesSelector, DirConstructor, FileConstructor, BytesCount) ->
    [Worker | _] = ?config(NodesSelector, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    Guid = resolve_guid(Config, NodesSelector, DirConstructor, FileConstructor),
    lfm_test_utils:write_file(Worker, SessId, Guid, {rand_content, BytesCount}).


resolve_guid(Config, NodesSelector, DirConstructor, FileConstructor) ->
    #file_attr{guid = Guid} = resolve_attrs(Config, NodesSelector, DirConstructor, FileConstructor),
    Guid.


resolve_update_times_in_metadata_and_stats(Config, NodesSelector, DirConstructor, FileConstructor) ->
    #file_attr{guid = Guid, mtime = MTime, ctime = CTime} =
        resolve_attrs(Config, NodesSelector, DirConstructor, FileConstructor),

    DirUpdateTime = case FileConstructor of
        [] ->
            [Worker | _] = ?config(NodesSelector, Config),
            get_dir_update_time_stat(Worker, Guid);
        _ ->
            not_a_dir
    end,

    {max(MTime, CTime), DirUpdateTime}.


resolve_attrs(Config, NodesSelector, DirConstructor, FileConstructor) ->
    [Worker | _] = ?config(NodesSelector, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceName = lfm_test_utils:get_user1_first_space_name(Config),

    DirPath = build_path(filename:join([<<"/">>, SpaceName]), DirConstructor, "dir"),
    Path = build_path(DirPath, FileConstructor, "file"),
    {ok, Attrs} = ?assertMatch({ok, _}, lfm_proxy:stat(Worker, SessId, {path, Path})),
    Attrs.


build_path(PathBeginning, Constructor, NamePrefix) ->
    lists:foldl(fun(DirNum, Acc) ->
        ChildName = str_utils:format_bin("~s_~p", [NamePrefix, DirNum]),
        filename:join([Acc, ChildName])
    end, PathBeginning, Constructor).


update_expectations_map(Map, DiffMap) ->
    maps:fold(fun
        (update_time, NewTime, Acc) -> maps:update_with(update_time, fun(Value) -> max(Value, NewTime) end, 0, Acc);
        (Key, Diff, Acc) -> maps:update_with(Key, fun(Value) -> Value + Diff end, Acc)
    end, Map, DiffMap).


get_dir_update_time_stat(Worker, Guid) ->
    {ok, CollectorTime} = ?assertMatch({ok, _}, rpc:call(Worker, dir_update_time_stats, get_update_time, [Guid])),
    CollectorTime.


mock_file_meta(Config, Uuid, SleepTime) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Master = self(),
    Tag = make_ref(),
    ok = test_utils:mock_new(Worker, file_meta, [passthrough]),
    ok = test_utils:mock_expect(Worker, file_meta, list_children, fun
        (FileUuid, ListOpts) when FileUuid =:= Uuid ->
            Ans = meck:passthrough([FileUuid, ListOpts]),
            Master ! {space_children_listed, Tag},
            timer:sleep(SleepTime),
            Ans;
        (FileUuid, ListOpts) ->
            meck:passthrough([FileUuid, ListOpts])
    end),
    Tag.


execute_file_meta_list_hook(Tag, Hook) ->
    MessageReceived = receive
        {space_children_listed, Tag} ->
            Hook(),
            ok
    after
        10000 -> timeout
    end,
    ?assertEqual(ok, MessageReceived).