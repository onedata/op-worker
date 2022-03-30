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


-export([single_provider_test/1, multiprovider_test/1]).
-export([init/1, teardown/1]).
-export([verify_dir_on_provider_creating_files/3, delete_stats/3]).


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

single_provider_test(Config) ->
    % TODO VFS-8835 - test rename
    create_initial_file_tree(Config, op_worker_nodes),
    check_initial_dir_stats(Config, op_worker_nodes),
    check_update_times(Config, [op_worker_nodes]).


multiprovider_test(Config) ->
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    create_initial_file_tree(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR),

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


%%%===================================================================
%%% Init and teardown
%%%===================================================================

init(Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    {ok, EnableDirStatsCollectorForNewSpaces} =
        test_utils:get_env(Worker, op_worker, enable_dir_stats_collector_for_new_spaces),
    test_utils:set_env(Workers, op_worker, enable_dir_stats_collector_for_new_spaces, true),

    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    lists:foreach(fun(W) ->
        rpc:call(W, dir_stats_collector_config, init_for_space, [SpaceId])
    end, initializer:get_different_domain_workers(Config)),


    {ok, MinimalSyncRequest} = test_utils:get_env(Worker, op_worker, minimal_sync_request),
    test_utils:set_env(Workers, op_worker, minimal_sync_request, 1),

    [{default_enable_dir_stats_collector_for_new_spaces, EnableDirStatsCollectorForNewSpaces},
        {default_minimal_sync_request, MinimalSyncRequest} | Config].


teardown(Config) ->
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    lists:foreach(fun(W) ->
        rpc:call(W, dir_stats_collector_config, clean_for_space, [SpaceId])
    end, initializer:get_different_domain_workers(Config)),

    Workers = ?config(op_worker_nodes, Config),
    EnableDirStatsCollectorForNewSpaces = ?config(default_enable_dir_stats_collector_for_new_spaces, Config),
    test_utils:set_env(
        Workers, op_worker, enable_dir_stats_collector_for_new_spaces, EnableDirStatsCollectorForNewSpaces),

    MinimalSyncRequest = ?config(default_minimal_sync_request, Config),
    test_utils:set_env(Workers, op_worker, minimal_sync_request, MinimalSyncRequest).


%%%===================================================================
%%% Helper functions to be used in various suites to verify statistics
%%%===================================================================

verify_dir_on_provider_creating_files(Config, NodesSelector, Guid) ->
    [Worker | _] = ?config(NodesSelector, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),

    {ok, Children, _} = ?assertMatch({ok, _, _},
        lfm_proxy:get_children_attrs(Worker, SessId, ?FILE_REF(Guid), #{offset => 0, limit => 100000, optimize_continuous_listing => false})),

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


delete_stats(Config, NodesSelector, Guid) ->
    [Worker | _] = ?config(NodesSelector, Config),
    ?assertEqual(ok, rpc:call(Worker, dir_size_stats, delete_stats, [Guid])),
    ?assertEqual(ok, rpc:call(Worker, dir_update_time_stats, delete_stats, [Guid])).

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_initial_file_tree(Config, NodesSelector) ->
    [Worker | _] = ?config(NodesSelector, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),

    check_space_dir_values_map_and_time_series_collection(Config, NodesSelector, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 0, 
        ?DIR_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE(Config, NodesSelector) => 0
    }, true),

    % Init tree structure
    Structure = [{3, 3}, {3, 3}, {3, 3}, {3, 3}, {0, 3}],
    lfm_test_utils:create_files_tree(Worker, SessId, Structure, SpaceGuid),

    % Fill files with data
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
    }, false).


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


check_space_dir_values_map_and_time_series_collection(Config, NodesSelector, SpaceGuid, ExpectedMap, IsCollectionEmpty) ->
    [Worker | _] = ?config(NodesSelector, Config),
    {ok, {CurrentValues, WindowsMap}} = ?assertMatch({ok, {_, _}},
        rpc:call(Worker, dir_size_stats, get_stats_and_time_series_collections, [SpaceGuid])),

    ?assertEqual(ExpectedMap, CurrentValues),

    case IsCollectionEmpty of
        true -> ?assertEqual(lists:duplicate(16, []), maps:values(WindowsMap));
        false -> ?assertEqual(16, maps:size(WindowsMap))
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