%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of files counting to be used with different environments
%%% @end
%%%-------------------------------------------------------------------
-module(files_counting_test_base).
-author("Michal Wrzeszcz").


-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/errors.hrl").


-export([files_counting_test/1, multiprovider_files_counting_test/1]).


%%%===================================================================
%%% Test functions
%%%===================================================================

files_counting_test(Config) ->
    % TODO VFS-8835 - test rename
    init_environment(Config, op_worker_nodes),
    check_files_count_and_size(Config, op_worker_nodes).


multiprovider_files_counting_test(Config) ->
    [Workers2 | _] = ?config(workers2, Config),
    UserId = <<"user1">>,
    [{SpaceId, _SpaceName} | _] = ?config({spaces, UserId}, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    init_environment(Config, workers1),

    lists:foreach(fun(NodesSelector) ->
        check_files_count_and_size(Config, NodesSelector)
    end, [workers1, workers2]),

    % Write to file on second provider to check if size is decreased properly
    write_to_file(Config, workers2, [1, 1, 1], [1], 20),
    check_files_count_and_size(Config, workers1, [1, 1, 1], [], 12, 3, 84),
    check_files_count_and_size(Config, workers1, SpaceGuid, 363, 120, 1314),

    % Check if deletions of files are counted properly
    lfm_test_utils:clean_space([Workers2], SpaceId, 30),
    lists:foreach(fun(WorkersType) ->
        check_files_count_and_size(Config, WorkersType, SpaceGuid, 0, 0, 0)
    end, [workers2, workers1]).


%%%===================================================================
%%% Internal functions
%%%===================================================================

init_environment(Config, NodesSelector) ->
    [Worker | _] = ?config(NodesSelector, Config),
    UserId = <<"user1">>,
    {SessId, _UserId} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, UserId}, Config)},
    [{SpaceId, _SpaceName} | _] = ?config({spaces, UserId}, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    % Check if histogram is empty
    {ok, CurrentValues, WindowsMap} = ?assertMatch({ok, _, _},
        rpc:call(Worker, files_counter, get_values_map_and_histogram, [SpaceGuid])),
    ?assertEqual(get_expected_current_values_map(0, 0, 0), CurrentValues),
    ?assertEqual(lists:duplicate(12, []), maps:values(WindowsMap)),

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


check_files_count_and_size(Config, NodesSelector) ->
    [Worker | _] = ?config(NodesSelector, Config),
    UserId = <<"user1">>,
    [{SpaceId, _SpaceName} | _] = ?config({spaces, UserId}, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    % Check file and dir count (files are empty)
    check_files_count_and_size(Config, NodesSelector, [2, 1, 1, 1], [], 3, 0, 0),
    check_files_count_and_size(Config, NodesSelector, [2, 1, 1], [], 12, 3, 0),
    check_files_count_and_size(Config, NodesSelector, [2, 1], [], 39, 12, 0),
    check_files_count_and_size(Config, NodesSelector, [2], [], 120, 39, 0),

    % Check size sum, file and dir count for dirs with not empty files
    check_files_count_and_size(Config, NodesSelector, [1, 1, 1, 1], [], 3, 0, 55),
    check_files_count_and_size(Config, NodesSelector, [1, 1, 1], [], 12, 3, 104),
    check_files_count_and_size(Config, NodesSelector, [1, 1], [], 39, 12, 124),
    check_files_count_and_size(Config, NodesSelector, [1], [], 120, 39, 334),

    % Check size sum, file and dir count for space dir
    check_files_count_and_size(Config, NodesSelector, SpaceGuid, 363, 120, 1334),

    % Check histogram
    {ok, CurrentValues2, WindowsMap2} = ?assertMatch({ok, _, _},
        rpc:call(Worker, files_counter, get_values_map_and_histogram, [SpaceGuid])),
    ExpectedSize = case NodesSelector of
        workers2 -> 0;
        _ -> 1334
    end,
    ?assertEqual(get_expected_current_values_map(363, 120, ExpectedSize), CurrentValues2),
    ?assertEqual(12, maps:size(WindowsMap2)).


check_files_count_and_size(
    Config, NodesSelector, DirConstructor, FileConstructor,
    ExpectedFileCount, ExpectedDirCount, ExpectedSizeOnCreator
) ->
    Guid = get_guid(Config, NodesSelector, DirConstructor, FileConstructor),
    check_files_count_and_size(Config, NodesSelector, Guid, ExpectedFileCount, ExpectedDirCount, ExpectedSizeOnCreator).


check_files_count_and_size(Config, NodesSelector, Guid, ExpectedFileCount, ExpectedDirCount, ExpectedSizeOnCreator) ->
    [Worker | _] = ?config(NodesSelector, Config),
    Attempts = 60,

    ExpectedSize = case NodesSelector of
        workers2 -> 0;
        _ -> ExpectedSizeOnCreator
    end,

    ExpectedMap = get_expected_current_values_map(ExpectedFileCount, ExpectedDirCount, ExpectedSize),
    ?assertEqual({ok, ExpectedMap}, rpc:call(Worker, files_counter, get_values_map, [Guid]), Attempts).


get_expected_current_values_map(ExpectedFileCount, ExpectedDirCount, ExpectedSize) ->
    #{
        <<"file_count">> => ExpectedFileCount,
        <<"dir_count">> => ExpectedDirCount,
        <<"size_sum">> => ExpectedSize
    }.


write_to_file(Config, NodesSelector, DirConstructor, FileConstructor, BytesCount) ->
    [Worker | _] = ?config(NodesSelector, Config),
    UserId = <<"user1">>,
    {SessId, _UserId} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, UserId}, Config)},
    Guid = get_guid(Config, NodesSelector, DirConstructor, FileConstructor),
    {ok, Handle} = lfm_proxy:open(Worker, SessId, ?FILE_REF(Guid), write),
    {ok, _} = lfm_proxy:write(Worker, Handle, 0, crypto:strong_rand_bytes(BytesCount)),
    ok = lfm_proxy:close(Worker, Handle).


get_guid(Config, NodesSelector, DirConstructor, FileConstructor) ->
    [Worker | _] = ?config(NodesSelector, Config),
    UserId = <<"user1">>,
    {SessId, _UserId} =
        {?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config), ?config({user_id, UserId}, Config)},
    [{_SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    DirPath = create_path(filename:join([<<"/">>, SpaceName]), DirConstructor, "dir"),
    Path = create_path(DirPath, FileConstructor, "file"),
    {ok, #file_attr{guid = Guid}} = ?assertMatch({ok, _}, lfm_proxy:stat(Worker, SessId, {path, Path})),
    Guid.


create_path(PathBeginning, Constructor, NamePrefix) ->
    lists:foldl(fun(DirNum, Acc) ->
        ChildName = str_utils:format_bin("~s_~p", [NamePrefix, DirNum]),
        filename:join([Acc, ChildName])
    end, PathBeginning, Constructor).
