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
-include_lib("cluster_worker/include/modules/datastore/datastore_time_series.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/errors.hrl").


-export([basic_test/1, hardlinks_test/1, multiprovider_hardlinks_test/1,
    multiprovider_test/1, multiprovider_trash_test/1, transfer_after_enabling_test/1,
    enabling_for_empty_space_test/1, enabling_for_not_empty_space_test/1, enabling_large_dirs_test/1,
    enabling_during_writing_test/1, race_with_file_adding_test/1, race_with_file_writing_test/1,
    race_with_subtree_adding_test/1, race_with_subtree_filling_with_data_test/1,
    race_with_file_adding_to_large_dir_test/1,
    multiple_status_change_test/1, adding_file_when_disabled_test/1,
    restart_test/1, parallel_write_test/4]).
-export([init/1, init_and_enable_for_new_space/1, teardown/1, teardown/3]).
-export([verify_dir_on_provider_creating_files/3]).
% TODO VFS-9148 - extend tests


% For multiprovider test, one provider creates files and fills them with data,
% second reads some data and deletes files
-define(PROVIDER_CREATING_FILES_NODES_SELECTOR, workers1).
-define(PROVIDER_DELETING_FILES_NODES_SELECTOR, workers2).

-define(TOTAL_SIZE_ON_STORAGE_VALUE(Selector, BytesWritten),
    case Selector of
        {empty_provider, _} -> 0;
        % initially the files are not replicated - all blocks are located on the creating provider
        {initial_size_on_provider, ?PROVIDER_DELETING_FILES_NODES_SELECTOR} -> 0;
        _ -> BytesWritten
    end
).
-define(TOTAL_SIZE_ON_STORAGE_KEY(Config, Selector),
    ?SIZE_ON_STORAGE((lfm_test_utils:get_user1_first_storage_id(Config, get_config_nodes_selector(Selector))))).

-define(ATTEMPTS, 60).

%%%===================================================================
%%% Test functions
%%%===================================================================

basic_test(Config) ->
    % TODO VFS-8835 - test rename
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    TmpDirGuid = fslogic_file_id:spaceid_to_tmp_dir_guid(SpaceId),
    {ok, NotCountedDirGuid} = ?assertMatch({ok, _},
        lfm_proxy:mkdir(Worker, SessId, TmpDirGuid, ?RAND_STR(), undefined)),

    create_initial_file_tree_and_fill_files(Config, op_worker_nodes, enabled),
    check_initial_dir_stats(Config, op_worker_nodes),
    check_update_times(Config, [op_worker_nodes]),

    check_dir_stats(Config, op_worker_nodes, TmpDirGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 0,
        ?DIR_COUNT => 2, % includes dir for opened deleted dirs (created with space)
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?LOGICAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, op_worker_nodes) => 0
    }),

    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(NotCountedDirGuid))),
    check_dir_stats(Config, op_worker_nodes, TmpDirGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 0,
        ?DIR_COUNT => 1, % includes dir for opened deleted dirs (created with space)
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?LOGICAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, op_worker_nodes) => 0
    }),

    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
    {ok, GuidsAndNames} = ?assertMatch({ok, _}, lfm_proxy:get_children(Worker, SessId, ?FILE_REF(SpaceGuid), 0, 100)),

    lists:foreach(fun({Guid, _}) ->
        {ok, ChildrenGuidsAndNames} = ?assertMatch({ok, _}, lfm_proxy:get_children(Worker, SessId, ?FILE_REF(Guid), 0, 100)),
        lists:foreach(fun({ChildGuid, _}) ->
            ?assertEqual(ok, lfm_proxy:rm_recursive(Worker, SessId, {uuid, file_id:guid_to_uuid(ChildGuid)}))
        end, ChildrenGuidsAndNames),

        check_dir_stats(Config, op_worker_nodes, Guid, #{
            ?REG_FILE_AND_LINK_COUNT => 0,
            ?DIR_COUNT => 0,
            ?FILE_ERRORS_COUNT => 0,
            ?DIR_ERRORS_COUNT => 0,
            ?TOTAL_SIZE => 0,
            ?LOGICAL_SIZE => 0,
            ?TOTAL_SIZE_ON_STORAGE_KEY(Config, op_worker_nodes) => 0
        }),

        ?assertEqual(ok, lfm_proxy:rm_recursive(Worker, SessId, {uuid, file_id:guid_to_uuid(Guid)}))
    end, GuidsAndNames),

    check_dir_stats(Config, op_worker_nodes, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 0,
        ?DIR_COUNT => 0,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?LOGICAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, op_worker_nodes) => 0
    }),

    check_dir_stats(Config, op_worker_nodes, fslogic_file_id:spaceid_to_trash_dir_guid(SpaceId), #{
        ?REG_FILE_AND_LINK_COUNT => 0,
        ?DIR_COUNT => 0,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?LOGICAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, op_worker_nodes) => 0
    }).


hardlinks_test(Config) ->
    hardlinks_test(Config, op_worker_nodes, op_worker_nodes, undefined).


multiprovider_hardlinks_test(Config) ->
    ct:print("Write on creator test"),
    hardlinks_test(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR, ?PROVIDER_CREATING_FILES_NODES_SELECTOR,
        ?PROVIDER_DELETING_FILES_NODES_SELECTOR),
    ct:print("Write on remote node test"),
    hardlinks_test(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR, ?PROVIDER_CREATING_FILES_NODES_SELECTOR,
        ?PROVIDER_DELETING_FILES_NODES_SELECTOR).


hardlinks_test(Config, CreatorSelector, WriteNodesSelector, StatsCheckNodesSelector) ->
    [Worker | _] = ?config(WriteNodesSelector, Config),
    [Creator | _] = ?config(CreatorSelector, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    CreatorSessId = lfm_test_utils:get_user1_session_id(Config, Creator),
    SpaceName = lfm_test_utils:get_user1_first_space_name(Config),
    {CheckSelectors, OverriddenFileCheckSelectors} = case StatsCheckNodesSelector of
        undefined ->
            {[WriteNodesSelector], [WriteNodesSelector]}; % Check only on node that writes
        % Check both writer and node chosen for verification
        CreatorSelector ->
            {
                [{empty_provider, WriteNodesSelector}, StatsCheckNodesSelector],
                [WriteNodesSelector, {empty_provider, StatsCheckNodesSelector}]
            };
        _ ->
            {
                [WriteNodesSelector, {empty_provider, StatsCheckNodesSelector}],
                [WriteNodesSelector, {empty_provider, StatsCheckNodesSelector}]
            }
    end,
    [OpenedFileCheckSelector | _] = CheckSelectors,
    [OverriddenOpenedFileCheckSelector | _] = OverriddenFileCheckSelectors,

    % Setup dirs, files and links
    [Dir1Guid, Dir2Guid] = DirGuids = lists:map(fun(_) ->
        {ok, DirGuid} = ?assertMatch({ok, _},
            lfm_proxy:mkdir(Creator, CreatorSessId, filename:join(["/", SpaceName, generator:gen_name()]))),
        DirGuid
    end, lists:seq(1,2)),

    FileContent = <<"1234567890">>,
    FileSize = byte_size(FileContent),
    [File1Guid, File2Guid, File3Guid, File4Guid] = FileGuids = lists:map(fun(_) ->
        file_ops_test_utils:create_file(Creator, CreatorSessId, Dir1Guid, generator:gen_name(), FileContent)
    end, lists:seq(1,4)),

    [Link1Guid, Link2Guid, Link3Guid, Link4Guid] = lists:map(fun(FileGuid) ->
        {ok, #file_attr{guid = LinkGuid}} = ?assertMatch({ok, _},
            lfm_proxy:make_link(Creator, CreatorSessId, ?FILE_REF(FileGuid), ?FILE_REF(Dir2Guid), generator:gen_name())),
        LinkGuid
    end, FileGuids),

    % Check operations on links - list of tuples describe {INodesExpectedCount, LinksExpectedCount} for each dir
    verify_hardlinks(Config, CheckSelectors, DirGuids, [{4, 4}, {0, 4}], FileSize),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link1Guid))),
    verify_hardlinks(Config, CheckSelectors, DirGuids, [{4, 4}, {0, 3}], FileSize),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(File1Guid))),
    verify_hardlinks(Config, CheckSelectors, DirGuids, [{3, 3}, {0, 3}], FileSize),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(File2Guid))),
    verify_hardlinks(Config, CheckSelectors, DirGuids, [{2, 2}, {1, 3}], FileSize),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link2Guid))),
    verify_hardlinks(Config, CheckSelectors, DirGuids, [{2, 2}, {0, 2}], FileSize),

    {ok, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(Link3Guid), rdwr)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(File3Guid))),
    verify_hardlinks(Config, CheckSelectors, DirGuids, [{1, 1}, {1, 2}], FileSize),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link3Guid))),
    verify_after_delete_of_opened_file(Config, OpenedFileCheckSelector, DirGuids, [{1, 1}, {0, 1}], 1, FileSize),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle1)),
    verify_after_delete_of_opened_file(Config, CheckSelectors, DirGuids, [{1, 1}, {0, 1}], 0, FileSize),

    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(Link4Guid), rdwr)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link4Guid))),
    verify_hardlinks(Config, CheckSelectors, DirGuids, [{1, 1}, {0, 0}], FileSize),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(File4Guid))),
    verify_after_delete_of_opened_file(Config, OpenedFileCheckSelector, DirGuids, [{0, 0}, {0, 0}], 1, FileSize),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle2)),
    verify_after_delete_of_opened_file(Config, CheckSelectors, DirGuids, [{0, 0}, {0, 0}], 0, FileSize),

    % Create additional dirs, files and links
    {ok, Dir3Guid} = ?assertMatch({ok, _},
        lfm_proxy:mkdir(Creator, CreatorSessId, filename:join(["/", SpaceName, generator:gen_name()]))),
    DirGuids2 = DirGuids ++ [Dir3Guid],

    [File5Guid, File6Guid, File7Guid] = FileGuids2 = lists:map(fun(_) ->
        file_ops_test_utils:create_file(Creator, CreatorSessId, Dir3Guid, generator:gen_name(), FileContent)
    end, lists:seq(1,3)),

    [Link5Guid, Link6Guid, Link7Guid] = lists:map(fun(FileGuid) ->
        {ok, #file_attr{guid = LinkGuid}} = ?assertMatch({ok, _},
            lfm_proxy:make_link(Creator, CreatorSessId, ?FILE_REF(FileGuid), ?FILE_REF(Dir1Guid), generator:gen_name())),
        LinkGuid
    end, FileGuids2),

    [Link8Guid, Link9Guid, Link10Guid] = lists:map(fun(FileGuid) ->
        {ok, #file_attr{guid = LinkGuid}} = ?assertMatch({ok, _},
            lfm_proxy:make_link(Creator, CreatorSessId, ?FILE_REF(FileGuid), ?FILE_REF(Dir2Guid), generator:gen_name())),
        LinkGuid
    end, FileGuids2),

    % Check additional dirs, files and links
    verify_hardlinks(Config, CheckSelectors, DirGuids2, [{0, 3}, {0, 3}, {3, 3}], FileSize),
    append_files(Config, WriteNodesSelector, [File5Guid, Link6Guid, Link10Guid], 0), % Override on writer node
    FileSize2 = append_files(Config, WriteNodesSelector, [File5Guid, Link6Guid, Link10Guid], FileSize),
    verify_hardlinks(Config, OverriddenFileCheckSelectors, DirGuids2, [{0, 3}, {0, 3}, {3, 3}], FileSize2),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(File5Guid))),
    FileSize3 = append_files(Config, WriteNodesSelector, [Link5Guid, File6Guid, Link10Guid], FileSize2),
    verify_hardlinks(Config, OverriddenFileCheckSelectors, DirGuids2, [{0, 3}, {1, 3}, {2, 2}], FileSize3),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link8Guid))),
    verify_hardlinks(Config, OverriddenFileCheckSelectors, DirGuids2, [{1, 3}, {0, 2}, {2, 2}], FileSize3),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link5Guid))),
    verify_hardlinks(Config, OverriddenFileCheckSelectors, DirGuids2, [{0, 2}, {0, 2}, {2, 2}], FileSize3),

    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link9Guid))),
    FileSize4 = append_files(Config, WriteNodesSelector, [Link6Guid, File7Guid], FileSize3),
    verify_hardlinks(Config, OverriddenFileCheckSelectors, DirGuids2, [{0, 2}, {0, 1}, {2, 2}], FileSize4),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(File6Guid))),
    verify_hardlinks(Config, OverriddenFileCheckSelectors, DirGuids2, [{1, 2}, {0, 1}, {1, 1}], FileSize4),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link6Guid))),
    verify_hardlinks(Config, OverriddenFileCheckSelectors, DirGuids2, [{0, 1}, {0, 1}, {1, 1}], FileSize4),

    {ok, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(File7Guid), rdwr)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(File7Guid))),
    verify_hardlinks(Config, OverriddenFileCheckSelectors, DirGuids2, [{0, 1}, {1, 1}, {0, 0}], FileSize4),
    FileSize5 = append_files(Config, WriteNodesSelector, [Link7Guid], FileSize4),
    ?assertMatch({ok, _}, lfm_proxy:write(Worker, Handle3, FileSize5, <<"xyz">>)),
    FileSize6 = FileSize5 + 3,
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link7Guid))),
    verify_hardlinks(Config, OverriddenFileCheckSelectors, DirGuids2, [{0, 0}, {1, 1}, {0, 0}], FileSize6),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link10Guid))),
    ?assertMatch({ok, _}, lfm_proxy:write(Worker, Handle3, FileSize6, <<"xyz">>)),
    FileSize7 = FileSize6 + 3,
    verify_after_delete_of_opened_file(Config, OverriddenOpenedFileCheckSelector, DirGuids2,
        [{0, 0}, {0, 0}, {0, 0}], 1, FileSize7),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle3)),
    verify_after_delete_of_opened_file(Config, OverriddenFileCheckSelectors, DirGuids2, [{0, 0}, {0, 0}, {0, 0}], 0, FileSize7),

    % Test rename on hardlinks
    {ok, Dir4Guid} = ?assertMatch({ok, _},
        lfm_proxy:mkdir(Creator, CreatorSessId, filename:join(["/", SpaceName, generator:gen_name()]))),
    DirGuids3 = DirGuids2 ++ [Dir4Guid],
    File8Guid = file_ops_test_utils:create_file(Creator, CreatorSessId, Dir4Guid, generator:gen_name(), FileContent),
    {ok, #file_attr{guid = Link11Guid}} = ?assertMatch({ok, _},
        lfm_proxy:make_link(Creator, CreatorSessId, ?FILE_REF(File8Guid), ?FILE_REF(Dir3Guid), generator:gen_name())),

    verify_hardlinks(Config, CheckSelectors, DirGuids3, [{0, 0}, {0, 0}, {0, 1}, {1, 1}], FileSize),
    ?assertMatch({ok, _}, lfm_proxy:mv(Worker, SessId, ?FILE_REF(Link11Guid), ?FILE_REF(Dir1Guid), generator:gen_name())),
    verify_hardlinks(Config, CheckSelectors, DirGuids3, [{0, 1}, {0, 0}, {0, 0}, {1, 1}], FileSize),
    append_files(Config, WriteNodesSelector, [Link11Guid], 0), % Override on writer node
    FileSize8 = append_files(Config, WriteNodesSelector, [Link11Guid], FileSize),
    verify_hardlinks(Config, OverriddenFileCheckSelectors, DirGuids3, [{0, 1}, {0, 0}, {0, 0}, {1, 1}], FileSize8),
    ?assertMatch({ok, _}, lfm_proxy:mv(Worker, SessId, ?FILE_REF(File8Guid), ?FILE_REF(Dir2Guid), generator:gen_name())),
    verify_hardlinks(Config, OverriddenFileCheckSelectors, DirGuids3, [{0, 1}, {1, 1}, {0, 0}, {0, 0}], FileSize8),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(File8Guid))),
    verify_hardlinks(Config, OverriddenFileCheckSelectors, DirGuids3, [{1, 1}, {0, 0}, {0, 0}, {0, 0}], FileSize8),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link11Guid))),
    verify_hardlinks(Config, OverriddenFileCheckSelectors, DirGuids3, [{0, 0}, {0, 0}, {0, 0}, {0, 0}], FileSize8),

    % Test rename of dir with hardlinks
    File9Guid = file_ops_test_utils:create_file(Creator, CreatorSessId, Dir1Guid, generator:gen_name(), FileContent),
    File10Guid = file_ops_test_utils:create_file(Creator, CreatorSessId, Dir2Guid, generator:gen_name(), FileContent),
    ?assertMatch({ok, _},
        lfm_proxy:make_link(Creator, CreatorSessId, ?FILE_REF(File10Guid), ?FILE_REF(Dir1Guid), generator:gen_name())),
    ?assertMatch({ok, _},
        lfm_proxy:make_link(Creator, CreatorSessId, ?FILE_REF(File9Guid), ?FILE_REF(Dir3Guid), generator:gen_name())),

    % Note - list of tuples describe {INodesExpectedCount, LinksExpectedCount} or
    %        {INodesExpectedCount, LinksExpectedCount, DirExpectedCount} if dir contains subdirectories
    verify_hardlinks(Config, CheckSelectors, DirGuids3, [{1, 2}, {1, 1}, {0, 1}, {0, 0}], FileSize),
    ?assertMatch({ok, _}, lfm_proxy:mv(Worker, SessId, ?FILE_REF(Dir2Guid), ?FILE_REF(Dir4Guid), generator:gen_name())),
    verify_hardlinks(Config, CheckSelectors, [Dir1Guid, Dir3Guid, Dir4Guid], [{1, 2}, {0, 1}, {1, 1, 1}], FileSize),
    ?assertMatch({ok, _}, lfm_proxy:mv(Worker, SessId, ?FILE_REF(Dir3Guid), ?FILE_REF(Dir4Guid), generator:gen_name())),
    verify_hardlinks(Config, CheckSelectors, [Dir1Guid, Dir4Guid], [{1, 2}, {1, 2, 2}], FileSize),
    ?assertMatch({ok, _}, lfm_proxy:mv(Worker, SessId, ?FILE_REF(Dir1Guid), ?FILE_REF(Dir4Guid), generator:gen_name())),
    verify_hardlinks(Config, CheckSelectors, [Dir4Guid], [{2, 4, 3}], FileSize),

    ?assertMatch(ok, lfm_proxy:rm_recursive(Worker, SessId, ?FILE_REF(Dir2Guid))),
    verify_hardlinks(Config, CheckSelectors, [Dir4Guid], [{2, 3, 2}], FileSize),
    ?assertMatch(ok, lfm_proxy:rm_recursive(Worker, SessId, ?FILE_REF(Dir3Guid))),
    verify_hardlinks(Config, CheckSelectors, [Dir4Guid], [{2, 2, 1}], FileSize),
    ?assertMatch(ok, lfm_proxy:rm_recursive(Worker, SessId, ?FILE_REF(Dir1Guid))),
    verify_hardlinks(Config, CheckSelectors, [Dir4Guid], [{0, 0}], FileSize),
    ?assertMatch(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Dir4Guid))),


    % Verify multiple open/close operations on single file/hardlink and cleaning of files after provider restart
    [Dir5Guid, Dir6Guid] = DirGuids4 = lists:map(fun(_) ->
        {ok, DirGuid} = ?assertMatch({ok, _},
            lfm_proxy:mkdir(Creator, CreatorSessId, filename:join(["/", SpaceName, generator:gen_name()]))),
        DirGuid
    end, lists:seq(1,2)),
    [File12Guid, File13Guid, File14Guid, File15Guid] = FileGuids3 = lists:map(fun(_) ->
        file_ops_test_utils:create_file(Creator, CreatorSessId, Dir5Guid, generator:gen_name(), FileContent)
    end, lists:seq(1,4)),
    [Link12Guid, Link13Guid, Link14Guid, Link15Guid] = lists:map(fun(FileGuid) ->
        {ok, #file_attr{guid = LinkGuid}} = ?assertMatch({ok, _},
            lfm_proxy:make_link(Creator, CreatorSessId, ?FILE_REF(FileGuid), ?FILE_REF(Dir6Guid), generator:gen_name())),
        LinkGuid
    end, FileGuids3),

    verify_hardlinks(Config, CheckSelectors, DirGuids4, [{4, 4}, {0, 4}], FileSize),
    {ok, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(Link12Guid), rdwr)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(File12Guid))),
    verify_after_delete_of_opened_file(Config, OpenedFileCheckSelector, DirGuids4, [{3, 3}, {1, 4}], 0, FileSize),
    {ok, Handle5} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(Link12Guid), rdwr)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link12Guid))),
    verify_after_delete_of_opened_file(Config, OpenedFileCheckSelector, DirGuids4, [{3, 3}, {0, 3}], 1, FileSize),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle4)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle5)),
    verify_after_delete_of_opened_file(Config, CheckSelectors, DirGuids4, [{3, 3}, {0, 3}], 0, FileSize),

    {ok, Handle6} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(File13Guid), rdwr)),
    {ok, Handle7} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(Link13Guid), rdwr)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(File13Guid))),
    verify_after_delete_of_opened_file(Config, OpenedFileCheckSelector, DirGuids4, [{2, 2}, {1, 3}], 0, FileSize),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link13Guid))),
    verify_after_delete_of_opened_file(Config, OpenedFileCheckSelector, DirGuids4, [{2, 2}, {0, 2}], 1, FileSize),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle6)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle7)),
    verify_after_delete_of_opened_file(Config, CheckSelectors, DirGuids4, [{2, 2}, {0, 2}], 0, FileSize),

    {ok, Handle8} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(File14Guid), rdwr)),
    {ok, Handle9} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(Link14Guid), rdwr)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(File14Guid))),
    verify_after_delete_of_opened_file(Config, CheckSelectors, DirGuids4, [{1, 1}, {1, 2}], 0, FileSize),

    {ok, Handle10} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(File15Guid), rdwr)),
    {ok, Handle11} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(Link15Guid), rdwr)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(File15Guid))),
    verify_after_delete_of_opened_file(Config, CheckSelectors, DirGuids4, [{0, 0}, {2, 2}], 0, FileSize),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link15Guid))),
    verify_after_delete_of_opened_file(Config, OpenedFileCheckSelector, DirGuids4, [{0, 0}, {1, 1}], 1, FileSize),

    ?assertMatch(ok, rpc:call(Worker, fslogic_delete, cleanup_opened_files, [])),
    verify_after_delete_of_opened_file(Config, OpenedFileCheckSelector, DirGuids4, [{0, 0}, {1, 1}], 0, FileSize),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link14Guid))),
    verify_after_delete_of_opened_file(Config, OpenedFileCheckSelector, DirGuids4, [{0, 0}, {0, 0}], 0, FileSize),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle8)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle9)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle10)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle11)),
    verify_after_delete_of_opened_file(Config, OpenedFileCheckSelector, DirGuids4, [{0, 0}, {0, 0}], 0, FileSize),

    % Test close right after unlink
    File16Guid = file_ops_test_utils:create_file(Creator, CreatorSessId, Dir6Guid, generator:gen_name(), FileContent),
    {ok, #file_attr{guid = Link16Guid}} = ?assertMatch({ok, _},
        lfm_proxy:make_link(Creator, CreatorSessId, ?FILE_REF(File16Guid), ?FILE_REF(Dir5Guid), generator:gen_name())),
    verify_hardlinks(Config, CheckSelectors, DirGuids4, [{0, 1}, {1, 1}], FileSize),

    {ok, Handle12} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(File16Guid), rdwr)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(File16Guid))),
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Link16Guid))),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle12)),
    verify_after_delete_of_opened_file(Config, OpenedFileCheckSelector, DirGuids4, [{0, 0}, {0, 0}], 0, FileSize),

    timer:sleep(15000),
    verify_after_delete_of_opened_file(Config, OpenedFileCheckSelector, DirGuids4, [{0, 0}, {0, 0}], 0, FileSize),

    ?assertMatch(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Dir5Guid))),
    ?assertMatch(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Dir6Guid))).



verify_hardlinks(Config, NodesSelectors, DirGuids, DirSizes, FileSize) ->
    ct:print("Verify existing hardlinks stats"),
    verify_hardlinks_stats_enabled(Config, NodesSelectors, DirGuids, DirSizes, FileSize),
    disable(Config),
    enable(Config),
    ct:print("Verify reinitialized hardlinks stats"),
    verify_hardlinks_stats_enabled(Config, NodesSelectors, DirGuids, DirSizes, FileSize).


verify_hardlinks_stats_enabled(_Config, [], _DirGuids, _DirSizes, _FileSize) ->
    ok;

verify_hardlinks_stats_enabled(Config, [NodesSelector | NodesSelectors], DirGuids, DirSizes, FileSize) ->
    verify_hardlinks_stats_enabled(Config, NodesSelector, DirGuids, DirSizes, FileSize),
    verify_hardlinks_stats_enabled(Config, NodesSelectors, DirGuids, DirSizes, FileSize);

verify_hardlinks_stats_enabled(Config, NodesSelector, DirGuids, DirSizes, FileSize) ->
    ct:print("Verify hardlinks stats for nodes: ~p", [NodesSelector]),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(lfm_test_utils:get_user1_first_space_id(Config)),

    lists:foreach(fun
        ({Guid, {INodesExpected, LinksExpected}}) ->
            ct:print("Verify hardlinks stats for dir: ~p", [Guid]),
            check_dir_stats(Config, NodesSelector, Guid, #{
                ?REG_FILE_AND_LINK_COUNT => LinksExpected,
                ?DIR_COUNT => 0,
                ?FILE_ERRORS_COUNT => 0,
                ?DIR_ERRORS_COUNT => 0,
                ?TOTAL_SIZE => INodesExpected * FileSize,
                ?LOGICAL_SIZE => LinksExpected * FileSize,
                ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) =>
                    ?TOTAL_SIZE_ON_STORAGE_VALUE(NodesSelector, INodesExpected * FileSize)
            });
        ({Guid, {INodesExpected, LinksExpected, SubdirsExpected}}) ->
            ct:print("Verify hardlinks stats for dir: ~p", [Guid]),
            check_dir_stats(Config, NodesSelector, Guid, #{
                ?REG_FILE_AND_LINK_COUNT => LinksExpected,
                ?DIR_COUNT => SubdirsExpected,
                ?FILE_ERRORS_COUNT => 0,
                ?DIR_ERRORS_COUNT => 0,
                ?TOTAL_SIZE => INodesExpected * FileSize,
                ?LOGICAL_SIZE => LinksExpected * FileSize,
                ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) =>
                    ?TOTAL_SIZE_ON_STORAGE_VALUE(NodesSelector, INodesExpected * FileSize)
            })
    end, lists:zip(DirGuids, DirSizes)),

    {INodesExpectedSum, LinksExpectedSum, SubdirsExpectedSum} =
        lists:foldl(fun
            ({DirINodesExpected, DirLinksExpected}, {Acc1, Acc2, Acc3}) ->
                {Acc1 + DirINodesExpected, Acc2 + DirLinksExpected, Acc3};
            ({DirINodesExpected, DirLinksExpected, DirSubdirsExpected}, {Acc1, Acc2, Acc3}) ->
                {Acc1 + DirINodesExpected, Acc2 + DirLinksExpected, Acc3 + DirSubdirsExpected}
        end, {0, 0, 0}, DirSizes),

    ct:print("Verify hardlinks stats for space"),
    check_dir_stats(Config, NodesSelector, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => LinksExpectedSum,
        ?DIR_COUNT => length(DirGuids) + SubdirsExpectedSum,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => INodesExpectedSum * FileSize,
        ?LOGICAL_SIZE => LinksExpectedSum * FileSize,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) =>
            ?TOTAL_SIZE_ON_STORAGE_VALUE(NodesSelector, INodesExpectedSum * FileSize)
    }).


verify_after_delete_of_opened_file(Config, NodesSelectors, DirGuids, DirSizes, DeletedFilesCount, FileSize) ->
    ct:print("Verify files stats after delete of opened file"),
    verify_hardlinks_stats_enabled(Config, NodesSelectors, DirGuids, DirSizes, FileSize),
    verify_opened_deleted_files_stats_enabled(Config, NodesSelectors, DeletedFilesCount, FileSize),
    disable(Config),
    enable(Config),
    ct:print("Verify reinitialized stats after delete of opened file"),
    verify_hardlinks_stats_enabled(Config, NodesSelectors, DirGuids, DirSizes, FileSize),
    verify_opened_deleted_files_stats_enabled(Config, NodesSelectors, DeletedFilesCount, FileSize).


verify_opened_deleted_files_stats_enabled(_Config, [], _FilesExpected, _FileSize) ->
    ok;

verify_opened_deleted_files_stats_enabled(Config, [NodesSelector | NodesSelectors], FilesExpected, FileSize) ->
    verify_opened_deleted_files_stats_enabled(Config, NodesSelector, FilesExpected, FileSize),
    verify_opened_deleted_files_stats_enabled(Config, NodesSelectors, FilesExpected, FileSize);

verify_opened_deleted_files_stats_enabled(Config, NodesSelector, FilesExpected, FileSize) ->
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    TmpDirGuid = fslogic_file_id:spaceid_to_tmp_dir_guid(SpaceId),
    ct:print("Verify opened deleted files stats for nodes ~p", [NodesSelector]),
    check_dir_stats(Config, NodesSelector, TmpDirGuid, #{
        ?REG_FILE_AND_LINK_COUNT => FilesExpected,
        ?DIR_COUNT => 1,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => FilesExpected * FileSize,
        ?LOGICAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) =>
            ?TOTAL_SIZE_ON_STORAGE_VALUE(NodesSelector, FilesExpected * FileSize)
    }).


multiprovider_test(Config) ->
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(lfm_test_utils:get_user1_first_space_id(Config)),

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
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 104,
        ?LOGICAL_SIZE => 104,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR) => 10
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
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 104,
        ?LOGICAL_SIZE => 104,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR) => 20
    }),
    check_dir_stats(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR, [1, 1, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 12,
        ?DIR_COUNT => 3,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 104,
        ?LOGICAL_SIZE => 104,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR) => 84
    }),
    check_dir_stats(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 363,
        ?DIR_COUNT => 120,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 1334,
        ?LOGICAL_SIZE => 1334,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR) => 20
    }),
    check_dir_stats(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 363,
        ?DIR_COUNT => 120,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 1334,
        ?LOGICAL_SIZE => 1334,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR) => 1314
    }).


multiprovider_trash_test(Config) ->
    create_initial_file_tree_and_fill_files(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR, enabled),

    lists:foreach(fun(NodesSelector) ->
        check_initial_dir_stats(Config, NodesSelector)
    end, [?PROVIDER_CREATING_FILES_NODES_SELECTOR, ?PROVIDER_DELETING_FILES_NODES_SELECTOR]),

    check_update_times(Config, [?PROVIDER_CREATING_FILES_NODES_SELECTOR, ?PROVIDER_DELETING_FILES_NODES_SELECTOR]),

    [Worker | _] = ?config(?PROVIDER_DELETING_FILES_NODES_SELECTOR, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
    {ok, GuidsAndNames} = ?assertMatch({ok, _}, lfm_proxy:get_children(Worker, SessId, ?FILE_REF(SpaceGuid), 0, 100)),

    lists:foreach(fun({Guid, _}) ->
        {ok, ChildrenGuidsAndNames} = ?assertMatch({ok, _}, lfm_proxy:get_children(Worker, SessId, ?FILE_REF(Guid), 0, 100)),
        lists:foreach(fun({ChildGuid, _}) ->
            ?assertEqual(ok, lfm_proxy:rm_recursive(Worker, SessId, {uuid, file_id:guid_to_uuid(ChildGuid)}))
        end, ChildrenGuidsAndNames),

        ?assertEqual(ok, lfm_proxy:rm_recursive(Worker, SessId, {uuid, file_id:guid_to_uuid(Guid)}))
    end, GuidsAndNames),

    lists:foreach(fun(NodesSelector) ->
        ct:print("Checking node ~p", [NodesSelector]),
        CheckFun = fun() ->
            [W | _] = ?config(NodesSelector, Config),
            SpaceStats = rpc:call(W, dir_size_stats, get_stats, [SpaceGuid]),
            TrashStats = rpc:call(W, dir_size_stats, get_stats, [fslogic_file_id:spaceid_to_trash_dir_guid(SpaceId)]),
            HighestLevelDirStats = lists:map(fun({Guid, _}) ->
                rpc:call(W, dir_size_stats, get_stats, [Guid])
            end, GuidsAndNames),
            [SpaceStats, TrashStats | HighestLevelDirStats]
        end,
        EmptyDirStats = #{
            ?REG_FILE_AND_LINK_COUNT => 0,
            ?DIR_COUNT => 0,
            ?FILE_ERRORS_COUNT => 0,
            ?DIR_ERRORS_COUNT => 0,
            ?TOTAL_SIZE => 0,
            ?LOGICAL_SIZE => 0,
            ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) => 0
        },
        ExpectedResult = lists:duplicate(length(GuidsAndNames) + 2, {ok, EmptyDirStats}),
        ?assertEqual(ExpectedResult, CheckFun(), ?ATTEMPTS)
    end, [?PROVIDER_DELETING_FILES_NODES_SELECTOR, ?PROVIDER_CREATING_FILES_NODES_SELECTOR]).


transfer_after_enabling_test(Config) ->
    [WorkerCreatingFiles | _] = ?config(?PROVIDER_CREATING_FILES_NODES_SELECTOR, Config),
    [WorkerWithDelayedInit | _] = ?config(?PROVIDER_DELETING_FILES_NODES_SELECTOR, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, WorkerCreatingFiles),
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),

    ?assertEqual(ok, rpc:call(WorkerCreatingFiles, dir_stats_service_state, enable, [SpaceId])),
    ?assertEqual(enabled,
        rpc:call(WorkerCreatingFiles, dir_stats_service_state, get_extended_status, [SpaceId]), ?ATTEMPTS),
    create_initial_file_tree_and_fill_files(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR, initializing),
    check_initial_dir_stats(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR),


    ProviderWithDelayedInitId = rpc:call(WorkerWithDelayedInit, oneprovider, get_id_or_undefined, []),
    ?assertEqual(ok, rpc:call(WorkerWithDelayedInit, dir_stats_service_state, enable, [SpaceId])),
    check_initial_dir_stats(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR),
    ?assertMatch({ok, _},
        opt_transfers:schedule_file_replication(WorkerCreatingFiles, SessId, #file_ref{guid = SpaceGuid}, ProviderWithDelayedInitId)),
    check_filled_tree(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR).


enabling_for_empty_space_test(Config) ->
    enable(Config),
    verify_collecting_status(Config, enabled),
    create_initial_file_tree_and_fill_files(Config, op_worker_nodes, initializing),
    check_initial_dir_stats(Config, op_worker_nodes),
    check_update_times(Config, [op_worker_nodes]).


enabling_for_not_empty_space_test(Config) ->
    create_initial_file_tree_and_fill_files(Config, op_worker_nodes, disabled),
    enable(Config),
    verify_collecting_status(Config, enabled),
    check_initial_dir_stats(Config, op_worker_nodes),
    check_update_times(Config, [op_worker_nodes]).


enabling_large_dirs_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
    Structure = [{3, 2000}, {3, 300}],
    lfm_test_utils:create_files_tree(Worker, SessId, Structure, SpaceGuid),

    enable(Config),
    verify_collecting_status(Config, enabled),

    check_dir_stats(Config, op_worker_nodes, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 2900,
        ?DIR_COUNT => 12,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?LOGICAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, op_worker_nodes) => 0
    }).


enabling_during_writing_test(Config) ->
    create_initial_file_tree(Config, op_worker_nodes, disabled),
    enable(Config),
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
        ?REG_FILE_AND_LINK_COUNT => 22,
        ?DIR_COUNT => 21,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 10,
        ?LOGICAL_SIZE => 10,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, op_worker_nodes) => 10
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
        ?REG_FILE_AND_LINK_COUNT => 21,
        ?DIR_COUNT => 21,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 10,
        ?LOGICAL_SIZE => 10,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, op_worker_nodes) => 10
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
        ?REG_FILE_AND_LINK_COUNT => 21,
        ?DIR_COUNT => 41,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?LOGICAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, op_worker_nodes) => 0
    }).


race_with_subtree_filling_with_data_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    OnSpaceChildrenListed = fun() ->
        TestDirGuid = resolve_guid(Config, op_worker_nodes, [1], []),
        TestDir2Guid = resolve_guid(Config, op_worker_nodes, [1, 1], []),

        lists:foreach(fun(Seq) ->
            SeqBin = integer_to_binary(Seq),
            timer:sleep(2000),
            lfm_test_utils:create_and_write_file(
                Worker, SessId, TestDirGuid, <<"test_file", SeqBin/binary>>, 0, {rand_content, 10}),
            lfm_test_utils:create_and_write_file(
                Worker, SessId, TestDir2Guid, <<"test_file2", SeqBin/binary>>, 0, {rand_content, 10})
        end, lists:seq(1, 10))
    end,
    test_with_race_base(Config, [1], OnSpaceChildrenListed, #{
        ?REG_FILE_AND_LINK_COUNT => 41,
        ?DIR_COUNT => 21,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 200,
        ?LOGICAL_SIZE => 200,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, op_worker_nodes) => 200
    }).


test_with_race_base(Config, TestDirIdentifier, OnSpaceChildrenListed, ExpectedSpaceStats) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),

    Structure = [{3, 3}, {3, 3}, {1, 1}],
    lfm_test_utils:create_files_tree(Worker, SessId, Structure, SpaceGuid),

    TestDirGuid = case is_list(TestDirIdentifier) of
        true -> resolve_guid(Config, op_worker_nodes, TestDirIdentifier, []);
        _ -> TestDirIdentifier
    end,
    TestDirUuid = file_id:guid_to_uuid(TestDirGuid),

    Tag = mock_file_listing(Config, TestDirUuid, 1000),
    enable(Config),
    execute_file_listing_hook(Tag, OnSpaceChildrenListed),

    check_dir_stats(Config, op_worker_nodes, SpaceGuid, ExpectedSpaceStats),
    verify_collecting_status(Config, enabled).


race_with_file_adding_to_large_dir_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
    Structure = [{3, 2000}, {3, 3}],
    lfm_test_utils:create_files_tree(Worker, SessId, Structure, SpaceGuid),

    Tag = mock_file_listing(Config, file_id:guid_to_uuid(SpaceGuid), 10),
    enable(Config),
    OnSpaceChildrenListed = fun() ->
        lfm_test_utils:create_and_write_file(Worker, SessId, SpaceGuid, <<"test_raced_file">>, 0, {rand_content, 10})
    end,
    execute_file_listing_hook(Tag, OnSpaceChildrenListed),

    verify_collecting_status(Config, enabled),
    check_dir_stats(Config, op_worker_nodes, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 2010,
        ?DIR_COUNT => 12,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 10,
        ?LOGICAL_SIZE => 10,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, op_worker_nodes) => 10
    }).


multiple_status_change_test(Config) ->
    create_initial_file_tree_and_fill_files(Config, op_worker_nodes, disabled),

    enable(Config),
    enable(Config),
    enable(Config),
    verify_collecting_status(Config, enabled),

    disable(Config),
    disable(Config),
    disable(Config),
    verify_collecting_status(Config, disabled),

    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    StatusChangesWithTimestamps = rpc:call(
        Worker, dir_stats_service_state, get_status_change_timestamps, [SpaceId]),
    StatusChanges = lists:map(
        fun({StatusChangeDescription, _Timestamp}) -> StatusChangeDescription end, StatusChangesWithTimestamps),
    ExpectedStatusChanges = [disabled, stopping, enabled],
    ?assertEqual(ExpectedStatusChanges, StatusChanges),

    enable(Config),
    disable(Config),
    verify_collecting_status(Config, disabled),

    enable(Config),
    disable(Config),
    enable(Config),
    disable(Config),
    enable(Config),
    enable(Config),
    verify_collecting_status(Config, enabled),

    disable(Config),
    enable(Config),
    verify_collecting_status(Config, enabled),

    check_initial_dir_stats(Config, op_worker_nodes),
    check_update_times(Config, [op_worker_nodes]),

    {ok, InitializationTime} = ?assertMatch({ok, _},
        rpc:call(Worker, dir_stats_service_state, get_last_initialization_timestamp_if_in_enabled_status, [SpaceId])),
    [{_, EnablingTime2}, {_, InitializationTime2} | _] = rpc:call(
        Worker, dir_stats_service_state, get_status_change_timestamps, [SpaceId]),
    ?assertEqual(InitializationTime, InitializationTime2),
    ?assert(EnablingTime2 >= InitializationTime),

    disable(Config),
    enable(Config),
    disable(Config),
    disable(Config),
    enable(Config),
    disable(Config),
    verify_collecting_status(Config, disabled),

    StatusChangesWithTimestamps2 = rpc:call(
        Worker, dir_stats_service_state, get_status_change_timestamps, [SpaceId]),
    [{_, LastChangeTime} | _] = StatusChangesWithTimestamps2,
    lists:foldl(fun({_, Timestamp}, TimestampToCompare) ->
        ?assert(TimestampToCompare >= Timestamp),
        Timestamp
    end, LastChangeTime, StatusChangesWithTimestamps2),

    ?assertEqual(?ERROR_DIR_STATS_DISABLED_FOR_SPACE,
        rpc:call(Worker, dir_stats_service_state, get_last_initialization_timestamp_if_in_enabled_status, [SpaceId])).


adding_file_when_disabled_test(Config) ->
    create_initial_file_tree_and_fill_files(Config, op_worker_nodes, disabled),
    enable(Config),
    verify_collecting_status(Config, enabled),
    check_initial_dir_stats(Config, op_worker_nodes),

    disable(Config),
    verify_collecting_status(Config, disabled),
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),
    lfm_test_utils:create_and_write_file(Worker, SessId, SpaceGuid, <<"test_file">>, 0, {rand_content, 10}),

    enable(Config),
    verify_collecting_status(Config, enabled),
    check_dir_stats(Config, op_worker_nodes, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 364,
        ?DIR_COUNT => 120,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 1344,
        ?LOGICAL_SIZE => 1344,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, op_worker_nodes) => 1344
    }),
    check_update_times(Config, [op_worker_nodes]).


restart_test(Config) ->
    enable(Config),

    reset_restart_hooks(Config),
    hang_stopping(Config),
    execute_restart_hooks(Config),
    verify_collecting_status(Config, disabled),

    enable(Config),
    verify_collecting_status(Config, enabled),

    hang_stopping(Config),
    execute_restart_hooks(Config),
    verify_collecting_status(Config, stopping), % restarts hooks have been executed once so they have no effect

    reset_restart_hooks(Config),
    execute_restart_hooks(Config),
    verify_collecting_status(Config, disabled).


parallel_write_test(Config, SleepOnWrite, InitialFileSize, OverrideInitialBytes) ->
    [Worker | _] = ?config(?PROVIDER_CREATING_FILES_NODES_SELECTOR, Config),
    [WorkerProvider2 | _] = ?config(?PROVIDER_DELETING_FILES_NODES_SELECTOR, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),

    check_space_dir_values_map_and_time_series_collection(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 0,
        ?DIR_COUNT => 0,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?LOGICAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR) => 0
    }, true, enabled),

    % Create files and fill using 100 processes (spawn is hidden in pmap)
    lfm_test_utils:create_files_tree(Worker, SessId, [{5, 20}], SpaceGuid, InitialFileSize),
    WriteAnswers = lists_utils:pmap(fun(N) ->
        FileNum = N div 5 + 1,
        ChunkNum = N rem 5,

        case SleepOnWrite of
            true -> timer:sleep(timer:seconds(20 - ChunkNum * 4));
            false -> ok
        end,

        Offset = case OverrideInitialBytes of
            true -> ChunkNum * 1000;
            false -> InitialFileSize + ChunkNum * 1000
        end,
        write_to_file(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR, [], [FileNum], 1000, Offset)
    end, lists:seq(0, 99)),
    ?assert(lists:all(fun(Ans) -> Ans =:= ok end, WriteAnswers)),

    FileSize = case OverrideInitialBytes of
        true -> 5000;
        false -> InitialFileSize + 5000
    end,

    % Check stats on both providers
    check_dir_stats(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 20,
        ?DIR_COUNT => 5,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 20 * FileSize,
        ?LOGICAL_SIZE => 20 * FileSize,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, ?PROVIDER_CREATING_FILES_NODES_SELECTOR) => 20 * FileSize
    }),
    check_dir_stats(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 20,
        ?DIR_COUNT => 5,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 20 * FileSize,
        ?LOGICAL_SIZE => 20 * FileSize,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR) => 0
    }),

    % Read files using 20 processes (spawn is hidden in pmap)
    ReadAnswers = lists_utils:pmap(fun(FileNum) ->
        % Check blocks visibility on reading provider before reading from file
        % Guid is resolved using path so it is possible that resolve_guid fails even if file is seen in statistics
        FileGuid = resolve_guid(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR, [], [FileNum], ?ATTEMPTS),
        GetBlocks = fun() ->
            % @TODO VFS-VFS-9498 use distribution after replication uses fetched file location instead of dbsynced
            case opt_file_metadata:get_local_knowledge_of_remote_provider_blocks(WorkerProvider2, FileGuid, opw_test_rpc:get_provider_id(Worker)) of
                {ok, Blocks} -> Blocks;
                {error, _} = Error -> Error
            end
        end,
        ?assertEqual([[0, FileSize]], GetBlocks(), ?ATTEMPTS),

        Bytes = read_from_file(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR, [], [FileNum], FileSize),
        byte_size(Bytes)
    end, lists:seq(1, 20)),
    ?assert(lists:all(fun(Ans) -> Ans =:= FileSize end, ReadAnswers)),

    % Check stats after reading
    check_dir_stats(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 20,
        ?DIR_COUNT => 5,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 20 * FileSize,
        ?LOGICAL_SIZE => 20 * FileSize,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, ?PROVIDER_DELETING_FILES_NODES_SELECTOR) => 20 * FileSize
    }).


%%%===================================================================
%%% Init and teardown
%%%===================================================================

init(Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),

    {ok, MinimalSyncRequest} = test_utils:get_env(Worker, op_worker, minimal_sync_request),
    test_utils:set_env(Workers, op_worker, minimal_sync_request, 1),
    [{default_minimal_sync_request, MinimalSyncRequest} | Config].


init_and_enable_for_new_space(Config) ->
    UpdatedConfig = init(Config),
    enable(UpdatedConfig),
    verify_collecting_status(Config, enabled),
    UpdatedConfig.


teardown(Config) ->
    teardown(Config, lfm_test_utils:get_user1_first_space_id(Config), true).


teardown(Config, SpaceId, CleanSpace) ->
    Workers = ?config(op_worker_nodes, Config),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),

    case CleanSpace of
        true -> clean_space_and_verify_stats(Config);
        false -> ok
    end,

    disable(Config),
    verify_collecting_status(Config, disabled),

    lists:foreach(fun(W) ->
        ?assertEqual(ok, rpc:call(W, dir_stats_service_state, clean, [SpaceId])),
        delete_stats(W, SpaceGuid),
        lists:foreach(fun(Incarnation) ->
            % Clean traverse data (do not assert as not all tests use initialization traverses)
            rpc:call(W, traverse_task, delete_ended, [
                <<"dir_stats_collections_initialization_traverse">>,
                dir_stats_collections_initialization_traverse:gen_task_id(SpaceId, Incarnation)
            ])
        end, lists:seq(1, 200))
    end, initializer:get_different_domain_workers(Config)),

    MinimalSyncRequest = ?config(default_minimal_sync_request, Config),
    test_utils:set_env(Workers, op_worker, minimal_sync_request, MinimalSyncRequest),

    test_utils:mock_unload(Workers, [file_meta, dir_stats_collector]).


%%%===================================================================
%%% Helper functions to be used in various suites to verify statistics
%%%===================================================================

verify_dir_on_provider_creating_files(Config, NodesSelector, Guid) ->
    [Worker | _] = ?config(NodesSelector, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),

    {ok, Children, _} = ?assertMatch({ok, _, _}, lfm_proxy:get_children_attrs(Worker, SessId,
        ?FILE_REF(Guid), #{offset => 0, limit => 100000, tune_for_large_continuous_listing => false})),

    StatsForEmptyDir = #{
        ?REG_FILE_AND_LINK_COUNT => 0,
        ?DIR_COUNT => 0,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?LOGICAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) => 0
    },
    Expectations = lists:foldl(fun
        (#file_attr{type = ?DIRECTORY_TYPE, guid = ChildGuid}, Acc) ->
            Acc2 = update_expectations_map(Acc, #{?DIR_COUNT => 1}),
            update_expectations_map(Acc2, verify_dir_on_provider_creating_files(Config, NodesSelector, ChildGuid));
        (#file_attr{size = ChildSize, mtime = ChildMTime, ctime = ChildCTime}, Acc) ->
            update_expectations_map(Acc, #{
                ?REG_FILE_AND_LINK_COUNT => 1,
                ?TOTAL_SIZE => ChildSize,
                ?LOGICAL_SIZE => ChildSize,
                ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) => ChildSize,
                update_time => max(ChildMTime, ChildCTime)
            })
    end, StatsForEmptyDir, Children),

    check_dir_stats(Config, NodesSelector, Guid, maps:remove(update_time, Expectations)),

    VerifyTimes = fun() ->
        try
            {ok, #file_attr{mtime = MTime, ctime = CTime}} = lfm_proxy:stat(Worker, SessId, ?FILE_REF(Guid)),
            CollectorTime = get_dir_update_time_stat(Worker, Guid),
            true = CollectorTime >= max(MTime, CTime),
            % Time for directory should not be earlier than time for any child
            true = CollectorTime >= maps:get(update_time, Expectations, 0),
            {ok, CollectorTime}
        catch
            Error:Reason:Stacktrace ->
                {Error, Reason, Stacktrace}
        end
    end,
    {ok, UpdateTime} = ?assertMatch({ok, _}, VerifyTimes(), ?ATTEMPTS),
    update_expectations_map(Expectations, #{update_time => UpdateTime}).


%%%===================================================================
%%% Internal functions
%%%===================================================================

delete_stats(Worker, Guid) ->
    ?assertEqual(ok, rpc:call(Worker, dir_size_stats, delete_stats, [Guid])),
    ?assertEqual(ok, rpc:call(Worker, dir_stats_collector_metadata, delete, [Guid])).


enable(Config) ->
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    lists:foreach(fun(W) ->
        ?assertEqual(ok, rpc:call(W, dir_stats_service_state, enable, [SpaceId]))
    end, initializer:get_different_domain_workers(Config)).


disable(Config) ->
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    lists:foreach(fun(W) ->
        ?assertEqual(ok, rpc:call(W, dir_stats_service_state, disable, [SpaceId]))
    end, initializer:get_different_domain_workers(Config)).


verify_collecting_status(Config, ExpectedStatus) ->
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    lists:foreach(fun(W) ->
        ?assertEqual(ExpectedStatus,
            rpc:call(W, dir_stats_service_state, get_extended_status, [SpaceId]), ?ATTEMPTS)
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
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?LOGICAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) => 0
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
    check_filled_tree(Config, {initial_size_on_provider, NodesSelector}).


check_filled_tree(Config, NodesSelector) ->
    SpaceGuid = lfm_test_utils:get_user1_first_space_guid(Config),

    % all files in paths starting with dir 2 are empty
    check_dir_stats(Config, NodesSelector, [2, 1, 1, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 3, 
        ?DIR_COUNT => 0,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?LOGICAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) => 0
    }),
    check_dir_stats(Config, NodesSelector, [2, 1, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 12,
        ?DIR_COUNT => 3,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?LOGICAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) => 0
    }),
    check_dir_stats(Config, NodesSelector, [2, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 39,
        ?DIR_COUNT => 12,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?LOGICAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) => 0
    }),
    check_dir_stats(Config, NodesSelector, [2], #{
        ?REG_FILE_AND_LINK_COUNT => 120,
        ?DIR_COUNT => 39,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 0,
        ?LOGICAL_SIZE => 0,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) => 0
    }),

    check_dir_stats(Config, NodesSelector, [1, 1, 1, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 3, 
        ?DIR_COUNT => 0,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 55,
        ?LOGICAL_SIZE => 55,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) => ?TOTAL_SIZE_ON_STORAGE_VALUE(NodesSelector, 55)
    }),
    check_dir_stats(Config, NodesSelector, [1, 1, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 12,
        ?DIR_COUNT => 3,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 104,
        ?LOGICAL_SIZE => 104,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) => ?TOTAL_SIZE_ON_STORAGE_VALUE(NodesSelector, 104)
    }),
    check_dir_stats(Config, NodesSelector, [1, 1], #{
        ?REG_FILE_AND_LINK_COUNT => 39,
        ?DIR_COUNT => 12,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 124,
        ?LOGICAL_SIZE => 124,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) => ?TOTAL_SIZE_ON_STORAGE_VALUE(NodesSelector, 124)
    }),
    check_dir_stats(Config, NodesSelector, [1], #{
        ?REG_FILE_AND_LINK_COUNT => 120,
        ?DIR_COUNT => 39,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 334,
        ?LOGICAL_SIZE => 334,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) => ?TOTAL_SIZE_ON_STORAGE_VALUE(NodesSelector, 334)
    }),

    % the space dir should have a sum of all statistics
    check_dir_stats(Config, NodesSelector, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 363,
        ?DIR_COUNT => 120,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 1334,
        ?LOGICAL_SIZE => 1334,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) => ?TOTAL_SIZE_ON_STORAGE_VALUE(NodesSelector, 1334)
    }),
    check_space_dir_values_map_and_time_series_collection(Config, NodesSelector, SpaceGuid, #{
        ?REG_FILE_AND_LINK_COUNT => 363,
        ?DIR_COUNT => 120,
        ?FILE_ERRORS_COUNT => 0,
        ?DIR_ERRORS_COUNT => 0,
        ?TOTAL_SIZE => 1334,
        ?LOGICAL_SIZE => 1334,
        ?TOTAL_SIZE_ON_STORAGE_KEY(Config, NodesSelector) => ?TOTAL_SIZE_ON_STORAGE_VALUE(NodesSelector, 1334)
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
        true = lists:all(fun(NodeUpdateTimes) -> NodeUpdateTimes =:= UpdateTimesOnFirstProvider end, AllUpdateTimes),

        lists:foldl(fun
            ({MetadataTime, not_a_dir}, {MaxMetadataTime, LastCollectorTime}) ->
                {max(MaxMetadataTime, MetadataTime), LastCollectorTime};
            ({MetadataTime, CollectorTime}, {MaxMetadataTime, LastCollectorTime}) ->
                % Time for directory should not be earlier than time for any child
                NewMaxMetadataTime = max(MaxMetadataTime, MetadataTime),
                true = CollectorTime >= NewMaxMetadataTime,
                true = CollectorTime >= LastCollectorTime,
                {NewMaxMetadataTime, CollectorTime}
        end, {0, 0}, UpdateTimesOnFirstProvider),

        ok
    catch
        Error:Reason ->
            {Error, Reason}
    end.


check_space_dir_values_map_and_time_series_collection(
    Config, Selector, SpaceGuid, _ExpectedCurrentStats, _IsCollectionEmpty, disabled = _CollectingStatus
) ->
    [Worker | _] = ?config(get_config_nodes_selector(Selector), Config),
    ?assertMatch(?ERROR_DIR_STATS_DISABLED_FOR_SPACE, rpc:call(Worker, dir_size_stats, get_stats, [SpaceGuid]));

check_space_dir_values_map_and_time_series_collection(
    Config, Selector, SpaceGuid, ExpectedCurrentStats, IsCollectionEmpty, CollectingStatus
) ->
    Attempts = case CollectingStatus of
        enabled -> 1;
        initializing -> ?ATTEMPTS
    end,
    [Worker | _] = ?config(get_config_nodes_selector(Selector), Config),
    {ok, CurrentStats} = ?assertMatch({ok, _}, rpc:call(Worker, dir_size_stats, get_stats, [SpaceGuid]), ?ATTEMPTS),
    {ok, #time_series_layout_get_result{layout = TimeStatsLayout}} = ?assertMatch({ok, _}, 
        rpc:call(Worker, dir_size_stats, browse_historical_stats_collection, [SpaceGuid, #time_series_layout_get_request{}])),
    {ok, #time_series_slice_get_result{slice = TimeStats}} = ?assertMatch({ok, _}, 
        rpc:call(Worker, dir_size_stats, browse_historical_stats_collection, [SpaceGuid, #time_series_slice_get_request{layout = TimeStatsLayout}]), Attempts),

    ?assertEqual(ExpectedCurrentStats, CurrentStats),

    case IsCollectionEmpty of
        true ->
            maps:foreach(fun(_TimeSeriesName, WindowsPerMetric) ->
                ?assertEqual(lists:duplicate(4, 0),
                    lists:map(fun([#window_info{value = Value}]) -> Value end, maps:values(WindowsPerMetric)))
            end, TimeStats);
        false ->
            maps:foreach(fun(_TimeSeriesName, WindowsPerMetric) ->
                ?assertEqual(4, maps:size(WindowsPerMetric))
            end, TimeStats)
    end.


check_dir_stats(Config, Selector, Guid, ExpectedMap) when is_binary(Guid) ->
    [Worker | _] = ?config(get_config_nodes_selector(Selector), Config),
    ?assertEqual({ok, ExpectedMap}, rpc:call(Worker, dir_size_stats, get_stats, [Guid]), ?ATTEMPTS);

check_dir_stats(Config, Selector, DirConstructor, ExpectedMap) ->
    NodesSelector = get_config_nodes_selector(Selector),
    Guid = resolve_guid(Config, NodesSelector, DirConstructor, []),
    check_dir_stats(Config, NodesSelector, Guid, ExpectedMap).


read_from_file(Config, NodesSelector, DirConstructor, FileConstructor, BytesCount) ->
    [Worker | _] = ?config(NodesSelector, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    Guid = resolve_guid(Config, NodesSelector, DirConstructor, FileConstructor),
    lfm_test_utils:read_file(Worker, SessId, Guid, BytesCount).


write_to_file(Config, NodesSelector, DirConstructor, FileConstructor, BytesCount) ->
    write_to_file(Config, NodesSelector, DirConstructor, FileConstructor, BytesCount, 0).


write_to_file(Config, NodesSelector, DirConstructor, FileConstructor, BytesCount, Offset) ->
    [Worker | _] = ?config(NodesSelector, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    Guid = resolve_guid(Config, NodesSelector, DirConstructor, FileConstructor),
    lfm_test_utils:write_file(Worker, SessId, Guid, Offset, {rand_content, BytesCount}).


resolve_guid(Config, NodesSelector, DirConstructor, FileConstructor) ->
    resolve_guid(Config, NodesSelector, DirConstructor, FileConstructor, 1).

resolve_guid(Config, NodesSelector, DirConstructor, FileConstructor, Attempts) ->
    #file_attr{guid = Guid} = resolve_attrs(Config, NodesSelector, DirConstructor, FileConstructor, Attempts),
    Guid.


resolve_update_times_in_metadata_and_stats(Config, NodesSelector, DirConstructor, FileConstructor) ->
    #file_attr{guid = Guid, mtime = MTime, ctime = CTime} =
        resolve_attrs(Config, NodesSelector, DirConstructor, FileConstructor, 1),

    DirUpdateTime = case FileConstructor of
        [] ->
            [Worker | _] = ?config(NodesSelector, Config),
            get_dir_update_time_stat(Worker, Guid);
        _ ->
            not_a_dir
    end,

    {max(MTime, CTime), DirUpdateTime}.


resolve_attrs(Config, NodesSelector, DirConstructor, FileConstructor, Attempts) ->
    [Worker | _] = ?config(NodesSelector, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    SpaceName = lfm_test_utils:get_user1_first_space_name(Config),

    DirPath = build_path(filename:join([<<"/">>, SpaceName]), DirConstructor, "dir"),
    Path = build_path(DirPath, FileConstructor, "file"),
    {ok, Attrs} = ?assertMatch({ok, _}, lfm_proxy:stat(Worker, SessId, {path, Path}), Attempts),
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
    {ok, CollectorTime} = rpc:call(Worker, dir_update_time_stats, get_update_time, [Guid]),
    CollectorTime.


mock_file_listing(Config, Uuid, SleepTime) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Master = self(),
    Tag = make_ref(),
    ok = test_utils:mock_new(Worker, file_listing, [passthrough]),
    ok = test_utils:mock_expect(Worker, file_listing, list, fun
        (FileUuid, ListOpts) when FileUuid =:= Uuid ->
            Ans = meck:passthrough([FileUuid, ListOpts]),
            Master ! {space_children_listed, Tag},
            timer:sleep(SleepTime),
            Ans;
        (FileUuid, ListOpts) ->
            meck:passthrough([FileUuid, ListOpts])
    end),
    Tag.


execute_file_listing_hook(Tag, Hook) ->
    MessageReceived = receive
        {space_children_listed, Tag} ->
            Hook(),
            ok
    after
        10000 -> timeout
    end,
    ?assertEqual(ok, MessageReceived).


hang_stopping(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Worker, dir_stats_collector, [passthrough]),
    ok = test_utils:mock_expect(Worker, dir_stats_collector, stop_collecting, fun(_SpaceId) -> ok end),
    
    disable(Config),
    test_utils:mock_unload(Worker, [dir_stats_collector]),
    verify_collecting_status(Config, stopping).


execute_restart_hooks(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, restart_hooks, maybe_execute_hooks, [])).


reset_restart_hooks(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, node_cache, clear, [restart_hooks_status])).


clean_space_and_verify_stats(Config) ->
    [Worker2 | _] = Workers = ?config(op_worker_nodes, Config),
    SpaceId = lfm_test_utils:get_user1_first_space_id(Config),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),

    lfm_test_utils:clean_space([Worker2], SpaceId, 30),
    lists:foreach(fun(Worker) ->
        case rpc:call(Worker, dir_stats_service_state, get_extended_status, [SpaceId]) of
            enabled ->
                {ok, StorageId} = rpc:call(Worker, space_logic, get_local_supporting_storage, [SpaceId]),
                ?assertEqual({ok, #{
                    ?REG_FILE_AND_LINK_COUNT => 0,
                    ?DIR_COUNT => 0,
                    ?FILE_ERRORS_COUNT => 0,
                    ?DIR_ERRORS_COUNT => 0,
                    ?TOTAL_SIZE => 0,
                    ?LOGICAL_SIZE => 0,
                    ?SIZE_ON_STORAGE(StorageId) => 0
                }}, rpc:call(Worker, dir_size_stats, get_stats, [SpaceGuid]), ?ATTEMPTS),

                TmpDirGuid = fslogic_file_id:spaceid_to_tmp_dir_guid(SpaceId),
                ?assertEqual({ok, #{
                    ?REG_FILE_AND_LINK_COUNT => 0,
                    ?DIR_COUNT => 1, % includes dir for opened deleted dirs (created with space)
                    ?FILE_ERRORS_COUNT => 0,
                    ?DIR_ERRORS_COUNT => 0,
                    ?TOTAL_SIZE => 0,
                    ?LOGICAL_SIZE => 0,
                    ?SIZE_ON_STORAGE(StorageId) => 0
                }}, rpc:call(Worker, dir_size_stats, get_stats, [TmpDirGuid]), ?ATTEMPTS);
            _ ->
                ok
        end
    end, Workers).


append_files(Config, NodesSelector, Guids, CurrentSize) ->
    [Worker | _] = ?config(NodesSelector, Config),
    SessId = lfm_test_utils:get_user1_session_id(Config, Worker),
    BytesToAppend = 10,
    lists:foreach(fun(Guid) ->
        lfm_test_utils:write_file(Worker, SessId, Guid, CurrentSize, {rand_content, BytesToAppend})
    end, Guids),
    CurrentSize + BytesToAppend.


get_config_nodes_selector({_NodeType, ConfigNodeSelector}) ->
    ConfigNodeSelector;
get_config_nodes_selector(ConfigNodeSelector) ->
    ConfigNodeSelector.