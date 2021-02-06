%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Util functions for operation on files using lfm_proxy.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_test_utils).
-author("Jakub Kudzia").

-include("lfm_test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([clean_space/3, clean_space/4, assert_space_and_trash_are_empty/3, assert_space_dir_empty/3]).
-export([create_files_tree/4]).

% TODO VFS-7215 - merge this module with file_ops_test_utils

%%%===================================================================
%%% API functions
%%%===================================================================

create_files_tree(Worker, SessId, Structure, RootGuid) ->
    create_files_tree(Worker, SessId, Structure, RootGuid, <<"dir">>, <<"file">>, [], []).

clean_space(Workers, SpaceId, Attempts) ->
    Workers2 = utils:ensure_list(Workers),
    CleaningWorker = lists_utils:random_element(Workers2),
    clean_space(CleaningWorker, Workers2, SpaceId, Attempts).

clean_space(CleaningWorker, AllWorkers, SpaceId, Attempts) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    BatchSize = 1000,
    lists:foreach(fun(W) -> lfm_proxy:close_all(W) end, AllWorkers),
    rm_recursive(CleaningWorker, ?ROOT_SESS_ID, SpaceGuid, <<>>, BatchSize, false),
    % TODO VFS-7064 remove below line after introducing link to trash directory
    rm_recursive(CleaningWorker, ?ROOT_SESS_ID, fslogic_uuid:spaceid_to_trash_dir_guid(SpaceId), <<>>, BatchSize, false),
    assert_space_and_trash_are_empty(AllWorkers, SpaceId, Attempts).

assert_space_dir_empty(Workers, SpaceId, Attempts) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    lists:foreach(fun(W) ->
        ?assertMatch({ok, []},
            % TODO VFS-7064 after introducing link to trash directory this function must be adapted
            lfm_proxy:get_children(W, ?ROOT_SESS_ID, {guid, SpaceGuid}, 0, 10), Attempts)
    end, utils:ensure_list(Workers)).


assert_space_and_trash_are_empty(Workers, SpaceId, Attempts) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    lists:foreach(fun(W) ->
        case opw_test_rpc:supports_space(W, SpaceId) of
            true ->
                ?assertMatch({ok, []},
                    lfm_proxy:get_children(W, ?ROOT_SESS_ID, {guid, SpaceGuid}, 0, 100), Attempts),
                % trash directory should be empty
                ?assertMatch({ok, []},
                    lfm_proxy:get_children(W, ?ROOT_SESS_ID, {guid, fslogic_uuid:spaceid_to_trash_dir_guid(SpaceId)}, 0, 100), Attempts),
                ?assertEqual(0, opw_test_rpc:get_space_capacity_usage(W, SpaceId), Attempts);
            false ->
                ok
        end
    end, utils:ensure_list(Workers)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

rm_recursive(Worker, SessId, DirGuid, Token, BatchSize) ->
    rm_recursive(Worker, SessId, DirGuid, Token, BatchSize, true).

rm_recursive(Worker, SessId, DirGuid, Token, BatchSize, DeleteDir) ->
    case lfm_proxy:get_children(Worker, SessId, {guid, DirGuid}, 0, BatchSize, Token) of
        {ok, GuidsAndNames, Token2, IsLast} ->
            case rm_files(Worker, SessId, GuidsAndNames, BatchSize) of
                ok ->
                    case IsLast of
                        true when DeleteDir -> lfm_proxy:unlink(Worker, SessId, {guid, DirGuid});
                        true -> ok;
                        false -> rm_recursive(Worker, SessId, DirGuid, Token2, BatchSize, DeleteDir)
                    end;
                Error ->
                    Error
            end;
        Error2 ->
            Error2
    end.


rm_files(Worker, SessId, GuidsAndPaths, BatchSize) ->
    Results = lists:map(fun({G, Name}) ->
        case Name =:= ?TRASH_DIR_NAME of
            true ->
                rm_recursive(Worker, SessId, G, <<>>, BatchSize, false);
            false ->
                case lfm_proxy:is_dir(Worker, SessId, {guid, G}) of
                    true ->
                        rm_recursive(Worker, SessId, G, <<>>, BatchSize);
                    false ->
                        lfm_proxy:unlink(Worker, SessId, {guid, G});
                    {error, not_found} -> ok;
                    Error -> Error
                end
        end
    end, GuidsAndPaths),

    lists:foldl(fun
        (_, FirsError = {error, _}) -> FirsError;
        (ok, ok) -> ok;
        (Error = {error, _}, ok) -> Error
    end, ok, Results).



create_files_tree(_Worker, _SessId, [], _RootGuid, _DirPrefix, _FilePrefix, DirGuids, FileGuids) ->
    {DirGuids, FileGuids};
create_files_tree(Worker, SessId, [{DirsCount, FilesCount} | Rest], RootGuid, DirPrefix, FilePrefix,
    DirGuids, FileGuids
) ->
    NewDirGuids = create_dirs(Worker, SessId, RootGuid, DirPrefix, DirsCount),
    NewFileGuids = create_files(Worker, SessId, RootGuid, FilePrefix, FilesCount),
    lists:foldl(fun(ChildDirGuid, {DirGuidsAcc, FileGuidsAcc}) ->
        create_files_tree(Worker, SessId, Rest, ChildDirGuid, DirPrefix, FilePrefix, DirGuidsAcc, FileGuidsAcc)
    end, {DirGuids ++ NewDirGuids, FileGuids ++ NewFileGuids}, NewDirGuids).


create_dirs(Worker, SessId, ParentGuid, DirPrefix, DirsCount) ->
    create_children(DirPrefix, DirsCount, fun(ChildDirName) ->
        {ok, Guid} = lfm_proxy:mkdir(Worker, SessId, ParentGuid, ChildDirName, ?DEFAULT_DIR_PERMS),
        Guid
    end).


create_files(Worker, SessId, ParentGuid, FilePrefix, FilesCount) ->
    create_children(FilePrefix, FilesCount, fun(ChildFileName) ->
        {ok, {Guid, Handle}} = lfm_proxy:create_and_open(Worker, SessId, ParentGuid, ChildFileName, ?DEFAULT_FILE_MODE),
        ok = lfm_proxy:close(Worker, Handle),
        Guid
    end).


create_children(ChildPrefix, ChildCount, CreateFun) ->
    lists:map(fun(N) ->
        ChildName = str_utils:format_bin("~s_~p", [ChildPrefix, N]),
        CreateFun(ChildName)
    end, lists:seq(1, ChildCount)).