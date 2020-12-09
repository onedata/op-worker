%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_test_utils).
-author("Jakub Kudzia").


-include("lfm_test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([clean_space/3, ensure_space_and_trash_are_empty/3, is_space_dir_empty/4]).
-export([create_files_tree/4]).

% TODO merge this module with file_ops_test_utils

%%%===================================================================
%%% API functions
%%%===================================================================

create_files_tree(Worker, SessId, Structure, RootGuid) ->
    create_files_tree(Worker, SessId, Structure, RootGuid, <<"dir">>, <<"file">>, [], []).

clean_space(Worker, SpaceId, Attempts) when is_atom(Worker) ->
    clean_space([Worker], SpaceId, Attempts);
clean_space(Workers, SpaceId, Attempts) when is_list(Workers) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    BatchSize = 1000,
    lists:foreach(fun(W) -> lfm_proxy:close_all(W) end, Workers),
    CleaningWorker = lists_utils:random_element(Workers),
    clean_dir(CleaningWorker, ?ROOT_SESS_ID, SpaceGuid, undefined, BatchSize),
    % TODO VFS-7064 remove below line after introducing link to trash directory
    clean_dir(CleaningWorker, ?ROOT_SESS_ID, fslogic_uuid:spaceid_to_trash_dir_guid(SpaceId), undefined, BatchSize),
    ensure_space_and_trash_are_empty(Workers, SpaceId, Attempts).

is_space_dir_empty(Worker, SessId, SpaceId, Attempts) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    ?assertMatch({ok, []},
        % TODO VFS-7064 after introducing link to trash directory this function must be adapted
        lfm_proxy:get_children(Worker, SessId, {guid, SpaceGuid}, 0, 10), Attempts).


ensure_space_and_trash_are_empty(Workers, SpaceId, Attempts) ->
    Guid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    lists:foreach(fun(W) ->
        ?assertMatch({ok, []},
            lfm_proxy:get_children(W, ?ROOT_SESS_ID, {guid, Guid}, 0, 100), Attempts),
        % trash directory should be empty
        ?assertMatch({ok, []},
            lfm_proxy:get_children(W, ?ROOT_SESS_ID, {guid, fslogic_uuid:spaceid_to_trash_dir_guid(SpaceId)}, 0, 100), Attempts),
        ?assertEqual(0, space_occupation(W, SpaceId), Attempts)
    end, Workers).

space_occupation(Worker, SpaceId) ->
    rpc:call(Worker, space_quota, current_size, [SpaceId]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

clean_dir(Worker, SessId, DirGuid, Token, BatchSize) ->
    {ok, GuidsAndNames, Token2, IsLast} = lfm_proxy:get_children(Worker, SessId, {guid, DirGuid}, 0, BatchSize, Token),
    delete_files(Worker, SessId, GuidsAndNames, BatchSize),
    case IsLast of
        true ->
            ok;
        false ->
            clean_dir(Worker, SessId, DirGuid, Token2, BatchSize)
    end.


delete_files(Worker, SessId, GuidsAndPaths, BatchSize) ->
    lists:foreach(fun({G, Name}) ->
        case Name =:= ?TRASH_DIR_NAME of
            true ->
                clean_dir(Worker, SessId, G, undefined, BatchSize);
            false ->
                lfm_proxy:rm_recursive(Worker, SessId, {guid, G})
        end
    end, GuidsAndPaths).


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



