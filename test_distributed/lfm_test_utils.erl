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
-export([clean_space/3, ensure_space_empty/3]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%create_files_tree(Worker, SessId, Structure, RootGuid, RootPath) ->
%%    create_files_tree(Worker, SessId, Structure, RootGuid, RootPath, <<"dir">>, <<"file">>).
%%
%%create_files_tree(Worker, SessId, Structure, RootGuid, DirPrefix, FilePrefix) ->
%%    create_files_tree(Worker, SessId, Structure, RootGuid, DirPrefix, FilePrefix, []).

clean_space(Worker, SpaceId, Attempts) when is_atom(Worker) ->
    clean_space([Worker], SpaceId, Attempts);
clean_space(Workers, SpaceId, Attempts) when is_list(Workers) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    BatchSize = 1000,
    lists:foreach(fun(W) -> lfm_proxy:close_all(W) end, Workers),
    clean_space(lists_utils:random_element(Workers), ?ROOT_SESS_ID, SpaceGuid, 0, BatchSize),
    ensure_space_empty(Workers, SpaceId, Attempts).


ensure_space_empty(Workers, SpaceId, Attempts) ->
    Guid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    lists:foreach(fun(W) ->
        ?assertMatch({ok, []}, lfm_proxy:get_children(W, ?ROOT_SESS_ID, {guid, Guid}, 0, 1), Attempts),
        ?assertEqual(0, space_occupation(W, SpaceId), Attempts)
    end, Workers).

space_occupation(Worker, SpaceId) ->
    rpc:call(Worker, space_quota, current_size, [SpaceId]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

clean_space(Worker, SessId, SpaceGuid, Offset, BatchSize) ->
    {ok, GuidsAndPaths} = lfm_proxy:get_children(Worker, SessId, {guid, SpaceGuid}, Offset, BatchSize),
    FilesNum = length(GuidsAndPaths),
    delete_files(Worker, SessId, GuidsAndPaths),
    case FilesNum < BatchSize of
        true ->
            ok;
        false ->
            clean_space(Worker, SessId, SpaceGuid, Offset + BatchSize, BatchSize)
    end.

delete_files(Worker, SessId, GuidsAndPaths) ->
    lists:foreach(fun({G, _P}) ->
        lfm_proxy:rm_recursive(Worker, SessId, {guid, G})
    end, GuidsAndPaths).


%%create_files_tree(_Worker, _SessId, [], _RootGuid, _DirPrefix, _FilePrefix, Guids) ->
%%    Guids;
%%create_files_tree(Worker, SessId, [{DirsCount, FilesCount} | Rest], RootGuid, DirPrefix, FilePrefix,
%%    CreatedStructure, DirGuids, FileGuids
%%) ->
%%    FileGuids = create_files(Worker, SessId, RootGuid, FilePrefix, FilesCount),
%%    DirGuids = create_dirs(Worker, SessId, RootGuid, FilePrefix, FilesCount),
%%
%%
%%
%%
%%
%%create_files(Worker, SessId, ParentGuid, FilePrefix, FilesCount) ->
%%    lists:map(fun(N) ->
%%        FileName = str_utils:format_bin("~s_~d", [FilePrefix, N]),
%%        {ok, {Guid, Handle}} = lfm_proxy:create_and_open(Worker, SessId, ParentGuid, FileName, ?DEFAULT_FILE_MODE),
%%        lfm_proxy:close(Worker, Handle),
%%        {FileName, Guid}
%%    end, lists:seq(1, FilesCount)).
%%
%%create_subdirs(Worker, SessId, TreeStructure, ParentGuid, DirPrefix, FilePrefix, DirsCount) ->
%%    lists:map(fun(N) ->
%%        DirName = str_utils:format_bin("~s_~d", [DirPrefix, N]),
%%        {ok, Guid} = lfm_proxy:mkdir(Worker, SessId, ParentGuid, DirName, ?DEFAULT_DIR_MODE),
%%        {Subtree, DirGuids, FileGuids} =
%%            create_files_tree(Worker, SessId, TreeStructure, Guid, DirName, DirPrefix, FilePrefix),
%%        Guid
%%    end, lists:seq(1, DirsCount)).
