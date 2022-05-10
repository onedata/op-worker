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
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/dataset/archivisation_tree.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([get_user1_session_id/2, get_user1_first_space_id/1, get_user1_first_space_guid/1, get_user1_first_space_name/1,
    get_user1_first_storage_id/2]).
-export([create_file/4, create_file/5, write_file/4, write_file/5, create_and_write_file/6, read_file/4]).
-export([create_files_tree/4]).
-export([clean_space/3, clean_space/4, assert_space_and_trash_are_empty/3, assert_space_dir_empty/3]).

% TODO VFS-7215 - merge this module with file_ops_test_utils

-type file_type() :: binary(). % <<"file">> | <<"dir">>

%%%===================================================================
%%% API operating on Config map
%%% NOTE: <<"user1">> is considered default user
%%%===================================================================

get_user1_session_id(Config, Worker) ->
    ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config).


get_user1_first_space_id(Config) ->
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    SpaceId.


get_user1_first_space_guid(Config) ->
    fslogic_uuid:spaceid_to_space_dir_guid(get_user1_first_space_id(Config)).


get_user1_first_space_name(Config) ->
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    SpaceName.


get_user1_first_storage_id(Config, NodesSelector) ->
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    [Worker | _] = ?config(NodesSelector, Config),
    {ok, StorageId} = rpc:call(Worker, space_logic, get_local_supporting_storage, [SpaceId]),
    StorageId.


%%%===================================================================
%%% API functions
%%%===================================================================

-spec create_file(file_type(), node(), session:id(), file_meta:path()) ->
    {ok, file_id:file_guid()} | {error, term()}.
create_file(FileType, Node, SessId, Path) ->
    create_file(FileType, Node, SessId, Path, 8#777).


-spec create_file(file_type(), node(), session:id(), file_meta:path(), file_meta:mode()) ->
    {ok, file_id:file_guid()} | {error, term()}.
create_file(<<"file">>, Node, SessId, Path, Mode) ->
    lfm_proxy:create(Node, SessId, Path, Mode);
create_file(<<"dir">>, Node, SessId, Path, Mode) ->
    lfm_proxy:mkdir(Node, SessId, Path, Mode).


-spec write_file(node(), session:id(), file_id:file_guid(), binary() | {rand_content, non_neg_integer()}) ->
    ok.
write_file(Worker, SessId, FileGuid, ContentSpec) ->
    write_file(Worker, SessId, FileGuid, 0, ContentSpec).

-spec write_file(node(), session:id(), file_id:file_guid(), non_neg_integer(),
    binary() | {rand_content, non_neg_integer()}) -> ok.
write_file(Worker, SessId, FileGuid, Offset, {rand_content, Size}) ->
    write_file(Worker, SessId, FileGuid, Offset, crypto:strong_rand_bytes(Size));
write_file(Worker, SessId, FileGuid, Offset, Data) when is_binary(Data) ->
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(FileGuid), write)),
    ?assertMatch({ok, _}, lfm_proxy:write(Worker, Handle, Offset, Data)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle)).


-spec create_and_write_file(node(), session:id(), fslogic_worker:file_guid(), file_meta:name(), non_neg_integer(),
    binary() | {rand_content, non_neg_integer()}) -> ok.
create_and_write_file(Worker, SessId, ParentGuid, ChildFileName, Offset, {rand_content, Size}) ->
    create_and_write_file(Worker, SessId, ParentGuid, ChildFileName, Offset, crypto:strong_rand_bytes(Size));
create_and_write_file(Worker, SessId, ParentGuid, ChildFileName, Offset, Data) when is_binary(Data) ->
    {ok, {_, Handle}} = ?assertMatch({ok, _},
        lfm_proxy:create_and_open(Worker, SessId, ParentGuid, ChildFileName, ?DEFAULT_FILE_MODE)),
    ?assertMatch({ok, _}, lfm_proxy:write(Worker, Handle, Offset, Data)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle)).


-spec read_file(node(), session:id(), file_id:file_guid(), Size :: non_neg_integer()) ->
    binary().
read_file(Worker, SessId, FileGuid, Size) ->
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, ?FILE_REF(FileGuid), read)),
    {ok, Bytes} = ?assertMatch({ok, _}, lfm_proxy:read(Worker, Handle, 0, Size)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle)),
    Bytes.


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
    rm_recursive(CleaningWorker, ?ROOT_SESS_ID, SpaceGuid, BatchSize, false),
    % TODO VFS-7064 remove below line after introducing link to trash directory
    rm_recursive(CleaningWorker, ?ROOT_SESS_ID, fslogic_uuid:spaceid_to_trash_dir_guid(SpaceId), BatchSize, false),
    ArchivesDirGuid = file_id:pack_guid(?ARCHIVES_ROOT_DIR_UUID(SpaceId), SpaceId),
    rm_recursive(CleaningWorker, ?ROOT_SESS_ID, ArchivesDirGuid, BatchSize, true),
    assert_space_and_trash_are_empty(AllWorkers, SpaceId, Attempts).

assert_space_dir_empty(Workers, SpaceId, Attempts) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    lists:foreach(fun(W) ->
        ?assertMatch({ok, []},
            % TODO VFS-7064 after introducing link to trash directory this function must be adapted
            lfm_proxy:get_children(W, ?ROOT_SESS_ID, ?FILE_REF(SpaceGuid), 0, 10), Attempts)
    end, utils:ensure_list(Workers)).


assert_space_and_trash_are_empty(Workers, SpaceId, Attempts) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    lists:foreach(fun(W) ->
        case opw_test_rpc:supports_space(W, SpaceId) of
            true ->
                ?assertMatch({ok, []},
                    lfm_proxy:get_children(W, ?ROOT_SESS_ID, ?FILE_REF(SpaceGuid), 0, 100), Attempts),
                % trash directory should be empty
                ?assertMatch({ok, []},
                    lfm_proxy:get_children(W, ?ROOT_SESS_ID, ?FILE_REF(fslogic_uuid:spaceid_to_trash_dir_guid(SpaceId)), 0, 100), Attempts);
                % TODO VFS-7809 Check why sometimes after cleanup in tests, space capacity is not equal to 0
                % ?assertEqual(0, opw_test_rpc:get_space_capacity_usage(W, SpaceId), Attempts);
            false ->
                ok
        end
    end, utils:ensure_list(Workers)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

rm_recursive(Worker, SessId, DirGuid, BatchSize, DeleteDir) ->
    rm_recursive(Worker, SessId, DirGuid, BatchSize, DeleteDir, #{tune_for_large_continuous_listing => true}).

rm_recursive(Worker, SessId, DirGuid, BatchSize, DeleteDir, BaseListOpts) ->
    ListOpts = BaseListOpts#{limit => BatchSize},
    case lfm_proxy:get_children(Worker, SessId, ?FILE_REF(DirGuid), ListOpts) of
        {ok, GuidsAndNames, ListingPaginationToken} -> 
            case rm_files(Worker, SessId, GuidsAndNames, BatchSize) of
                ok ->
                    case file_listing:is_finished(ListingPaginationToken) of
                        true when DeleteDir -> 
                            lfm_proxy:unlink(Worker, SessId, ?FILE_REF(DirGuid));
                        true -> 
                            ok;
                        false -> 
                            NextListingOpts = #{pagination_token => ListingPaginationToken},
                            rm_recursive(Worker, SessId, DirGuid, NextListingOpts, BatchSize, DeleteDir)
                    end;
                Error ->
                    ct:print("Error during space cleanup [rm_files]: ~p", [Error]),
                    Error
            end;
        {error, enoent} -> 
            ok;
        Error2 ->
            ct:print("Error during space cleanup [get_children]: ~p", [Error2]),
            Error2
    end.


rm_files(Worker, SessId, GuidsAndPaths, BatchSize) ->
    Results = lists:map(fun({G, Name}) ->
        case Name =:= ?TRASH_DIR_NAME of
            true ->
                rm_recursive(Worker, SessId, G, BatchSize, false);
            false ->
                case lfm_proxy:is_dir(Worker, SessId, ?FILE_REF(G)) of
                    true ->
                        rm_recursive(Worker, SessId, G, BatchSize, true);
                    false ->
                        lfm_proxy:unlink(Worker, SessId, ?FILE_REF(G));
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