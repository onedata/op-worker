%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of deleting a file that is, or is about to be, open. Such a file
%%% disappears from the file tree at once, but its storage file must live on
%%% until the last handle to it is released - readers and writers already holding
%%% one keep working on it, while the name it occupied is free to be taken over
%%% by a new file.
%%%
%%% How the storage file survives depends on the storage: one that supports
%%% rename has it moved to a hidden directory in the root of the storage, while
%%% one that does not has it left in place under a deletion marker (see
%%% fslogic_delete:get_open_file_handling_method/1).
%%%
%%% The bodies are dispatched to from file_lifecycle_test_SUITE, which gives every
%%% case a space of its own backed by a freshly created storage - hence the
%%% storage contents can be asserted in absolute terms.
%%% @end
%%%-------------------------------------------------------------------
-module(file_deletion_tests).
-author("Michal Wrzeszcz").

-include("file/file_lifecycle_test.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% tests
-export([
    delete_during_open_with_deletion_marker_test/1,
    delete_during_open_with_storage_rename_test/1,

    delete_of_opened_file_moves_it_on_storage_test/1,
    name_of_deleted_opened_file_can_be_reused_test/1,
    release_before_deleted_file_is_moved_on_storage_test/1,
    release_after_deleted_file_is_moved_on_storage_test/1,

    rename_to_opened_file_test/1
]).

% marks the process dictionary of a mocked call that has already been suspended,
% so that a retry of the very same operation is let through
-define(SUSPENDED_ONCE, suspended_once).

-define(ATTEMPTS, 30).

-define(FILE_CONTENT, <<"file_content">>).


%%%====================================================================
%%% Test functions concerning deletion during an open
%%%====================================================================


%% NOTE: runs on an object storage, which cannot rename a storage file and so
%% takes the deletion marker method (fslogic_delete:get_open_file_handling_method/1
%% -> helper:is_rename_supported/1) of its own accord - no mock involved.
delete_during_open_with_deletion_marker_test(Config) ->
    Node = file_lifecycle_test_utils:get_node(),

    {_FileGuid, StorageId, StorageFileId} = delete_during_open_test_base(Config),

    % with the deletion marker method the storage file stays right where it was
    % until the handle is released
    ?assertMatch({ok, _},
        storage_file_tree_test_utils:stat(?PROVIDER_SELECTOR, StorageId, StorageFileId)),

    % releasing the handle removes it, marker and all; unlike the rename method,
    % whose cleanup on release the other cases already cover, this path has no
    % storage move to sequence it, hence the attempts
    ?assertEqual(ok, lfm_proxy:close_all(Node)),
    ?assertEqual({error, ?ENOENT},
        storage_file_tree_test_utils:stat(?PROVIDER_SELECTOR, StorageId, StorageFileId), ?ATTEMPTS).


delete_during_open_with_storage_rename_test(Config) ->
    Node = file_lifecycle_test_utils:get_node(),
    SpaceId = ?SPACE_ID(Config),

    {FileGuid, StorageId, StorageFileId} = delete_during_open_test_base(Config),

    % a POSIX storage supports rename, hence the method that moves the storage
    % file to a hidden directory the moment the file is deleted
    ?assertEqual({error, ?ENOENT},
        storage_file_tree_test_utils:stat(?PROVIDER_SELECTOR, StorageId, StorageFileId)),
    ?assertEqual([], file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)),
    ?assertEqual([FileGuid],
        file_lifecycle_test_utils:list_deleted_open_files_on_storage(Node, SpaceId)),

    ?assertEqual(ok, lfm_proxy:close_all(Node)),
    ?assertEqual([], file_lifecycle_test_utils:list_deleted_open_files_on_storage(Node, SpaceId)).


%%%====================================================================
%%% Test functions concerning deletion of an opened file
%%%====================================================================


delete_of_opened_file_moves_it_on_storage_test(Config) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    {FileGuid, FilePath} = file_lifecycle_test_utils:create_and_open_file(Node, SessId, SpaceId),
    ?assertEqual([filename:basename(FilePath)],
        file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)),

    ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, ?FILE_REF(FileGuid))),

    % the file is gone from the file tree, but as long as it is open its storage
    % file lives on, moved to a hidden directory in the root of the storage
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, SessId, ?FILE_REF(FileGuid))),
    ?assertEqual([], file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)),
    ?assertEqual([FileGuid],
        file_lifecycle_test_utils:list_deleted_open_files_on_storage(Node, SpaceId)),

    ExpStorageFileId = filename:join([?DELETED_OPENED_FILES_DIR, FileGuid]),
    ?assertMatch({ok, #file_location{file_id = ExpStorageFileId}},
        lfm_proxy:get_file_location(Node, SessId, ?FILE_REF(FileGuid))),

    % releasing the last handle finally removes the storage file
    ?assertEqual(ok, lfm_proxy:close_all(Node)),
    ?assertEqual([], file_lifecycle_test_utils:list_deleted_open_files_on_storage(Node, SpaceId)).


name_of_deleted_opened_file_can_be_reused_test(Config) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    {FileGuid, FilePath} = file_lifecycle_test_utils:create_and_open_file(Node, SessId, SpaceId),
    FileName = filename:basename(FilePath),

    ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, ?FILE_REF(FileGuid))),
    ?assertEqual([], file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)),
    ?assertEqual([FileGuid],
        file_lifecycle_test_utils:list_deleted_open_files_on_storage(Node, SpaceId)),

    % the freed name can be taken over by a new file tree entry, whose storage
    % file takes the very path the deleted one used to occupy
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, FilePath)),
    ChildPath = filename:join([FilePath, generator:gen_name()]),
    {ok, {ChildGuid, _}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(Node, SessId, ChildPath)),
    ?assertEqual([FileName], file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)),

    ?assertEqual(ok, lfm_proxy:close_all(Node)),
    ?assertEqual([], file_lifecycle_test_utils:list_deleted_open_files_on_storage(Node, SpaceId)),

    ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, ?FILE_REF(ChildGuid))),
    ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, ?FILE_REF(DirGuid))),
    ?assertEqual([], file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)).


release_before_deleted_file_is_moved_on_storage_test(Config) ->
    release_during_deletion_of_opened_file_test_base(Config, before_move).


release_after_deleted_file_is_moved_on_storage_test(Config) ->
    release_during_deletion_of_opened_file_test_base(Config, after_move).


%%%====================================================================
%%% Test functions awaiting the product to catch up
%%%====================================================================


%% TODO VFS-5290 - moving a file onto an opened one must not remove the latter
%% from the storage while it is still open (see sd_utils:rename_storage_file)
rename_to_opened_file_test(Config) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    % the target is left open, so its storage file must survive being overwritten
    {_, TargetPath} = file_lifecycle_test_utils:create_and_open_file(Node, SessId, SpaceId),

    SourcePath = file_lifecycle_test_utils:build_file_path(Node, SessId, SpaceId),
    {ok, {_, SourceHandle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        Node, SessId, SourcePath
    )),
    ?assertEqual(ok, lfm_proxy:close(Node, SourceHandle)),

    ?assertMatch({ok, _}, lfm_proxy:mv(Node, SessId, {path, SourcePath}, TargetPath)),

    ?assertEqual(2, file_lifecycle_test_utils:count_space_files_on_storage(Node, SpaceId)).


%%%===================================================================
%%% Test base functions
%%%===================================================================


%% @private
%% @doc
%% Deletes a file while it is being opened - the open is suspended halfway
%% through, after the storage file has been opened - and checks that the open
%% still succeeds. What becomes of the storage file depends on the method of
%% handling deletion of an opened file and is asserted by the calling test case,
%% for which the file and the location of its storage file are returned. NOTE:
%% the location is resolved before the deletion and keeps pointing at where the
%% storage file was back then; that a storage file was there to begin with is
%% asserted here, so that a later ENOENT can only mean it went away.
%% @end
-spec delete_during_open_test_base(test_config:config()) ->
    {file_id:file_guid(), storage:id(), helpers:file_id()} | no_return().
delete_during_open_test_base(Config) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),
    Master = self(),

    % the provider opens files on storage only when the session does not use
    % direct IO; the space is discarded after the case, so there is nothing to restore
    ?assertEqual(ok, rpc:call(Node, session, set_direct_io, [SessId, SpaceId, false])),

    {FileGuid, FilePath} = file_lifecycle_test_utils:create_file(Node, SessId, SpaceId),
    {StorageId, StorageFileId} = file_lifecycle_test_utils:locate_on_storage(
        Node, SpaceId, FileGuid
    ),

    % NOTE: the file is given content before anything else, so that it has a
    % storage file at all. A POSIX storage gets one as soon as the file is opened,
    % but an object storage has no empty objects - it only gets one once something
    % is written (which is why file_lfm_s3_test_SUITE skips the case asserting
    % that opening a file creates it on the storage).
    {ok, WriteHandle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), write)),
    ?assertMatch({ok, _}, lfm_proxy:write(Node, WriteHandle, 0, ?FILE_CONTENT)),
    ?assertEqual(ok, lfm_proxy:close(Node, WriteHandle)),
    ?assertMatch({ok, _},
        storage_file_tree_test_utils:stat(?PROVIDER_SELECTOR, StorageId, StorageFileId)),

    file_lifecycle_test_utils:mock(Node, file_req, open_on_storage,
        fun(UserCtx, FileCtx, SessionId, Flag, HandleId) ->
            Ans = meck:passthrough([UserCtx, FileCtx, SessionId, Flag, HandleId]),
            case file_ctx:get_space_id_const(FileCtx) of
                SpaceId -> ?SUSPEND_UNTIL_RESUMED(Master);
                _ -> ok
            end,
            Ans
        end
    ),

    [OpenResult] = file_lifecycle_test_utils:open_asynchronously(Node, SessId, FileGuid, 1),

    SuspendedProc = file_lifecycle_test_utils:await_suspension(),
    ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, {path, FilePath})),
    file_lifecycle_test_utils:resume(SuspendedProc),

    ?assertMatch({ok, _}, OpenResult()),
    {FileGuid, StorageId, StorageFileId}.


%% @private
%% @doc
%% Releases the handle to a deleted file while its storage file is being moved
%% to the hidden directory for deleted open files - either just before or just
%% after the move itself. Whichever order the two end up in, nothing may be left
%% behind on the storage.
%% @end
-spec release_during_deletion_of_opened_file_test_base(
    test_config:config(), before_move | after_move
) ->
    ok | no_return().
release_during_deletion_of_opened_file_test_base(Config, When) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),
    Master = self(),

    {FileGuid, _FilePath} = file_lifecycle_test_utils:create_and_open_file(Node, SessId, SpaceId),
    TargetFileId = filename:join([?DELETED_OPENED_FILES_DIR, FileGuid]),

    % NOTE: the storage of every test case starts out without the hidden directory,
    % so the first move fails with ENOENT and the provider retries it after creating
    % the directory (see fslogic_delete:maybe_rename_storage_file/1). Exactly one of
    % the two attempts may be suspended - the first one when the release is to come
    % before the move, and the one that actually succeeded when it is to come after.
    file_lifecycle_test_utils:mock(Node, storage_driver, mv, case When of
        before_move ->
            fun(Handle, FileId) ->
                case FileId =:= TargetFileId andalso get(?SUSPENDED_ONCE) =:= undefined of
                    true ->
                        put(?SUSPENDED_ONCE, true),
                        ?SUSPEND_UNTIL_RESUMED(Master);
                    false ->
                        ok
                end,
                meck:passthrough([Handle, FileId])
            end;
        after_move ->
            fun(Handle, FileId) ->
                Result = meck:passthrough([Handle, FileId]),
                case {Result, FileId} of
                    {ok, TargetFileId} -> ?SUSPEND_UNTIL_RESUMED(Master);
                    _ -> ok
                end,
                Result
            end
    end),

    UnlinkResult = file_lifecycle_test_utils:run_asynchronously(fun() ->
        lfm_proxy:unlink(Node, SessId, ?FILE_REF(FileGuid))
    end),

    SuspendedProc = file_lifecycle_test_utils:await_suspension(),
    ?assertEqual(ok, lfm_proxy:close_all(Node)),
    file_lifecycle_test_utils:resume(SuspendedProc),

    ?assertEqual(ok, UnlinkResult()),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, SessId, ?FILE_REF(FileGuid))),

    % the hidden directory was created, so the move was indeed attempted, but the
    % file released in the meantime must not have been left in it
    ?assert(file_lifecycle_test_utils:has_deleted_open_files_dir(Node, SpaceId)),
    ?assertEqual([], file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)),
    ?assertEqual([], file_lifecycle_test_utils:list_deleted_open_files_on_storage(Node, SpaceId)),
    ok.
