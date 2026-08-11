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
%%% The bodies are dispatched to from file_lifecycle_races_test_SUITE, which gives every
%%% case a space of its own backed by a freshly created storage - hence the
%%% storage contents can be asserted in absolute terms.
%%% @end
%%%-------------------------------------------------------------------
-module(file_deletion_tests).
-author("Michal Wrzeszcz").

% This module indirectly includes eunit.hrl, whose parse transform would
% otherwise auto-export every arity 0 function named *_test - clashing with
% the export list below.
-define(EUNIT_NOAUTO, 1).

-include("file/file_lifecycle_test.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_delete.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("clproto/include/messages.hrl").
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
    content_of_deleted_opened_file_survives_name_takeover_test/1,
    content_of_deleted_opened_file_survives_name_takeover_on_object_storage_test/1,
    delete_of_newer_generation_first_leaves_older_on_storage_test/1,
    delete_of_older_generation_first_leaves_newer_on_storage_test/1,

    delete_via_fuse_removes_object_from_storage_test/1,

    node_restart_deletes_open_files_marked_for_removal_test/1,
    node_restart_deletes_open_files_with_no_storage_file_test/1,
    release_of_deleted_file_removes_it_from_storage_test/1,
    release_of_deleted_file_with_no_storage_file_test/1,
    delete_of_not_opened_file_removes_it_from_storage_test/1,
    delete_of_not_opened_file_with_no_storage_file_test/1,

    rename_to_opened_file_test/1
]).

% Whether the file a test case works on already has a storage file behind it.
% A freshly created file does not - the provider defers creating it until the
% file is first opened - and deleting it must cope with either.
-type storage_file_policy() :: with_storage_file | without_storage_file.

% marks the process dictionary of a mocked call that has already been suspended,
% so that a retry of the very same operation is let through
-define(SUSPENDED_ONCE, suspended_once).

-define(ATTEMPTS, 30).

-define(FILE_CONTENT, <<"file_content">>).

% Id correlating a request sent over the fuse protocol with its response
-define(FUSE_MSG_ID, <<"1">>).


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


content_of_deleted_opened_file_survives_name_takeover_test(Config) ->
    content_survives_name_takeover_test_base(Config).


%% NOTE: runs on an object storage, which keeps the storage file of the deleted
%% file right where it was, under a deletion marker, rather than renaming it away
%% - the handle held to it must keep working all the same.
content_of_deleted_opened_file_survives_name_takeover_on_object_storage_test(Config) ->
    content_survives_name_takeover_test_base(Config).


delete_of_newer_generation_first_leaves_older_on_storage_test(Config) ->
    deletion_order_of_two_generations_test_base(Config, newer_first).


delete_of_older_generation_first_leaves_newer_on_storage_test(Config) ->
    deletion_order_of_two_generations_test_base(Config, older_first).


%%%====================================================================
%%% Test functions concerning deletion requested over the fuse protocol
%%%====================================================================


%% NOTE: runs on an object storage - what a POSIX one is left with after a
%% deletion is covered by the cases above, whichever way the deletion was asked for.
delete_via_fuse_removes_object_from_storage_test(Config) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    % NOTE: the file is given content, as an object storage holds no object for
    % an empty file - without it the ENOENT expected in the end would be
    % indistinguishable from there never having been an object
    {FileGuid, _FilePath} = file_lifecycle_test_utils:create_file_with_content(
        Node, SessId, SpaceId, ?FILE_CONTENT
    ),
    {StorageId, StorageFileId} = file_lifecycle_test_utils:locate_on_storage(
        Node, SpaceId, FileGuid
    ),
    ?assertMatch({ok, _},
        storage_file_tree_test_utils:stat(?PROVIDER_SELECTOR, StorageId, StorageFileId)),

    {ok, {Sock, _}} = ?assertMatch({ok, _}, fuse_test_utils:connect_via_token(
        Node, [{active, true}], ?RAND_STR(),
        oct_background:get_user_access_token(?USER_SELECTOR)
    )),
    ok = ssl:send(Sock, fuse_test_utils:generate_delete_file_message(FileGuid, ?FUSE_MSG_ID)),
    ?assertMatch(#'ServerMessage'{
        message_id = ?FUSE_MSG_ID,
        message_body = {fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}}
    }, fuse_test_utils:receive_server_message()),
    ok = ssl:close(Sock),

    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, SessId, ?FILE_REF(FileGuid))),
    ?assertEqual({error, ?ENOENT},
        storage_file_tree_test_utils:stat(?PROVIDER_SELECTOR, StorageId, StorageFileId), ?ATTEMPTS).


%%%====================================================================
%%% Test functions driving the steps of the deletion procedure directly
%%%====================================================================


node_restart_deletes_open_files_marked_for_removal_test(Config) ->
    node_restart_test_base(Config, with_storage_file).


node_restart_deletes_open_files_with_no_storage_file_test(Config) ->
    node_restart_test_base(Config, without_storage_file).


release_of_deleted_file_removes_it_from_storage_test(Config) ->
    release_of_deleted_file_test_base(Config, with_storage_file).


release_of_deleted_file_with_no_storage_file_test(Config) ->
    release_of_deleted_file_test_base(Config, without_storage_file).


delete_of_not_opened_file_removes_it_from_storage_test(Config) ->
    delete_of_not_opened_file_test_base(Config, with_storage_file).


delete_of_not_opened_file_with_no_storage_file_test(Config) ->
    delete_of_not_opened_file_test_base(Config, without_storage_file).


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

    % NOTE: the file is given content before anything else, so that it has a
    % storage file at all no matter the storage type
    {FileGuid, FilePath} = file_lifecycle_test_utils:create_file_with_content(
        Node, SessId, SpaceId, ?FILE_CONTENT
    ),
    {StorageId, StorageFileId} = file_lifecycle_test_utils:locate_on_storage(
        Node, SpaceId, FileGuid
    ),
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


%% @private
%% @doc
%% Deletes a file that is open and puts a new one in its place, under the very
%% same name. The two are different files that merely shared a name, and the
%% handles held to them stay that way - each reads and writes its own content,
%% with neither disturbed by what became of the other.
%% @end
-spec content_survives_name_takeover_test_base(test_config:config()) -> ok | no_return().
content_survives_name_takeover_test_base(Config) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    Size = 100,
    OldContent = crypto:strong_rand_bytes(Size),
    NewContent = crypto:strong_rand_bytes(Size),

    FilePath = file_lifecycle_test_utils:build_file_path(Node, SessId, SpaceId),
    {ok, {OldFileGuid, OldHandle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        Node, SessId, FilePath
    )),
    ?assertEqual({ok, Size}, lfm_proxy:write(Node, OldHandle, 0, OldContent)),

    ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, {path, FilePath})),

    {ok, {NewFileGuid, NewHandle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        Node, SessId, FilePath
    )),
    ?assertEqual({ok, Size}, lfm_proxy:write(Node, NewHandle, 0, NewContent)),
    ?assertEqual({ok, NewContent}, lfm_proxy:read(Node, NewHandle, 0, Size)),

    % the deleted file is gone from the file tree for good, so it can be neither
    % looked up nor opened anew...
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, SessId, ?FILE_REF(OldFileGuid))),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:open(Node, SessId, ?FILE_REF(OldFileGuid), read)),

    % ...yet the handle opened before it was deleted keeps reading and writing it
    ?assertEqual({ok, OldContent}, lfm_proxy:read(Node, OldHandle, 0, Size)),
    ?assertEqual({ok, Size}, lfm_proxy:write(Node, OldHandle, Size, OldContent)),
    ?assertEqual({ok, <<OldContent/binary, OldContent/binary>>},
        lfm_proxy:read(Node, OldHandle, 0, 2 * Size)),

    % and none of that reached the file that took over the name, nor does
    % releasing the handles change what it holds
    ?assertEqual({ok, NewContent}, lfm_proxy:read(Node, NewHandle, 0, Size)),
    ?assertEqual(ok, lfm_proxy:close_all(Node)),

    {ok, ReopenedHandle} = ?assertMatch({ok, _}, lfm_proxy:open(
        Node, SessId, ?FILE_REF(NewFileGuid), read
    )),
    ?assertEqual({ok, NewContent}, lfm_proxy:read(Node, ReopenedHandle, 0, Size)),
    ok.


%% @private
%% @doc
%% Disposes of two files that shared a name - one deleted while open and hence
%% living on in the hidden directory for deleted open files, the other holding
%% the name now - in either order. Whichever goes first, only its own storage
%% file may go away with it.
%% @end
-spec deletion_order_of_two_generations_test_base(
    test_config:config(), newer_first | older_first
) ->
    ok | no_return().
deletion_order_of_two_generations_test_base(Config, Order) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    FilePath = file_lifecycle_test_utils:build_file_path(Node, SessId, SpaceId),
    FileName = filename:basename(FilePath),

    {ok, {OldFileGuid, OldHandle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        Node, SessId, FilePath
    )),
    ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, ?FILE_REF(OldFileGuid))),
    ?assertEqual([], file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)),
    ?assertEqual([OldFileGuid],
        file_lifecycle_test_utils:list_deleted_open_files_on_storage(Node, SpaceId)),

    {ok, {NewFileGuid, NewHandle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        Node, SessId, FilePath
    )),
    ?assertEqual([FileName], file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)),

    case Order of
        newer_first ->
            % the newer file is deleted while open too, so it joins the older one
            % in the hidden directory and leaves it once its handle is released
            ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, ?FILE_REF(NewFileGuid))),
            ?assertEqual(ok, lfm_proxy:close(Node, NewHandle)),
            ?assertEqual([OldFileGuid],
                file_lifecycle_test_utils:list_deleted_open_files_on_storage(Node, SpaceId), ?ATTEMPTS),
            ?assertEqual([], file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)),
            ?assertEqual(ok, lfm_proxy:close(Node, OldHandle));

        older_first ->
            ?assertEqual(ok, lfm_proxy:close(Node, OldHandle)),
            ?assertEqual([],
                file_lifecycle_test_utils:list_deleted_open_files_on_storage(Node, SpaceId), ?ATTEMPTS),
            ?assertEqual([FileName],
                file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)),
            ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, ?FILE_REF(NewFileGuid))),
            ?assertEqual(ok, lfm_proxy:close(Node, NewHandle))
    end,

    ?assertEqual([], file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId), ?ATTEMPTS),
    ?assertEqual([],
        file_lifecycle_test_utils:list_deleted_open_files_on_storage(Node, SpaceId), ?ATTEMPTS),
    ok.


%% @private
%% @doc
%% Whatever descriptors a provider had open went away with it when it went down,
%% so on start it clears them all - and a file that was deleted while open, and
%% was therefore only waiting for its last descriptor to go, is deleted for good
%% right here (see node_manager_plugin:on_init/1 -> fslogic_delete:cleanup_opened_files/0).
%% @end
-spec node_restart_test_base(test_config:config(), storage_file_policy()) -> ok | no_return().
node_restart_test_base(Config, StorageFilePolicy) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    {DeletedFileGuid, DeletedFilePath} = create_file(Node, SessId, SpaceId, StorageFilePolicy),
    {KeptFileGuid1, KeptFilePath1} = create_file(Node, SessId, SpaceId, StorageFilePolicy),
    {KeptFileGuid2, KeptFilePath2} = create_file(Node, SessId, SpaceId, StorageFilePolicy),

    AllFileGuids = [DeletedFileGuid, KeptFileGuid1, KeptFileGuid2],
    KeptFileNames = lists:sort([filename:basename(KeptFilePath1), filename:basename(KeptFilePath2)]),
    AllFileNames = lists:sort([filename:basename(DeletedFilePath) | KeptFileNames]),

    ?assertEqual(expected_storage_files(AllFileNames, StorageFilePolicy),
        file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)),

    % all three files are open when the provider goes down, and one of them was
    % deleted in the meantime, so it is only waiting for its descriptors to go
    lists:foreach(fun(FileGuid) ->
        ?assertEqual(ok, register_open(Node, SessId, FileGuid))
    end, AllFileGuids),
    ?assertEqual([true, true, true], are_files_opened(Node, AllFileGuids)),
    ?assertEqual(ok, mark_to_remove(Node, DeletedFileGuid)),

    % NOTE: the hook clears the descriptors of every file the node knows of, not
    % only of the ones opened here - which also rules the suite out of running
    % its cases in parallel
    ?assertEqual(ok, rpc:call(Node, fslogic_delete, cleanup_opened_files, [])),
    ?assertEqual([false, false, false], are_files_opened(Node, AllFileGuids)),

    % the file that was waiting to be deleted is gone, the other two are intact
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, SessId, ?FILE_REF(DeletedFileGuid))),
    ?assertNot(has_file_meta(Node, DeletedFileGuid)),
    ?assertMatch({ok, _}, lfm_proxy:stat(Node, SessId, ?FILE_REF(KeptFileGuid1))),
    ?assertMatch({ok, _}, lfm_proxy:stat(Node, SessId, ?FILE_REF(KeptFileGuid2))),
    ?assertEqual(expected_storage_files(KeptFileNames, StorageFilePolicy),
        file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId), ?ATTEMPTS),
    ok.


%% @private
%% @doc
%% Runs the two steps of deleting an opened file one after the other: the first
%% takes the file out of the file tree, the second - which the provider runs once
%% the last descriptor is released - disposes of everything the first left behind.
%% @end
-spec release_of_deleted_file_test_base(test_config:config(), storage_file_policy()) ->
    ok | no_return().
release_of_deleted_file_test_base(Config, StorageFilePolicy) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    {FileGuid, FilePath} = create_file(Node, SessId, SpaceId, StorageFilePolicy),
    ?assertEqual(expected_storage_files([filename:basename(FilePath)], StorageFilePolicy),
        file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)),

    % the first step takes the file out of the file tree - the name it occupied is
    % free from now on, while the file itself is still all there
    UserCtx = rpc:call(Node, user_ctx, new, [SessId]),
    DetachedFileCtx = rpc:call(Node, fslogic_delete, delete_parent_link, [
        file_ctx:new_by_guid(FileGuid), UserCtx
    ]),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, SessId, {path, FilePath})),
    ?assertMatch({ok, _}, lfm_proxy:stat(Node, SessId, ?FILE_REF(FileGuid))),
    ?assert(has_file_meta(Node, FileGuid)),

    % and the second disposes of everything the first left behind
    ?assertEqual(ok, rpc:call(Node, fslogic_delete, handle_release_of_deleted_file, [
        DetachedFileCtx, ?LOCAL_REMOVE
    ])),

    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, SessId, ?FILE_REF(FileGuid))),
    ?assertNot(has_file_meta(Node, FileGuid)),
    ?assertEqual([], file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)),
    ok.


%% @private
%% @doc
%% Deletes a file that no one holds a descriptor to, which the provider does in
%% one step - this is the path a deletion performed by another provider takes
%% once it reaches this one, hence the file marked as deleted in its own document
%% beforehand.
%% @end
-spec delete_of_not_opened_file_test_base(test_config:config(), storage_file_policy()) ->
    ok | no_return().
delete_of_not_opened_file_test_base(Config, StorageFilePolicy) ->
    Node = file_lifecycle_test_utils:get_node(),
    SessId = file_lifecycle_test_utils:get_session_id(),
    SpaceId = ?SPACE_ID(Config),

    {FileGuid, FilePath} = create_file(Node, SessId, SpaceId, StorageFilePolicy),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    ?assertEqual(expected_storage_files([filename:basename(FilePath)], StorageFilePolicy),
        file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)),

    % marking the file as deleted does not delete its document - that is what the
    % procedure below is for
    ?assertMatch({ok, _}, rpc:call(Node, file_meta, update, [FileUuid, fun(FileMeta) ->
        {ok, FileMeta#file_meta{deleted = true}}
    end])),
    ?assert(has_file_meta(Node, FileGuid)),
    ?assertEqual(false, rpc:call(Node, file_handles, is_file_opened, [FileUuid])),

    UserCtx = rpc:call(Node, user_ctx, new, [SessId]),
    ?assertEqual(ok, rpc:call(Node, fslogic_delete, delete_file_locally, [
        UserCtx, file_ctx:new_by_guid(FileGuid),
        oct_background:get_provider_id(?PROVIDER_SELECTOR), false, update_dir_stats
    ])),

    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, SessId, ?FILE_REF(FileGuid))),
    ?assertNot(has_file_meta(Node, FileGuid)),
    ?assertEqual([], file_lifecycle_test_utils:list_space_files_on_storage(Node, SpaceId)),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_file(node(), session:id(), od_space:id(), storage_file_policy()) ->
    {file_id:file_guid(), file_meta:path()}.
create_file(Node, SessId, SpaceId, with_storage_file) ->
    file_lifecycle_test_utils:create_file_with_storage_file(Node, SessId, SpaceId);
create_file(Node, SessId, SpaceId, without_storage_file) ->
    file_lifecycle_test_utils:create_file(Node, SessId, SpaceId).


%% @private
-spec expected_storage_files([binary()], storage_file_policy()) -> [binary()].
expected_storage_files(FileNames, with_storage_file) -> lists:sort(FileNames);
expected_storage_files(_FileNames, without_storage_file) -> [].


%% @private
%% @doc
%% Registers a descriptor on a file without actually opening it, which is how the
%% cases above put the provider in the state it would be in with the file open.
%% @end
-spec register_open(node(), session:id(), file_id:file_guid()) -> ok | {error, term()}.
register_open(Node, SessId, FileGuid) ->
    rpc:call(Node, file_handles, register_open, [
        file_ctx:new_by_guid(FileGuid), SessId, 1, undefined
    ]).


%% @private
-spec mark_to_remove(node(), file_id:file_guid()) -> ok | {error, term()}.
mark_to_remove(Node, FileGuid) ->
    rpc:call(Node, file_handles, mark_to_remove, [
        file_ctx:new_by_guid(FileGuid), ?LOCAL_REMOVE
    ]).


%% @private
%% @doc
%% Whether the provider still has a document for the file. This is what tells a
%% file that was disposed of from one that was merely marked as deleted, and
%% unlike the storage contents it says so for a file that never had a storage
%% file behind it. NOTE: deleting a document of a synchronized model leaves a
%% tombstone in its place, so that the deletion reaches the other providers -
%% the document is therefore fetched including deleted ones and told apart by
%% the flag the datastore raises on it, exactly as file_meta:get/1 does it.
%% @end
-spec has_file_meta(node(), file_id:file_guid()) -> boolean().
has_file_meta(Node, FileGuid) ->
    case rpc:call(Node, file_meta, get_including_deleted, [file_id:guid_to_uuid(FileGuid)]) of
        {ok, #document{deleted = IsDeleted}} -> not IsDeleted;
        {error, not_found} -> false
    end.


%% @private
-spec are_files_opened(node(), [file_id:file_guid()]) -> [boolean()].
are_files_opened(Node, FileGuids) ->
    lists:map(fun(FileGuid) ->
        rpc:call(Node, file_handles, is_file_opened, [file_id:guid_to_uuid(FileGuid)])
    end, FileGuids).
