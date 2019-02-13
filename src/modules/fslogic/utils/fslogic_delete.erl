%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Util functions for fslogic_deletion_worker
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_delete).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([check_if_opened_and_remove/3, remove_opened_file/1,
    remove_storage_file_deleted_remotely/1, add_deletion_link_and_remove_normal_link/2,
    delete_all_opened_files/0]).
% TODO - sprawdzic co z ponizszego jest private
-export([remove_file/5,
    remove_file_handles/1, remove_auxiliary_documents/1]).

%%--------------------------------------------------------------------
%% @doc
%% Request deletion of given file
%% @end
%%--------------------------------------------------------------------
-spec check_if_opened_and_remove(user_ctx:ctx(), file_ctx:ctx(), Silent :: boolean()) ->
    ok.
check_if_opened_and_remove(UserCtx, FileCtx, Silent) ->
    try
        FileUuid = file_ctx:get_uuid_const(FileCtx),
        ok = case file_handles:exists(FileUuid) of
            true ->
                add_deletion_link_and_remove_normal_link(FileCtx, UserCtx),
                ok = file_handles:mark_to_remove(FileCtx),
                % Czemu nie bierzemy pod uwage silent?
                fslogic_event_emitter:emit_file_removed(FileCtx, [user_ctx:get_session_id(UserCtx)]);
            false ->
                remove_file(FileCtx, UserCtx, Silent, true, true)
        end
    catch
        _:{badmatch, {error, not_found}} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Request deletion of given open file
%% @end
%%--------------------------------------------------------------------
-spec remove_opened_file(file_ctx:ctx()) -> ok.
remove_opened_file(FileCtx) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    ok = case file_ctx:get_local_file_location_doc(FileCtx) of
        {#document{value = #file_location{storage_file_created = true}}, FileCtx} ->
            delete_file(FileCtx, UserCtx, fun delete_deletion_link_and_file/2);
        _ ->
            {FileDoc, _} = file_ctx:get_file_doc(FileCtx),
            file_meta:delete_without_link(FileDoc)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Request deletion of given file (delete triggered by dbsync).
%% @end
%%--------------------------------------------------------------------
-spec remove_storage_file_deleted_remotely(file_ctx:ctx()) -> ok.
remove_storage_file_deleted_remotely(FileCtx) ->
    try
        FileUuid = file_ctx:get_uuid_const(FileCtx),
        fslogic_event_emitter:emit_file_removed(FileCtx, []),
        UserCtx = user_ctx:new(?ROOT_SESS_ID),
        ok = case file_handles:exists(FileUuid) of
            true ->
                add_deletion_link(FileCtx, UserCtx),
                ok = file_handles:mark_to_remove(FileCtx);
            false ->
                % TODO - ja zabezpieczyc synca przed reimportem (nie ma linka i file_meta)?
                delete_file(FileCtx, UserCtx, fun check_and_maybe_delete_storage_file/2)
        end
    catch
        _:{badmatch, {error, not_found}} ->
            ok
    end.

delete_all_opened_files() ->
    case file_handles:list() of
        {ok, Docs} ->
            RemovedFiles = lists:filter(fun(#document{value = Handle}) ->
                Handle#file_handles.is_removed
                                        end, Docs),

            lists:foreach(fun(#document{key = FileUuid} = Doc) ->
                try
                    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid),
                    FileCtx = file_ctx:new_by_guid(FileGuid),
                    UserCtx = user_ctx:new(?ROOT_SESS_ID),
                    ok = remove_file(FileCtx, UserCtx, false, true, true)
                catch
                    E1:E2 ->
                        ?warning_stacktrace("Cannot remove old opened file ~p: ~p:~p",
                            [Doc, E1, E2])
                end
                          end, RemovedFiles),

            lists:foreach(fun(#document{key = FileUuid}) ->
                ok = file_handles:delete(FileUuid)
                          end, Docs);
        Error ->
            ?error_stacktrace("Cannot clean open files descriptors - ~p", [Error])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes file and file meta.
%% If parameter Silent is true, file_removed_event will not be emitted.
%% If parameter RemoveStorageFile is false, file will not be deleted
%% on storage.
%% If parameter DeleteParentLink is true, link in parent is deleted.
%% @end
%%--------------------------------------------------------------------
-spec remove_file(file_ctx:ctx(), user_ctx:ctx(), boolean(),
    boolean(), boolean()) -> ok.
remove_file(FileCtx, UserCtx, Silent, RemoveStorageFile,
    DeleteParentLink) ->
    {FileDoc = #document{value = #file_meta{
            shares = Shares
        }
    }, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ParentCtx, FileCtx3} = file_ctx:get_parent(FileCtx2, UserCtx),
    ok = delete_shares(UserCtx, Shares),

    fslogic_times:update_mtime_ctime(ParentCtx),

    case RemoveStorageFile of
        true ->
            maybe_remove_file_on_storage(FileCtx3, UserCtx);
        _ -> ok
    end,

    ok = case DeleteParentLink of
        true ->
            file_meta:delete(FileDoc);
        _ ->
            file_meta:delete_without_link(FileDoc)
    end,
    maybe_emit_event(FileCtx3, UserCtx, Silent).

%%--------------------------------------------------------------------
%% @doc
%% Removes file handles
%% @end
%%--------------------------------------------------------------------
-spec remove_file_handles(file_ctx:ctx()) -> ok.
remove_file_handles(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    ok = file_handles:delete(FileUuid).

remove_auxiliary_documents(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    ok = file_popularity:delete(FileUuid),
    ok = custom_metadata:delete(FileUuid),
    ok = times:delete(FileUuid),
    ok = transferred_file:clean_up(FileGuid).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% If file to remove and existing file are not the same file then does nothing.
%% Otherwise removes storage file.
%% @end
%%-------------------------------------------------------------------
-spec check_and_maybe_delete_storage_file(file_ctx:ctx(), user_ctx:ctx()) ->
    ok | {error, term()}.
check_and_maybe_delete_storage_file(FileCtx, UserCtx) ->
    {#document{key = Uuid, value = #file_meta{name = Name}},
        FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),
    try
        {ParentCtx, FileCtx3} = file_ctx:get_parent(FileCtx2, UserCtx),
        {ParentDoc, _} = file_ctx:get_file_doc_including_deleted(ParentCtx),
        % TODO - zamiast tego chyba powinnismy pobrac file_location i sprawdzic,
        % bo plik moglbyc na storage'u pod inna nazwa
        % czemu tego nie sprawdzamy ja nie jest kasowanie z dbsync tylko otwartego pliku?
        % czemy sfm_utils moze skasowac nie swoj plik?
        case fslogic_path:resolve(ParentDoc, <<"/", Name/binary>>) of
            {ok, #document{key = Uuid2}} when Uuid2 =/= Uuid ->
                ok;
            _ ->
                sfm_utils:recursive_delete(FileCtx3, UserCtx)
        end
    catch
        E1:E2 ->
            % Debug - parent could be deleted before
            ?debug_stacktrace("Cannot check parent during delete ~p: ~p:~p",
                [FileCtx, E1, E2]),
            sfm_utils:delete_storage_file_without_location(FileCtx, UserCtx)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Tries to delete storage file using given function, logs expected errors
%% and removes file document from metadata
%% @end
%%-------------------------------------------------------------------
-spec delete_file(file_ctx:ctx(), user_ctx:ctx(),
    fun((file_ctx:ctx(), user_ctx:ctx()) -> ok)) -> ok | {error, term()}.
delete_file(FileCtx, UserCtx, DeleteStorageFile) ->
    try
        ok = DeleteStorageFile(FileCtx, UserCtx)
    catch
        _:{badmatch, {error, not_found}} ->
            ?error_stacktrace("Cannot delete file at storage ~p", [FileCtx]),
            ok;
        _:{badmatch, {error, enoent}} ->
            ?debug_stacktrace("Cannot delete file at storage ~p", [FileCtx]),
            ok;
        _:{badmatch, {error, erofs}} ->
            ?warning_stacktrace("Cannot delete file at storage ~p", [FileCtx]),
            ok
    end,
    {FileDoc, _FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),
    file_meta:delete_without_link(FileDoc).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function is used to delete the file which deletion was delayed
%% due to existing handles. It removes the file and its deletion_link,
%% @end
%%-------------------------------------------------------------------
-spec delete_deletion_link_and_file(file_ctx:ctx(), user_ctx:ctx()) -> ok | {error, term()}.
delete_deletion_link_and_file(FileCtx, UserCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {ParentGuid, FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserCtx),
    ParentUuid = fslogic_uuid:guid_to_uuid(ParentGuid),
    {DeletionLinkName, FileCtx3} = file_deletion_link_name(FileCtx2),
    Scope = file_ctx:get_space_id_const(FileCtx3),
    ok =  sfm_utils:recursive_delete(FileCtx3, UserCtx),
    ok = file_meta:delete_child_link(ParentUuid, Scope, FileUuid, DeletionLinkName).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function adds a deletion_link for the file that is to be deleted.
%% It also deletes normal link from parent to the file.
%% @end
%%-------------------------------------------------------------------
-spec add_deletion_link_and_remove_normal_link(file_ctx:ctx(), user_ctx:ctx()) -> ok.
add_deletion_link_and_remove_normal_link(FileCtx, UserCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {ParentGuid, FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserCtx),
    {FileName, FileCtx3} = file_ctx:get_aliased_name(FileCtx2, UserCtx),
    ParentUuid = fslogic_uuid:guid_to_uuid(ParentGuid),
    {DeletionLinkName, FileCtx4} = file_deletion_link_name(FileCtx3),
    Scope = file_ctx:get_space_id_const(FileCtx4),
    ok = file_meta:add_child_link(ParentUuid, Scope, DeletionLinkName, FileUuid),
    ok = file_meta:delete_child_link(ParentUuid, Scope, FileUuid, FileName).

add_deletion_link(FileCtx, UserCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {ParentGuid, FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserCtx),
    ParentUuid = fslogic_uuid:guid_to_uuid(ParentGuid),
    {DeletionLinkName, FileCtx3} = file_deletion_link_name(FileCtx2),
    Scope = file_ctx:get_space_id_const(FileCtx3),
    ok = file_meta:add_child_link(ParentUuid, Scope, DeletionLinkName, FileUuid).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Utility function that returns deletion_link name for given file.
%% @end
%%-------------------------------------------------------------------
-spec file_deletion_link_name(file_ctx:ctx()) -> {file_meta:name(), file_ctx:ctx()}.
file_deletion_link_name(FileCtx) ->
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    BaseName = filename:basename(StorageFileId),
    {?FILE_DELETION_LINK_NAME(BaseName), FileCtx2}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes given file on storage if it exists.
%% Returns ok if file doesn't exist or if it was successfully deleted.
%% @end
%%--------------------------------------------------------------------
-spec maybe_remove_file_on_storage(file_ctx:ctx(), user_ctx:ctx())
        -> ok | {error, term()}.
maybe_remove_file_on_storage(FileCtx, UserCtx) ->
    case remove_file_on_storage(FileCtx, UserCtx) of
        ok -> ok;
        {error, ?ENOENT} -> ok;
        OtherError -> OtherError
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes given file on storage
%% @end
%%--------------------------------------------------------------------
-spec remove_file_on_storage(file_ctx:ctx(), user_ctx:ctx()) ->
    ok | {error, term()}.
remove_file_on_storage(FileCtx, UserCtx) ->
    sfm_utils:recursive_delete(FileCtx, UserCtx).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes given shares from oz and db.
%% @end
%%--------------------------------------------------------------------
-spec delete_shares(user_ctx:ctx(), [od_share:id()]) -> ok | no_return().
delete_shares(_UserCtx, []) ->
    ok;
delete_shares(UserCtx, Shares) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    [ok = share_logic:delete(SessionId, ShareId) || ShareId <- Shares],
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Emit file_removed event. If parameter Silent is true, event will not
%% be emitted.
%% @end
%%--------------------------------------------------------------------
maybe_emit_event(FileCtx, UserCtx, false) ->
    SessId = user_ctx:get_session_id(UserCtx),
    fslogic_event_emitter:emit_file_removed(FileCtx, [SessId]),
    ok;
maybe_emit_event(_FileCtx, _UserCtx, _) ->
    ok.
