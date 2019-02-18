%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Util functions for file deletion.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_delete).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_sufix.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([check_if_opened_and_remove/4, remove_opened_file/1, remove_file/4,
    remove_file_handles/1, remove_auxiliary_documents/1, delete_all_opened_files/0]).

%% Test API
-export([process_file_links/3]).

-type delete_metadata_opts() :: boolean() | deletion_link.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks if file is opened and deletes it or marks to be deleted.
%% @end
%%--------------------------------------------------------------------
-spec check_if_opened_and_remove(user_ctx:ctx(), file_ctx:ctx(),
    Silent :: boolean(), RemoteDelete :: boolean()) -> ok.
check_if_opened_and_remove(UserCtx, FileCtx, Silent, RemoteDelete) ->
    try
        FileUuid = file_ctx:get_uuid_const(FileCtx),
        ok = case file_handles:exists(FileUuid) of
                 true ->
                     process_file_links(FileCtx, UserCtx, RemoteDelete),
                     ok = file_handles:mark_to_remove(FileCtx);
                 _ ->
                     % TODO - ja zabezpieczyc synca przed reimportem (nie ma linka i file_meta)?
                     remove_file(FileCtx, UserCtx, true, not RemoteDelete)
             end,
        maybe_emit_event(FileCtx, UserCtx, Silent)
    catch
        _:{badmatch, {error, not_found}} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes opened file.
%% @end
%%--------------------------------------------------------------------
-spec remove_opened_file(file_ctx:ctx()) -> ok.
remove_opened_file(FileCtx) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    ok = remove_file(FileCtx, UserCtx, true, deletion_link).

%%--------------------------------------------------------------------
%% @doc
%% Deletes all opened files.
%% @end
%%--------------------------------------------------------------------
-spec delete_all_opened_files() -> ok.
delete_all_opened_files() ->
    case file_handles:list() of
        {ok, Docs} ->
            RemovedFiles = lists:filter(fun(#document{value = Handle}) ->
                Handle#file_handles.is_removed
                                        end, Docs),

            UserCtx = user_ctx:new(?ROOT_SESS_ID),
            lists:foreach(fun(#document{key = FileUuid} = Doc) ->
                try
                    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid),
                    FileCtx = file_ctx:new_by_guid(FileGuid),
                    ok = remove_file(FileCtx, UserCtx, true, deletion_link)
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
%% Removes file handles
%% @end
%%--------------------------------------------------------------------
-spec remove_file_handles(file_ctx:ctx()) -> ok.
remove_file_handles(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    ok = file_handles:delete(FileUuid).

%%--------------------------------------------------------------------
%% @doc
%% Removes auxiliary documents connected with file.
%% @end
%%--------------------------------------------------------------------
-spec remove_auxiliary_documents(file_ctx:ctx()) -> ok.
remove_auxiliary_documents(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    ok = file_popularity:delete(FileUuid),
    ok = custom_metadata:delete(FileUuid),
    ok = times:delete(FileUuid),
    ok = transferred_file:clean_up(FileGuid).

%%--------------------------------------------------------------------
%% @doc
%% Removes file and file meta.
%% If parameter RemoveStorageFile is false, file will not be deleted
%% on storage.
%% Parameter DeleteMetadata verifies which metadata is deleted with file.
%% @end
%%--------------------------------------------------------------------
-spec remove_file(file_ctx:ctx(), user_ctx:ctx(), boolean(),
    delete_metadata_opts()) -> ok.
remove_file(FileCtx, UserCtx, RemoveStorageFile, DeleteMetadata) ->
    {FileDoc, FileCtx4} = case DeleteMetadata of
        true ->
            {FD = #document{value = #file_meta{
                shares = Shares
            }
            }, FileCtx2} = file_ctx:get_file_doc(FileCtx),
            {ParentCtx, FileCtx3} = file_ctx:get_parent(FileCtx2, UserCtx),
            ok = delete_shares(UserCtx, Shares),

            fslogic_times:update_mtime_ctime(ParentCtx),
            {FD, FileCtx3};
        deletion_link ->
            file_ctx:get_file_doc_including_deleted(FileCtx);
        _ ->
            {undefined, FileCtx}
    end,

    ok = case RemoveStorageFile of
        true ->
            maybe_remove_file_on_storage(FileCtx4, UserCtx);
        _ -> ok
    end,

    case DeleteMetadata of
        true ->
            file_meta:delete(FileDoc);
        deletion_link ->
            file_meta:delete_without_link(FileDoc), % do not match, document may not exist
            {ParentGuid, FileCtx5} = file_ctx:get_parent_guid(FileCtx4, UserCtx),
            ParentUuid = fslogic_uuid:guid_to_uuid(ParentGuid),
            location_and_link_utils:remove_deletion_link(FileCtx5, ParentUuid),
            ok;
        false ->
            ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function adds a deletion_link for the file that is to be deleted.
%% It can also delete normal link from parent to the file.
%% @end
%%-------------------------------------------------------------------
-spec process_file_links(file_ctx:ctx(), user_ctx:ctx(), boolean()) -> ok.
process_file_links(FileCtx, UserCtx, KeepParentLink) ->
    {ParentGuid, FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserCtx),
    ParentUuid = fslogic_uuid:guid_to_uuid(ParentGuid),
    FileCtx3 = location_and_link_utils:add_deletion_link(FileCtx2, ParentUuid),
    ok = case KeepParentLink of
             false ->
                 FileUuid = file_ctx:get_uuid_const(FileCtx3),
                 Scope = file_ctx:get_space_id_const(FileCtx3),
                 {FileName, _FileCtx4} = file_ctx:get_aliased_name(FileCtx3, UserCtx),
                 file_meta:delete_child_link(ParentUuid, Scope, FileUuid, FileName);
             _ ->
                 ok
         end.

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
    try
        case sfm_utils:recursive_delete(FileCtx, UserCtx) of
            ok -> ok;
            {error, ?ENOENT} -> ok;
            OtherError -> OtherError
        end
    catch
        _:{badmatch, {error, not_found}} ->
            ?error_stacktrace("Cannot delete file at storage ~p", [FileCtx]),
            ok;
        _:{badmatch, {error, ?ENOENT}} ->
            ?debug_stacktrace("Cannot delete file at storage ~p", [FileCtx]),
            ok;
        _:{badmatch, {error, erofs}} ->
            ?warning_stacktrace("Cannot delete file at storage ~p", [FileCtx]),
            ok
    end.

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
