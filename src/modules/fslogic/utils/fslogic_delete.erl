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
-include("modules/events/definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([remove_file_and_file_meta/3, remove_file_and_file_meta/5,
    remove_file_handles/1]).

%%--------------------------------------------------------------------
%% @doc
%% @equiv remove_file_and_file_meta(FileCtx, UserCtx, Silent, true).
%% @end
%%--------------------------------------------------------------------
-spec remove_file_and_file_meta(file_ctx:ctx(), user_ctx:ctx(), boolean()) -> ok.
remove_file_and_file_meta(FileCtx, UserCtx, Silent) ->
    remove_file_and_file_meta(FileCtx, UserCtx, Silent, true, false).

%%--------------------------------------------------------------------
%% @doc
%% Removes file and file meta.
%% If parameter Silent is true, file_removed_event will not be emitted.
%% If parameter RemoveStorageFile is false, file will not be deleted
%% on storage.
%% If parameter DeleteParentLink is true, link in parent is deleted.
%% @end
%%--------------------------------------------------------------------
-spec remove_file_and_file_meta(file_ctx:ctx(), user_ctx:ctx(), boolean(),
    boolean(), boolean()) -> ok.
remove_file_and_file_meta(FileCtx, UserCtx, Silent, RemoveStorageFile,
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
