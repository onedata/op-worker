%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests for truncating file.
%%% @end
%%%--------------------------------------------------------------------
-module(truncate_req).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([truncate/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc 
%% Truncates file on storage and returns only if operation is complete. Does not change file size in
%% #file_meta model. Model's size should be changed by write events.
%% @end
%%--------------------------------------------------------------------
-spec truncate(user_ctx:ctx(), file_ctx:ctx(), Size :: non_neg_integer()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
truncate(UserCtx, FileCtx, Size) ->
    FileCtx2 = update_quota(FileCtx, Size),
    SessId = user_ctx:get_session_id(UserCtx),
    SpaceDirUuid = file_ctx:get_space_dir_uuid_const(FileCtx2),
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx2),
    lists:foreach(
        fun({SID, FID}) ->
            {ok, Storage} = storage:get(SID),
            SFMHandle = storage_file_manager:new_handle(SessId, SpaceDirUuid,
                FileUuid, Storage, FID),
            {ok, Handle} = storage_file_manager:open(SFMHandle, write),
            ok = storage_file_manager:truncate(Handle, Size)
        end, fslogic_utils:get_local_storage_file_locations({uuid, FileUuid})), %todo consider caching in file_ctx

    {FileDoc, _FileCtx3} = file_ctx:get_file_doc(FileCtx2),
    fslogic_times:update_mtime_ctime(FileDoc, user_ctx:get_user_id(UserCtx)), %todo pass file_ctx
    #fuse_response{status = #status{code = ?OK}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates space quota.
%% @end
%%--------------------------------------------------------------------
-spec update_quota(file_ctx:ctx(), file_meta:size()) -> NewFileCtx :: file_ctx:ctx().
update_quota(FileCtx, Size) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    OldSize = fslogic_blocks:get_file_size(FileDoc), %todo pass file_ctx
    ok = space_quota:assert_write(SpaceId, Size - OldSize),
    FileCtx2.
