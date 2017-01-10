%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Requests truncating file.
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
-spec truncate(user_context:ctx(), file_context:ctx(), Size :: non_neg_integer()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
truncate(Ctx, File, Size) ->
    File2 = update_quota(File, Size),
    SessId = user_context:get_session_id(Ctx),
    SpaceDirUuid = file_context:get_space_dir_uuid(File2),
    {uuid, FileUuid} = file_context:get_uuid_entry(File2),
    lists:foreach(
        fun({SID, FID}) ->
            {ok, Storage} = storage:get(SID),
            SFMHandle = storage_file_manager:new_handle(SessId, SpaceDirUuid, FileUuid, Storage, FID),
            {ok, Handle} = storage_file_manager:open(SFMHandle, write),
            ok = storage_file_manager:truncate(Handle, Size)
        end, fslogic_utils:get_local_storage_file_locations({uuid, FileUuid})), %todo consider caching in file_context

    {FileDoc, _File4} = file_context:get_file_doc(File2),
    fslogic_times:update_mtime_ctime(FileDoc, user_context:get_user_id(Ctx)), %todo pass file_context
    #fuse_response{status = #status{code = ?OK}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Update space quota.
%% @end
%%--------------------------------------------------------------------
-spec update_quota(file_context:ctx(), file_meta:size()) -> NewFile :: file_context:ctx().
update_quota(File, Size) ->
    {FileDoc, File2} = file_context:get_file_doc(File),
    SpaceId = file_context:get_space_id(File),
    OldSize = fslogic_blocks:get_file_size(FileDoc), %todo pass file_context
    ok = space_quota:assert_write(SpaceId, Size - OldSize),
    File2.
