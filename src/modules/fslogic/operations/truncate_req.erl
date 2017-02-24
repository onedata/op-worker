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
-check_permissions([traverse_ancestors, ?write_object]).
truncate(UserCtx, FileCtx, Size) ->
    FileCtx2 = update_quota(FileCtx, Size),
    SessId = user_ctx:get_session_id(UserCtx),
    SpaceDirUuid = file_ctx:get_space_dir_uuid_const(FileCtx2),
    FileUuid = file_ctx:get_uuid_const(FileCtx2),
    {LocalLocations, FileCtx3} = file_ctx:get_local_file_location_docs(FileCtx2),
    lists:foreach(
        fun(#document{value = #file_location{
            storage_id = StorageId,
            file_id = FileId
        }}) ->
            {ok, Storage} = storage:get(StorageId),
            SFMHandle = storage_file_manager:new_handle(SessId, SpaceDirUuid,
                FileUuid, Storage, FileId),
            {ok, Handle} = storage_file_manager:open(SFMHandle, write),
            ok = storage_file_manager:truncate(Handle, Size)
        end, LocalLocations),

    fslogic_times:update_mtime_ctime(FileCtx3),
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
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    OldSize = fslogic_blocks:get_file_size(FileCtx),
    ok = space_quota:assert_write(SpaceId, Size - OldSize),
    FileCtx.
