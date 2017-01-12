%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on file attributes.
%%% @end
%%%--------------------------------------------------------------------
-module(attr_req).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([get_file_attr/2, get_file_attr_insecure/2, get_child_attr/3, chmod/3,
    update_times/5]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes with permission check.
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}]).
get_file_attr(UserCtx, FileCtx) ->
    get_file_attr_insecure(UserCtx, FileCtx).

%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes.
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_file_attr_insecure(UserCtx, FileCtx) ->
    {FileDoc = #document{key = Uuid, value = #file_meta{
        type = Type, mode = Mode, provider_id = ProviderId, uid = OwnerId,
        shares = Shares}}, FileCtx2
    } = file_ctx:get_file_doc(FileCtx),
    ShareId = file_ctx:get_share_id_const(FileCtx),
    UserId = user_ctx:get_user_id(UserCtx),
    {FileName, FileCtx3} = file_ctx:get_aliased_name(FileCtx2, UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx3),
    {{Uid, Gid}, FileCtx4} = file_ctx:get_posix_storage_user_context(FileCtx3, OwnerId),
    Size = fslogic_blocks:get_file_size(FileDoc), %todo TL consider caching file_location in File record
    {ParentGuid, FileCtx5} = file_ctx:get_parent_guid(FileCtx4, UserId),
    {{ATime, CTime, MTime}, _FileCtx6} = file_ctx:get_times(FileCtx5),

    #fuse_response{status = #status{code = ?OK}, fuse_response = #file_attr{
        uid = Uid, gid = Gid, parent_uuid = ParentGuid,
        uuid = fslogic_uuid:uuid_to_share_guid(Uuid, SpaceId, ShareId),
        type = Type, mode = Mode, atime = ATime, mtime = MTime,
        ctime = CTime, size = Size, name = FileName, provider_id = ProviderId,
        shares = Shares, owner_id = OwnerId
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Returns attributes of directory child (if exists).
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr(user_ctx:ctx(), ParentFile :: file_ctx:ctx(),
    Name :: file_meta:name()) -> fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}]).
get_child_attr(UserCtx, ParentFileCtx, Name) ->
    UserId = user_ctx:get_user_id(UserCtx),
    {ChildFileCtx, _NewParentFileCtx} = file_ctx:get_child(ParentFileCtx, Name, UserId),
    attr_req:get_file_attr(UserCtx, ChildFileCtx).

%%--------------------------------------------------------------------
%% @doc
%% Changes file permissions.
%% @end
%%--------------------------------------------------------------------
-spec chmod(user_ctx:ctx(), file_ctx:ctx(), Perms :: fslogic_worker:posix_permissions()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}, {owner, 2}]).
chmod(UserCtx, FileCtx, Mode) ->
    {uuid, Uuid} = file_ctx:get_uuid_entry_const(FileCtx),
    sfm_utils:chmod_storage_files(UserCtx, {uuid, Uuid}, Mode), %todo pass file_ctx

    % remove acl
    xattr:delete_by_name(Uuid, ?ACL_KEY),
    {ok, _} = file_meta:update({uuid, Uuid}, #{mode => Mode}),
    ok = permissions_cache:invalidate(file_meta, Uuid),

    fslogic_times:update_ctime({uuid, Uuid}, user_ctx:get_user_id(UserCtx)), %todo pass file_ctx
    fslogic_event:emit_file_perm_changed(Uuid), %todo pass file_ctx

    #fuse_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Changes file access times.
%% @end
%%--------------------------------------------------------------------
-spec update_times(user_ctx:ctx(), file_ctx:ctx(),
    ATime :: file_meta:time() | undefined,
    MTime :: file_meta:time() | undefined,
    CTime :: file_meta:time() | undefined) -> fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}, {{owner, 'or', ?write_attributes}, 2}]).
update_times(UserCtx, FileCtx, ATime, MTime, CTime) ->
    UpdateMap = #{atime => ATime, mtime => MTime, ctime => CTime},
    UpdateMap1 = maps:filter(fun(_Key, Value) ->
        is_integer(Value) end, UpdateMap),

    FileEntry = file_ctx:get_uuid_entry_const(FileCtx),
    fslogic_times:update_times_and_emit(FileEntry, UpdateMap1, user_ctx:get_user_id(UserCtx)), %todo pass file_ctx

    #fuse_response{status = #status{code = ?OK}}.