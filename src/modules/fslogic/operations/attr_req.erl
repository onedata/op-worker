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
-check_permissions([traverse_ancestors]).
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
    {#document{key = Uuid, value = #file_meta{
        type = Type, mode = Mode, provider_id = ProviderId, owner = OwnerId,
        shares = Shares}}, FileCtx2
    } = file_ctx:get_file_doc(FileCtx),
    ShareId = file_ctx:get_share_id_const(FileCtx),
    {FileName, FileCtx3} = file_ctx:get_aliased_name(FileCtx2, UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx3),
    {{Uid, Gid}, FileCtx4} = file_ctx:get_posix_storage_user_context(FileCtx3),
    Size = fslogic_blocks:get_file_size(FileCtx4),
    {ParentGuid, FileCtx5} = file_ctx:get_parent_guid(FileCtx4, UserCtx),
    {{ATime, CTime, MTime}, _FileCtx6} = file_ctx:get_times(FileCtx5),

    #fuse_response{status = #status{code = ?OK}, fuse_response = #file_attr{
        uid = Uid, gid = Gid, parent_uuid = ParentGuid,
        guid = fslogic_uuid:uuid_to_share_guid(Uuid, SpaceId, ShareId),
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
-check_permissions([traverse_ancestors, ?traverse_container]).
get_child_attr(UserCtx, ParentFileCtx, Name) ->
    {ChildFileCtx, _NewParentFileCtx} = file_ctx:get_child(ParentFileCtx, Name, UserCtx),
    attr_req:get_file_attr(UserCtx, ChildFileCtx).

%%--------------------------------------------------------------------
%% @doc
%% Changes file permissions.
%% @end
%%--------------------------------------------------------------------
-spec chmod(user_ctx:ctx(), file_ctx:ctx(), Perms :: fslogic_worker:posix_permissions()) ->
    fslogic_worker:fuse_response().
-check_permissions([traverse_ancestors, owner]).
chmod(UserCtx, FileCtx, Mode) ->
    ok = sfm_utils:chmod_storage_file(UserCtx, FileCtx, Mode),

    % remove acl
    xattr:delete_by_name(FileCtx, ?ACL_KEY),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {ok, _} = file_meta:update({uuid, FileUuid}, #{mode => Mode}),
    ok = permissions_cache:invalidate(file_meta, FileCtx),

    fslogic_times:update_ctime(FileCtx),
    fslogic_event_emitter:emit_file_perm_changed(FileCtx),

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
-check_permissions([traverse_ancestors, {owner, 'or', ?write_attributes}]).
update_times(_UserCtx, FileCtx, ATime, MTime, CTime) ->
    UpdateMap = #{atime => ATime, mtime => MTime, ctime => CTime},
    UpdateMap1 = maps:filter(fun(_Key, Value) ->
        is_integer(Value) end, UpdateMap),
    fslogic_times:update_times_and_emit(FileCtx, UpdateMap1),
    #fuse_response{status = #status{code = ?OK}}.