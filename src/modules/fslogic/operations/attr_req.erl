%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Requests operating on file attributes.
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
-export([get_file_attr/2, get_file_attr_no_permission_check/2, get_child_attr/3, chmod/3, update_times/5]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check perms and get file's attributes
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr(user_context:ctx(), file_context:ctx()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}]).
get_file_attr(Ctx, File) ->
    get_file_attr_no_permission_check(Ctx, File).

%%--------------------------------------------------------------------
%% @doc
%% Get file's attributes.
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr_no_permission_check(user_context:ctx(), file_context:ctx()) ->
    fslogic_worker:fuse_response().
get_file_attr_no_permission_check(Ctx, File) ->
    {FileDoc = #document{key = Uuid, value = #file_meta{
        type = Type, mode = Mode, provider_id = ProviderId, uid = OwnerId,
        shares = Shares}}, File2
    } = file_context:get_file_doc(File),
    ShareId = file_context:get_share_id(File),
    UserId = user_context:get_user_id(Ctx),
    {FileName, File3} = file_context:get_aliased_name(File2, Ctx),
    SpaceId = file_context:get_space_id(File3),
    {{Uid, Gid}, File4} = file_context:get_posix_storage_user_context(File3, OwnerId),
    Size = fslogic_blocks:get_file_size(FileDoc), %todo TL consider caching file_location in File record
    {ParentGuid, File5} = file_context:get_parent_guid(File4, UserId),
    {{ATime, CTime, MTime}, _File6} = file_context:get_times(File5),

    #fuse_response{status = #status{code = ?OK}, fuse_response = #file_attr{
        uid = Uid, gid = Gid, parent_uuid = ParentGuid,
        uuid = fslogic_uuid:uuid_to_share_guid(Uuid, SpaceId, ShareId),
        type = Type, mode = Mode, atime = ATime, mtime = MTime,
        ctime = CTime, size = Size, name = FileName, provider_id = ProviderId,
        shares = Shares, owner_id = OwnerId
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Fetch attributes of directory's child (if exists).
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr(user_context:ctx(), ParentFile :: file_context:ctx(),
    Name :: file_meta:name()) -> fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}]).
get_child_attr(Ctx, ParentFile, Name) ->
    UserId = user_context:get_user_id(Ctx),
    {ChildFile, _NewParentFile} = file_context:get_child(ParentFile, Name, UserId),
    attr_req:get_file_attr(Ctx, ChildFile).

%%--------------------------------------------------------------------
%% @doc
%% Change file permissions.
%% @end
%%--------------------------------------------------------------------
-spec chmod(user_context:ctx(), file_context:ctx(), Perms :: fslogic_worker:posix_permissions()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}, {owner, 2}]).
chmod(Ctx, File, Mode) ->
    {Guid, _File2} = file_context:get_guid(File),
    Uuid = fslogic_uuid:guid_to_uuid(Guid),
    sfm_utils:chmod_storage_files(Ctx, {uuid, Uuid}, Mode), %todo pass file_context

    % remove acl
    xattr:delete_by_name(Uuid, ?ACL_KEY),
    {ok, _} = file_meta:update({uuid, Uuid}, #{mode => Mode}),
    ok = permissions_cache:invalidate_permissions_cache(file_meta, Uuid),

    fslogic_times:update_ctime({uuid, Uuid}, user_context:get_user_id(Ctx)), %todo pass file_context
    fslogic_event:emit_file_perm_changed(Uuid), %todo pass file_context

    #fuse_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Change file's access times.
%% @end
%%--------------------------------------------------------------------
-spec update_times(user_context:ctx(), file_context:ctx(),
    ATime :: file_meta:time() | undefined,
    MTime :: file_meta:time() | undefined,
    CTime :: file_meta:time() | undefined) -> fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}, {{owner, 'or', ?write_attributes}, 2}]).
update_times(Ctx, File, ATime, MTime, CTime) ->
    UpdateMap = #{atime => ATime, mtime => MTime, ctime => CTime},
    UpdateMap1 = maps:filter(fun(_Key, Value) ->
        is_integer(Value) end, UpdateMap),

    {Guid, _File2} = file_context:get_guid(File),
    Uuid = fslogic_uuid:guid_to_uuid(Guid),
    fslogic_times:update_times_and_emit({uuid, Uuid}, UpdateMap1, user_context:get_user_id(Ctx)), %todo pass file_context

    #fuse_response{status = #status{code = ?OK}}.