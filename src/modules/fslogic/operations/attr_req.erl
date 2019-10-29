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
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([
    get_file_attr/2, get_file_attr_insecure/2, get_file_attr_insecure/3,
    get_file_attr_insecure/4, get_child_attr/3, chmod/3, update_times/5,
    chmod_attrs_only_insecure/2
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @equiv get_file_attr_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_file_attr(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:authorize(
        UserCtx, FileCtx0, [traverse_ancestors], false
    ),
    get_file_attr_insecure(UserCtx, FileCtx1).


%%--------------------------------------------------------------------
%% @equiv get_file_attr_insecure(UserCtx, FileCtx, false).
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_file_attr_insecure(UserCtx, FileCtx) ->
    get_file_attr_insecure(UserCtx, FileCtx, false).


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes. When the AllowDeletedFiles flag is set to true,
%% function will return attributes even for files that are marked as deleted.
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr_insecure(user_ctx:ctx(), file_ctx:ctx(),
    AllowDeletedFiles :: boolean()) ->
fslogic_worker:fuse_response().
get_file_attr_insecure(UserCtx, FileCtx, AllowDeletedFiles) ->
    get_file_attr_insecure(UserCtx, FileCtx, AllowDeletedFiles, true).


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes. When the AllowDeletedFiles flag is set to true,
%% function will return attributes even for files that are marked as deleted.
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr_insecure(user_ctx:ctx(), file_ctx:ctx(),
    AllowDeletedFiles :: boolean(), IncludeSize :: boolean()) ->
    fslogic_worker:fuse_response().
get_file_attr_insecure(UserCtx, FileCtx, AllowDeletedFiles, IncludeSize) ->
    {#document{
        key = Uuid,
        value = #file_meta{
            type = Type,
            mode = Mode,
            provider_id = ProviderId,
            owner = OwnerId,
            shares = Shares
        }
    }, FileCtx2} = case AllowDeletedFiles of
        true ->
            file_ctx:get_file_doc_including_deleted(FileCtx);
        false ->
            file_ctx:get_file_doc(FileCtx)
    end,
    ShareId = file_ctx:get_share_id_const(FileCtx),
    {FileName, FileCtx3} = file_ctx:get_aliased_name(FileCtx2, UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx3),
    {{Uid, Gid}, FileCtx4} = file_ctx:get_posix_storage_user_context(FileCtx3, UserCtx),

    {Size, FileCtx5} = case IncludeSize of
        true -> file_ctx:get_file_size(FileCtx4);
        _ -> {undefined, FileCtx4}
    end,

    {{ATime, CTime, MTime}, FileCtx6} = file_ctx:get_times(FileCtx5),
    {ParentGuid, _FileCtx7} = file_ctx:get_parent_guid(FileCtx6, UserCtx),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_attr{
            uid = Uid,
            gid = Gid,
            parent_uuid = ParentGuid,
            guid = file_id:pack_share_guid(Uuid, SpaceId, ShareId),
            type = Type,
            mode = Mode,
            atime = ATime,
            mtime = MTime,
            ctime = CTime,
            size = Size,
            name = FileName,
            provider_id = ProviderId,
            shares = Shares,
            owner_id = OwnerId
        }
    }.


%%--------------------------------------------------------------------
%% @equiv get_child_attr_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr(user_ctx:ctx(), ParentFile :: file_ctx:ctx(),
    Name :: file_meta:name()) -> fslogic_worker:fuse_response().
get_child_attr(UserCtx, ParentFileCtx0, Name) ->
    ParentFileCtx1 = fslogic_authz:authorize(
        UserCtx, ParentFileCtx0,
        [traverse_ancestors, ?traverse_container]
    ),
    get_child_attr_insecure(UserCtx, ParentFileCtx1, Name).


%%--------------------------------------------------------------------
%% @equiv chmod_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec chmod(user_ctx:ctx(), file_ctx:ctx(), fslogic_worker:posix_permissions()) ->
    fslogic_worker:fuse_response().
chmod(UserCtx, FileCtx0, Mode) ->
    FileCtx1 = fslogic_authz:authorize(
        UserCtx, FileCtx0,
        [traverse_ancestors, owner]
    ),
    chmod_insecure(UserCtx, FileCtx1, Mode).


%%--------------------------------------------------------------------
%% @equiv update_times_insecure/5 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec update_times(user_ctx:ctx(), file_ctx:ctx(),
    ATime :: file_meta:time() | undefined,
    MTime :: file_meta:time() | undefined,
    CTime :: file_meta:time() | undefined) -> fslogic_worker:fuse_response().
update_times(UserCtx, FileCtx0, ATime, MTime, CTime) ->
    FileCtx1 = fslogic_authz:authorize(
        UserCtx, FileCtx0,
        [traverse_ancestors, {owner, 'or', ?write_attributes}]
    ),
    update_times_insecure(UserCtx, FileCtx1, ATime, MTime, CTime).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns attributes of directory child (if exists).
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr_insecure(user_ctx:ctx(), ParentFile :: file_ctx:ctx(),
    Name :: file_meta:name()) -> fslogic_worker:fuse_response().
get_child_attr_insecure(UserCtx, ParentFileCtx, Name) ->
    {ChildFileCtx, _NewParentFileCtx} = file_ctx:get_child(ParentFileCtx, Name, UserCtx),
    Response = attr_req:get_file_attr(UserCtx, ChildFileCtx),
    ensure_proper_file_name(Response, Name).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function ensures that requested Name is in returned #file_attr{}.
%% @end
%%-------------------------------------------------------------------
-spec ensure_proper_file_name(fslogic_worker:fuse_response(), file_meta:name()) ->
    fslogic_worker:fuse_response().
ensure_proper_file_name(FuseResponse = #fuse_response{
    status = #status{code = ?OK},
    fuse_response = FileAttr
}, Name) ->
    FuseResponse#fuse_response{fuse_response = FileAttr#file_attr{name = Name}}.


%%--------------------------------------------------------------------
%% @doc
%% Changes file posix mode.
%% @end
%%--------------------------------------------------------------------
-spec chmod_insecure(user_ctx:ctx(), file_ctx:ctx(), fslogic_worker:posix_permissions()) ->
    fslogic_worker:fuse_response().
chmod_insecure(UserCtx, FileCtx, Mode) ->
    sfm_utils:chmod_storage_file(UserCtx, FileCtx, Mode),
    chmod_attrs_only_insecure(FileCtx, Mode),
    fslogic_times:update_ctime(FileCtx),

    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc
%% Changes file permissions (only file_attrs, not on storage)
%% @end
%%--------------------------------------------------------------------
-spec chmod_attrs_only_insecure(file_ctx:ctx(),
    fslogic_worker:posix_permissions()) -> ok | {error, term()}.
chmod_attrs_only_insecure(FileCtx, Mode) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    ok = file_meta:update_mode(FileUuid, Mode),
    ok = permissions_cache:invalidate(),
    fslogic_event_emitter:emit_file_perm_changed(FileCtx).


%%--------------------------------------------------------------------
%% @doc
%% Changes file access times.
%% @end
%%--------------------------------------------------------------------
-spec update_times_insecure(user_ctx:ctx(), file_ctx:ctx(),
    ATime :: file_meta:time() | undefined,
    MTime :: file_meta:time() | undefined,
    CTime :: file_meta:time() | undefined) -> fslogic_worker:fuse_response().
update_times_insecure(_UserCtx, FileCtx, ATime, MTime, CTime) ->
    TimesDiff1 = fun
        (Times = #times{}) when ATime == undefined -> Times;
        (Times = #times{}) -> Times#times{atime = ATime}
    end,
    TimesDiff2 = fun
        (Times = #times{}) when MTime == undefined -> Times;
        (Times = #times{}) -> Times#times{mtime = MTime}
    end,
    TimesDiff3 = fun
        (Times = #times{}) when CTime == undefined -> Times;
        (Times = #times{}) -> Times#times{ctime = CTime}
    end,
    TimesDiff = fun(Times = #times{}) ->
        {ok, TimesDiff1(TimesDiff2(TimesDiff3(Times)))}
    end,
    fslogic_times:update_times_and_emit(FileCtx, TimesDiff),
    #fuse_response{status = #status{code = ?OK}}.
