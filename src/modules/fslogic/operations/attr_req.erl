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

-include("modules/fslogic/file_info.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([
    get_file_attr/2, get_file_attr_insecure/2, get_file_attr_insecure/3,
    get_file_attr_insecure/4, get_file_attr_light/3, get_file_attr_and_conflicts/5,
    get_file_info/2,
    get_child_attr/3, chmod/3, update_times/5,
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
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0, [traverse_ancestors], allow_ancestors
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
    {Ans, _} = get_file_attr_and_conflicts(UserCtx, FileCtx, AllowDeletedFiles, IncludeSize, true),
    Ans.

%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes. Internal function - no permissions check, no name verification, no deleted files.
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr_light(user_ctx:ctx(), file_ctx:ctx(),
    IncludeSize :: boolean()) -> fslogic_worker:fuse_response().
get_file_attr_light(UserCtx, FileCtx, IncludeSize) ->
    {Ans, _} = get_file_attr_and_conflicts(UserCtx, FileCtx, false, IncludeSize, false),
    Ans.

%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes and information about conflicts. When the AllowDeletedFiles flag is set to true,
%% function will return attributes even for files that are marked as deleted.
%% Returns also list of conflicting files.
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr_and_conflicts(user_ctx:ctx(), file_ctx:ctx(),
    AllowDeletedFiles :: boolean(), IncludeSize :: boolean(), VerifyName :: boolean()) ->
    {fslogic_worker:fuse_response(), Conflicts :: [{file_meta:uuid(), file_meta:name()}]}.
get_file_attr_and_conflicts(UserCtx, FileCtx, AllowDeletedFiles, IncludeSize, VerifyName) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    ShareId = file_ctx:get_share_id_const(FileCtx),

    {#document{
        key = Uuid,
        value = #file_meta{
            type = Type,
            mode = Mode,
            provider_id = ProviderId,
            owner = OwnerId,
            shares = Shares
        }
    } = Doc, FileCtx2} = case AllowDeletedFiles of
        true ->
            file_ctx:get_file_doc_including_deleted(FileCtx);
        false ->
            file_ctx:get_file_doc(FileCtx)
    end,

    {FileName, FileCtx3} = file_ctx:get_aliased_name(FileCtx2, UserCtx),
    {{Uid, Gid}, FileCtx4} = file_ctx:get_posix_storage_user_context(FileCtx3, UserCtx),

    {Size, FileCtx5} = case IncludeSize of
        true -> file_ctx:get_file_size(FileCtx4);
        _ -> {undefined, FileCtx4}
    end,

    {{ATime, CTime, MTime}, FileCtx6} = file_ctx:get_times(FileCtx5),
    {ParentGuid, _FileCtx7} = file_ctx:get_parent_guid(FileCtx6, UserCtx),

    {FinalName, ConflictingFiles} = case VerifyName andalso ParentGuid =/= undefined of
        true ->
            case file_meta:check_name(file_id:guid_to_uuid(ParentGuid), FileName, Doc) of
                {conflicting, ExtendedName, Others} -> {ExtendedName, Others};
                _ -> {FileName, []}
            end;
        _ ->
            {FileName, []}
    end,

    {#fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_attr{
            uid = Uid,  % TODO VFS-6095
            gid = Gid,  % TODO VFS-6095
            parent_uuid = ParentGuid,
            guid = file_id:pack_share_guid(Uuid, SpaceId, ShareId),
            type = Type,
            mode = Mode,
            atime = ATime,
            mtime = MTime,
            ctime = CTime,
            size = Size,
            name = FinalName,
            provider_id = ProviderId,
            shares = filter_visible_shares(ShareId, Shares),
            owner_id = OwnerId  % TODO VFS-6095
        }
    }, ConflictingFiles}.


%%--------------------------------------------------------------------
%% @equiv get_file_details_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_file_info(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_file_info(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0, [traverse_ancestors], allow_ancestors
    ),
    get_file_info_insecure(UserCtx, FileCtx1).


%%--------------------------------------------------------------------
%% @equiv get_child_attr_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr(user_ctx:ctx(), ParentFile :: file_ctx:ctx(),
    Name :: file_meta:name()) -> fslogic_worker:fuse_response().
get_child_attr(UserCtx, ParentFileCtx0, Name) ->
    ParentFileCtx1 = fslogic_authz:ensure_authorized(
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
    FileCtx1 = fslogic_authz:ensure_authorized(
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
    FileCtx1 = fslogic_authz:ensure_authorized(
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
%% This function ensures that requested Name is in returned #file_attr{} if it has no suffix.
%% Used to prevent races connected with remote rename.
%% @end
%%-------------------------------------------------------------------
-spec ensure_proper_file_name(fslogic_worker:fuse_response(), file_meta:name()) ->
    fslogic_worker:fuse_response().
ensure_proper_file_name(FuseResponse = #fuse_response{
    status = #status{code = ?OK},
    fuse_response = #file_attr{name = AnsName} = FileAttr
}, Name) ->
    case file_meta:has_suffix(Name) of
        {true, AnsName} -> FuseResponse;
        _ -> FuseResponse#fuse_response{fuse_response = FileAttr#file_attr{name = Name}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Changes file posix mode.
%% @end
%%--------------------------------------------------------------------
-spec chmod_insecure(user_ctx:ctx(), file_ctx:ctx(), fslogic_worker:posix_permissions()) ->
    fslogic_worker:fuse_response().
chmod_insecure(UserCtx, FileCtx, Mode) ->
    sd_utils:chmod_storage_file(UserCtx, FileCtx, Mode),
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
    fslogic_event_emitter:emit_sizeless_file_attrs_changed(FileCtx),
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


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes and additional information like active permissions
%% type or existence of metadata, qos.
%% @end
%%--------------------------------------------------------------------
-spec get_file_info_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_file_info_insecure(UserCtx, FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    ShareId = file_ctx:get_share_id_const(FileCtx),

    {#document{
        key = Uuid,
        value = #file_meta{
            name = FileName,
            type = Type,
            mode = Mode,
            provider_id = ProviderId,
            owner = OwnerId,
            shares = Shares
        }
    } = FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, ActivePermissionsType} = file_meta:get_active_perms_type(FileDoc),

    {ShownName, FileCtx3} = case Uuid == SpaceId of
        true -> file_ctx:get_aliased_name(FileCtx2, UserCtx);
        _ -> {FileName, FileCtx2}
    end,

    {{Uid, Gid}, FileCtx4} = file_ctx:get_posix_storage_user_context(FileCtx3, UserCtx),

    {Size, FileCtx5} = file_ctx:get_file_size(FileCtx4),
    {{ATime, CTime, MTime}, FileCtx6} = file_ctx:get_times(FileCtx5),
    {ParentGuid, FileCtx7} = file_ctx:get_parent_guid(FileCtx6, UserCtx),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_info{
            guid = file_id:pack_share_guid(Uuid, SpaceId, ShareId),
            name = ShownName,
            active_permissions_type = ActivePermissionsType,
            mode = Mode,
            parent_guid = ParentGuid,
            uid = Uid,  % TODO VFS-6095
            gid = Gid,  % TODO VFS-6095
            atime = ATime,
            mtime = MTime,
            ctime = CTime,
            type = Type,
            size = Size,
            shares = filter_visible_shares(ShareId, Shares),
            provider_id = ProviderId,
            owner_id = OwnerId,  % TODO VFS-6095
            has_metadata = has_metadata(FileCtx7)
        }
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Filters shares list as to not show other shares ids when accessing
%% file via share guid (using specific ShareId).
%% When accessing file in normal mode all shares are returned.
%%
%% NOTE !!!
%% ShareId is added to file_meta.shares only for directly shared
%% files/directories and not their children, so not every file
%% accessed via share guid will have ShareId in `file_attrs.shares`
%% @end
%%--------------------------------------------------------------------
-spec filter_visible_shares(undefined | od_share:id(), [od_share:id()]) ->
    [od_share:id()].
filter_visible_shares(undefined, Shares) ->
    Shares;
filter_visible_shares(ShareId, Shares) ->
    case lists:member(ShareId, Shares) of
        true -> [ShareId];
        false -> []
    end.


%% @private
-spec has_metadata(file_ctx:ctx()) -> boolean().
has_metadata(FileCtx) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    case xattr_req:list_xattr_insecure(RootUserCtx, FileCtx, false, true) of
        #fuse_response{
            status = #status{code = ?OK},
            fuse_response = #xattr_list{names = XattrList}
        } ->
            lists:any(fun
                (?JSON_METADATA_KEY) ->
                    true;
                (?RDF_METADATA_KEY) ->
                    true;
                (<<?CDMI_PREFIX_STR, _/binary>>) ->
                    false;
                (<<?ONEDATA_PREFIX_STR, _/binary>>) ->
                    false;
                (_) ->
                    true
            end, XattrList);
        _ ->
            false
    end.
