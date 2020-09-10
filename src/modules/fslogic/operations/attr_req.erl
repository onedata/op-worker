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

-include("modules/fslogic/file_details.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/privileges.hrl").

%% API
-export([
    get_file_attr/2, get_file_attr_insecure/3,
    get_file_attr_and_conflicts_insecure/3,

    get_file_details/2, get_file_details_insecure/3,

    get_child_attr/3, chmod/3, update_times/5,
    chmod_attrs_only_insecure/2,

    get_fs_stats/2
]).

-type name_conflicts_resolution_policy() ::
    resolve_name_conflicts |
    allow_name_conflicts.

-type compute_file_attr_opts() :: #{
    % Tells whether to calculate attr even if file was recently removed.
    allow_deleted_files => boolean(),

    % Tells whether to calculate size of file.
    include_size => boolean(),

    % Tells whether to perform a check if file name collide with other files in
    % directory. If it does suffix will be glued to name to differentiate it
    % and conflicting files will be returned.
    name_conflicts_resolution_policy => name_conflicts_resolution_policy()
}.

-export_type([name_conflicts_resolution_policy/0, compute_file_attr_opts/0]).


-type file_private_attrs_and_ctx() :: {
    Mode :: non_neg_integer(),
    Uid :: luma:uid(),
    Gid :: luma:gid(),
    OwnerId :: od_user:id(),
    ProviderId :: od_provider:id(),
    Shares :: [od_share:id()],
    FileCtx :: file_ctx:ctx()
}.


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
    get_file_attr_insecure(UserCtx, FileCtx1, #{
        allow_deleted_files => false,
        include_size => true,
        name_conflicts_resolution_policy => resolve_name_conflicts
    }).


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes depending on specific flags set.
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr_insecure(user_ctx:ctx(), file_ctx:ctx(), compute_file_attr_opts()) ->
    fslogic_worker:fuse_response().
get_file_attr_insecure(UserCtx, FileCtx, Opts) ->
    {Ans, _, _} = get_file_attr_and_conflicts_insecure(UserCtx, FileCtx, Opts),
    Ans.


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes and information about conflicts depending on
%% specific flags set.
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr_and_conflicts_insecure(user_ctx:ctx(), file_ctx:ctx(), compute_file_attr_opts()) ->
    {
        fslogic_worker:fuse_response(),
        Conflicts :: [{file_meta:uuid(), file_meta:name()}],
        IsDeleted :: boolean()
    }.
get_file_attr_and_conflicts_insecure(UserCtx, FileCtx, Opts) ->
    {FileAttr, FileDoc, ConflictingFiles, _FileCtx2} = resolve_file_attr(
        UserCtx, FileCtx, Opts
    ),
    FuseResponse = #fuse_response{
        status = #status{code = ?OK},
        fuse_response = FileAttr
    },
    {FuseResponse, ConflictingFiles, file_meta:is_deleted(FileDoc)}.


%%--------------------------------------------------------------------
%% @equiv get_file_details_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_file_details(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_file_details(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0, [traverse_ancestors], allow_ancestors
    ),
    get_file_details_insecure(UserCtx, FileCtx1, #{
        allow_deleted_files => false,
        include_size => true,
        name_conflicts_resolution_policy => resolve_name_conflicts
    }).


%%--------------------------------------------------------------------
%% @doc
%% Returns file details (see file_details.hrl).
%% @end
%%--------------------------------------------------------------------
-spec get_file_details_insecure(user_ctx:ctx(), file_ctx:ctx(), compute_file_attr_opts()) ->
    fslogic_worker:fuse_response().
get_file_details_insecure(UserCtx, FileCtx, Opts) ->
    {FileAttr, FileDoc, _, FileCtx2} = resolve_file_attr(UserCtx, FileCtx, Opts),
    {ok, ActivePermissionsType} = file_meta:get_active_perms_type(FileDoc),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_details{
            file_attr = FileAttr,
            index_startid = file_meta:get_name(FileDoc),
            active_permissions_type = ActivePermissionsType,
            has_metadata = has_metadata(FileCtx2),
            has_direct_qos = file_qos:has_any_qos_entry(FileDoc, direct),
            has_eff_qos = file_qos:has_any_qos_entry(FileDoc, effective)
        }
    }.


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


%%--------------------------------------------------------------------
%% @equiv get_fs_stats/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_fs_stats(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_fs_stats(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0, [traverse_ancestors]
    ),
    get_fs_stats_insecure(UserCtx, FileCtx1).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
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
%% @private
%% @doc
%% Changes file posix mode.
%% @end
%%--------------------------------------------------------------------
-spec chmod_insecure(user_ctx:ctx(), file_ctx:ctx(), fslogic_worker:posix_permissions()) ->
    fslogic_worker:fuse_response().
chmod_insecure(UserCtx, FileCtx, Mode) ->
    sd_utils:chmod(UserCtx, FileCtx, Mode),
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
%% @private
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
%% @private
%% @doc
%% Resolves attributes of a file. Depending on compute_file_attr_opts() set
%% some attributes may be left undefined (see description of individual
%% options).
%% @end
%%--------------------------------------------------------------------
-spec resolve_file_attr(user_ctx:ctx(), file_ctx:ctx(), compute_file_attr_opts()) ->
    {
        #file_attr{},
        FileDoc :: file_meta:doc(),
        Conflicts :: [{file_meta:uuid(), file_meta:name()}],
        file_ctx:ctx()
    }.
resolve_file_attr(UserCtx, FileCtx, Opts) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    {_FileUuid, _SpaceId, ShareId} = file_id:unpack_share_guid(FileGuid),

    {FileDoc, FileCtx2} = case maps:get(allow_deleted_files, Opts, false) of
        true ->
            file_ctx:get_file_doc_including_deleted(FileCtx);
        false ->
            file_ctx:get_file_doc(FileCtx)
    end,

    {{ATime, CTime, MTime}, FileCtx3} = file_ctx:get_times(FileCtx2),
    {ParentGuid, FileCtx4} = file_ctx:get_parent_guid(FileCtx3, UserCtx),

    {Mode, Uid, Gid, OwnerId, ProviderId, Shares, FileCtx5} = case ShareId of
        undefined -> get_private_attrs(UserCtx, FileCtx4, FileDoc);
        _ -> get_masked_private_attrs(ShareId, FileCtx4, FileDoc)
    end,
    {Size, FileCtx6} = case maps:get(include_size, Opts, true) of
        true -> file_ctx:get_file_size(FileCtx5);
        _ -> {undefined, FileCtx5}
    end,
    {FileName, ConflictingFiles, FileCtx7} = resolve_file_name(
        UserCtx, FileDoc, FileCtx6, ParentGuid,
        maps:get(name_conflicts_resolution_policy, Opts, resolve_name_conflicts)
    ),

    FileAttr = #file_attr{
        guid = FileGuid,
        name = FileName,
        mode = Mode,
        parent_uuid = ParentGuid,
        uid = Uid,
        gid = Gid,
        atime = ATime,
        mtime = MTime,
        ctime = CTime,
        type = file_meta:get_type(FileDoc),
        size = Size,
        shares = Shares,
        provider_id = ProviderId,
        owner_id = OwnerId
    },
    {FileAttr, FileDoc, ConflictingFiles, FileCtx7}.


%% @private
-spec get_private_attrs(user_ctx:ctx(), file_ctx:ctx(), file_meta:doc()) ->
    file_private_attrs_and_ctx().
get_private_attrs(UserCtx, FileCtx0, #document{
    value = #file_meta{
        mode = Mode,
        provider_id = ProviderId,
        owner = OwnerId,
        shares = Shares
    }
}) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx0),
    VisibleShares = case user_ctx:is_root(UserCtx) of
        true ->
            Shares;
        false ->
            UserId = user_ctx:get_user_id(UserCtx),
            case space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW) of
                true -> Shares;
                false -> []
            end
    end,
    {{Uid, Gid}, FileCtx1} = file_ctx:get_display_credentials(FileCtx0),
    {Mode, Uid, Gid, OwnerId, ProviderId, VisibleShares, FileCtx1}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns masked private attrs values when accessing file via share guid (e.g.
%% in share mode only last 3 bits of mode - 'other' bits - should be visible).
%%
%% NOTE !!!
%% ShareId is added to file_meta.shares only for directly shared
%% files/directories and not their children, so not every file
%% accessed via share guid will have ShareId in `file_attrs.shares`
%% @end
%%--------------------------------------------------------------------
-spec get_masked_private_attrs(od_share:id(), file_ctx:ctx(), file_meta:doc()) ->
    file_private_attrs_and_ctx().
get_masked_private_attrs(ShareId, FileCtx, #document{
    value = #file_meta{
        mode = RealMode,
        shares = AllShares
    }
}) ->
    Mode = RealMode band 2#111,
    Shares = case lists:member(ShareId, AllShares) of
        true -> [ShareId];
        false -> []
    end,
    {Mode, ?SHARE_UID, ?SHARE_GID, <<"unknown">>, <<"unknown">>, Shares, FileCtx}.


%% @private
-spec resolve_file_name(
    user_ctx:ctx(),
    file_meta:doc(),
    file_ctx:ctx(),
    ParentGuid :: undefined | file_id:file_guid(),
    name_conflicts_resolution_policy()
) ->
    {
        file_meta:name(),
        ConflictingFiles :: [{file_meta:uuid(), file_meta:name()}],
        file_ctx:ctx()
    }.
resolve_file_name(UserCtx, FileDoc, FileCtx0, <<_/binary>> = ParentGuid, resolve_name_conflicts) ->
    ParentUuid = file_id:guid_to_uuid(ParentGuid),
    {FileName, FileCtx1} = file_ctx:get_aliased_name(FileCtx0, UserCtx),

    case file_meta:check_name(ParentUuid, FileName, FileDoc) of
        {conflicting, ExtendedName, ConflictingFiles} ->
            {ExtendedName, ConflictingFiles, FileCtx1};
        _ ->
            {FileName, [], FileCtx1}
    end;
resolve_file_name(UserCtx, _FileDoc, FileCtx0, _ParentGuid, _NameConflictResolutionPolicy) ->
    {FileName, FileCtx1} = file_ctx:get_aliased_name(FileCtx0, UserCtx),
    {FileName, [], FileCtx1}.


%% @private
-spec has_metadata(file_ctx:ctx()) -> boolean().
has_metadata(FileCtx) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    {ok, XattrList} = xattr:list_xattrs_insecure(RootUserCtx, FileCtx, false, true),
    lists:any(fun
        (<<?CDMI_PREFIX_STR, _/binary>>) -> false;
        (?JSON_METADATA_KEY) -> true;
        (?RDF_METADATA_KEY) -> true;
        (<<?ONEDATA_PREFIX_STR, _/binary>>) -> false;
        (_) -> true
    end, XattrList).


%% @private
-spec get_fs_stats_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_fs_stats_insecure(_UserCtx, FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),

    %% @TODO VFS-5497 Calc size/occupied for all supporting storages
    {ok, StorageId} = space_logic:get_local_storage_id(SpaceId),
    {ok, SupportSize} = provider_logic:get_support_size(SpaceId),
    Occupied = space_quota:current_size(SpaceId),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #fs_stats{
            space_id = SpaceId,
            storage_stats = [#storage_stats{
                storage_id = StorageId,
                size = SupportSize,
                occupied = Occupied
            }]
        }
    }.
