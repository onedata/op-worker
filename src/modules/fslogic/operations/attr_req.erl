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

-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/file_details.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/privileges.hrl").

%% API
-export([
    get_file_attr/4, get_file_attr_insecure/3,
    get_file_attr_and_conflicts_insecure/3,

    get_file_details/2, get_file_details_insecure/3,

    get_file_references/2,

    get_child_attr/5, chmod/3, update_times/5,
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
    name_conflicts_resolution_policy => name_conflicts_resolution_policy(),

    % Tells whether replication status should be included in answer.
    include_replication_status => boolean(),

    % Tells whether hardlink count should be included in answer.
    include_link_count => boolean(),

    % Tells whether fields calculated using effective value should be included in answer.
    effective_values_references_limit => non_neg_integer() | infinity
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


-define(DEFAULT_REFERENCES_LIMIT, 100).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @equiv get_file_attr_insecure/3 with permission checks and default options
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr(user_ctx:ctx(), file_ctx:ctx(), boolean(), boolean()) ->
    fslogic_worker:fuse_response().
get_file_attr(UserCtx, FileCtx0, IncludeReplicationStatus, IncludeLinkCount) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS], allow_ancestors
    ),
    get_file_attr_insecure(UserCtx, FileCtx1, #{
        allow_deleted_files => false,
        include_size => true,
        name_conflicts_resolution_policy => resolve_name_conflicts,
        include_replication_status => IncludeReplicationStatus,
        include_link_count => IncludeLinkCount
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
        Conflicts :: file_meta:conflicts(),
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
        UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS], allow_ancestors
    ),
    get_file_details_insecure(UserCtx, FileCtx1, #{
        allow_deleted_files => false,
        include_size => true,
        name_conflicts_resolution_policy => resolve_name_conflicts,
        include_link_count => true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Returns file details (see file_details.hrl).
%% @end
%%--------------------------------------------------------------------
-spec get_file_details_insecure(user_ctx:ctx(), file_ctx:ctx(), compute_file_attr_opts()) ->
    fslogic_worker:fuse_response().
get_file_details_insecure(UserCtx, FileCtx, Opts) ->
    {#file_attr{name = FileAttrName} = FileAttr, FileDoc, _, FileCtx2} = resolve_file_attr(UserCtx, FileCtx, Opts),
    {ok, ActivePermissionsType} = file_meta:get_active_perms_type(FileDoc),

    ReferencesLimit = maps:get(effective_values_references_limit, Opts, ?DEFAULT_REFERENCES_LIMIT),
    ShouldCalculateEffectiveValues = case ReferencesLimit of
        infinity -> true;
        _ ->
            case file_meta_hardlinks:count_references(FileDoc) of
                {ok, LinksCount} -> LinksCount =< ReferencesLimit;
                _ -> false
            end
    end,
    {EffectiveValues, FileCtx3} = case ShouldCalculateEffectiveValues of
        true ->
            calculate_effective_values(FileCtx2);
        false ->
            {#{}, FileCtx2}
    end,

    Uuid = file_ctx:get_logical_uuid_const(FileCtx2),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_details{
            file_attr = FileAttr,
            symlink_value = case fslogic_uuid:is_symlink_uuid(Uuid) of
                true ->
                    {ok, SymlinkValue} = file_meta_symlinks:readlink(FileDoc),
                    SymlinkValue;
                false ->
                    undefined
            end,
            index_startid = case file_ctx:is_space_dir_const(FileCtx) of
                true -> 
                    % As provider id in space doc is random (depends on which provider called `file_meta:make_space_exist/0`) 
                    % use only space id in index (there are no conflicts on spaces between providers, so it is not a problem).
                    file_listing:build_index(file_meta:get_name(FileDoc));
                false -> 
                    file_listing:build_index(
                        file_meta:get_name(FileDoc), file_meta:get_provider_id(FileDoc))
            end,
            active_permissions_type = ActivePermissionsType,
            has_metadata = has_metadata(FileCtx3),
            eff_qos_membership = maps:get(effective_qos_membership, EffectiveValues, undefined),
            eff_dataset_membership = maps:get(effective_dataset_membership, EffectiveValues, undefined),
            eff_protection_flags = maps:get(effective_protection_flags, EffectiveValues, undefined),
            recall_root_id = maps:get(effective_recall, EffectiveValues, undefined),
            conflicting_name = case {fslogic_uuid:is_space_dir_uuid(Uuid), file_meta:get_name(FileDoc)} of
                {true, _} -> undefined;
                {false, FileAttrName} -> undefined;
                {false, ConflictingName} -> ConflictingName
            end
        }
    }.


-spec get_file_references(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_file_references(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS, ?OPERATIONS(?read_attributes_mask)]
    ),
    FileCtx2 = file_ctx:assert_not_dir(FileCtx1),

    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    {ok, RefUuids} = file_ctx:list_references_const(FileCtx2),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_references{references = lists:map(fun(RefUuid) ->
            file_id:pack_guid(RefUuid, SpaceId)
        end, RefUuids)}
    }.


%%--------------------------------------------------------------------
%% @equiv get_child_attr_insecure/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr(user_ctx:ctx(), ParentFile :: file_ctx:ctx(),
    Name :: file_meta:name(), boolean(), boolean()) -> fslogic_worker:fuse_response().
get_child_attr(UserCtx, ParentFileCtx0, Name, IncludeReplicationStatus, IncludeLinkCount) ->
    ParentFileCtx1 = ensure_access_to_child(UserCtx, ParentFileCtx0, Name),
    get_child_attr_insecure(
        UserCtx, ParentFileCtx1, Name, IncludeReplicationStatus, IncludeLinkCount
    ).


%%--------------------------------------------------------------------
%% @equiv chmod_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec chmod(user_ctx:ctx(), file_ctx:ctx(), fslogic_worker:posix_permissions()) ->
    fslogic_worker:fuse_response().
chmod(UserCtx, FileCtx0, Mode) ->
    file_ctx:assert_not_special_const(FileCtx0),
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS, ?OWNERSHIP]
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
        [?TRAVERSE_ANCESTORS, ?OR(?OWNERSHIP, ?OPERATIONS(?write_attributes_mask))]
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
        UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS]
    ),
    get_fs_stats_insecure(UserCtx, FileCtx1).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ensure_access_to_child(user_ctx:ctx(), file_ctx:ctx(), file_meta:name()) ->
    file_ctx:ctx().
ensure_access_to_child(UserCtx, ParentFileCtx0, ChildName) ->
    case fslogic_authz:ensure_authorized_readdir(
        UserCtx, ParentFileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask)]
    ) of
        {undefined, ParentFileCtx1} ->
            ParentFileCtx1;
        {CanonicalChildrenWhitelist, ParentFileCtx1} ->
            CanonicalChildName = case file_ctx:is_user_root_dir_const(ParentFileCtx1, UserCtx) of
                true ->
                    SessionId = user_ctx:get_session_id(UserCtx),
                    UserDoc = user_ctx:get_user(UserCtx),

                    case user_logic:get_space_by_name(SessionId, UserDoc, ChildName) of
                        {true, SpaceId} -> SpaceId;
                        false -> throw(?ENOENT)
                    end;
                false ->
                    ChildName
            end,
            lists:member(CanonicalChildName, CanonicalChildrenWhitelist) orelse throw(?ENOENT),
            ParentFileCtx1
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns attributes of directory child (if exists).
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr_insecure(user_ctx:ctx(), ParentFile :: file_ctx:ctx(),
    Name :: file_meta:name(), boolean(), boolean()) -> fslogic_worker:fuse_response().
get_child_attr_insecure(UserCtx, ParentFileCtx, Name, IncludeReplicationStatus, IncludeLinkCount) ->
    {ChildFileCtx, _NewParentFileCtx} = file_tree:get_child(ParentFileCtx, Name, UserCtx),
    Response = attr_req:get_file_attr(UserCtx, ChildFileCtx, IncludeReplicationStatus, IncludeLinkCount),
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
    FileCtx2 = chmod_attrs_only_insecure(FileCtx, Mode),
    fslogic_times:update_ctime(FileCtx2),

    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc
%% Changes file permissions (only file_attrs, not on storage)
%% @end
%%--------------------------------------------------------------------
-spec chmod_attrs_only_insecure(file_ctx:ctx(),
    fslogic_worker:posix_permissions()) -> file_ctx:ctx().
chmod_attrs_only_insecure(FileCtx, Mode) ->
    % TODO VFS-7524 - verify if file_meta doc updates invalidate cached docs in file_ctx everywhere
    % TODO VFS-7525 - Protect races on events production after parallel file_meta updates
    FileUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    {ok, NewDoc} = file_meta:update_mode(FileUuid, Mode),
    ok = permissions_cache:invalidate(),
    FileCtx2 = file_ctx:set_file_doc(FileCtx, NewDoc),
    fslogic_event_emitter:emit_sizeless_file_attrs_changed(FileCtx2),
    fslogic_event_emitter:emit_file_perm_changed(FileCtx2),
    FileCtx2.


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
update_times_insecure(UserCtx, FileCtx, ATime, MTime, CTime) ->
    % Flush events queue to prevent race with file_written_event
    % TODO VFS-7139: This is temporary solution to be removed after fixing oneclient
    SessId = user_ctx:get_session_id(UserCtx),
    catch lfm_event_controller:flush_event_queue(
        SessId, oneprovider:get_id(), file_ctx:get_logical_guid_const(FileCtx)),

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
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    {_FileUuid, _SpaceId, ShareId} = file_id:unpack_share_guid(FileGuid),

    {FileDoc, FileCtx2} = case maps:get(allow_deleted_files, Opts, false) of
        true ->
            file_ctx:get_file_doc_including_deleted(FileCtx);
        false ->
            file_ctx:get_file_doc(FileCtx)
    end,
    EffectiveType = file_meta:get_effective_type(FileDoc),

    {{ATime, CTime, MTime}, FileCtx3} = file_ctx:get_times(FileCtx2),
    {ParentGuid, FileCtx4} = file_tree:get_parent_guid_if_not_root_dir(FileCtx3, UserCtx),

    {Mode, Uid, Gid, OwnerId, ProviderId, Shares, FileCtx5} = case ShareId of
        undefined -> get_private_attrs(UserCtx, FileCtx4, FileDoc);
        _ -> get_masked_private_attrs(ShareId, FileCtx4, FileDoc)
    end,

    {ReplicationStatus, Size, FileCtx6} =
        case {EffectiveType, maps:get(include_replication_status, Opts, false), maps:get(include_size, Opts, true)} of
            {?REGULAR_FILE_TYPE, true, true} ->
                file_ctx:get_replication_status_and_size(FileCtx5);
            {?REGULAR_FILE_TYPE, true, _} ->
                {RS, _, Ctx} = file_ctx:get_replication_status_and_size(FileCtx5),
                {RS, undefined, Ctx};
            {?DIRECTORY_TYPE, _, true} ->
                case dir_size_stats:get_stats(file_ctx:get_logical_guid_const(FileCtx5), [?TOTAL_SIZE]) of
                    {ok, #{?TOTAL_SIZE := S}} ->
                        {undefined, S, FileCtx5};
                    ?ERROR_NOT_FOUND ->
                        {undefined, 0, FileCtx5};
                    ?ERROR_DIR_STATS_DISABLED_FOR_SPACE ->
                        {undefined, undefined, FileCtx5};
                    ?ERROR_DIR_STATS_NOT_READY ->
                        {undefined, undefined, FileCtx5}
                end;
            {?SYMLINK_TYPE, _, true} ->
                {ok, Symlink} = file_meta_symlinks:readlink(FileDoc),
                {undefined, byte_size(Symlink), FileCtx5};
            {_, _, true} ->
                {S, Ctx} = file_ctx:get_file_size(FileCtx5),
                {undefined, S, Ctx};
            _ ->
                {undefined, undefined, FileCtx5}
        end,
    {FileName, ConflictingFiles, FileCtx7} = resolve_file_name(
        UserCtx, FileDoc, FileCtx6, ParentGuid,
        maps:get(name_conflicts_resolution_policy, Opts, resolve_name_conflicts)
    ),

    {ok, LinksCount} = case {ShareId, maps:get(include_link_count, Opts, false)} of
        {undefined, true} ->
            file_ctx:count_references_const(FileCtx7);
        _ ->
            {ok, undefined}
    end,

    FileAttr = #file_attr{
        guid = FileGuid,
        name = FileName,
        mode = Mode,
        parent_guid = ParentGuid,
        uid = Uid,
        gid = Gid,
        atime = ATime,
        mtime = MTime,
        ctime = CTime,
        type = EffectiveType,
        size = Size,
        shares = Shares,
        provider_id = ProviderId,
        owner_id = OwnerId,
        fully_replicated = ReplicationStatus,
        nlink = LinksCount
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
        ConflictingFiles :: file_meta:conflicts(),
        file_ctx:ctx()
    }.
resolve_file_name(UserCtx, FileDoc, FileCtx0, <<_/binary>> = ParentGuid, resolve_name_conflicts) ->
    ParentUuid = file_id:guid_to_uuid(ParentGuid),
    {FileName, FileCtx1} = file_ctx:get_aliased_name(FileCtx0, UserCtx),
    ProviderId = file_meta:get_provider_id(FileDoc),
    {ok, FileUuid} = file_meta:get_uuid(FileDoc),

    case file_meta:check_name_and_get_conflicting_files(ParentUuid, FileName, FileUuid, ProviderId) of
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
    {ok, StorageId} = space_logic:get_local_supporting_storage(SpaceId),
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


%% @private
-spec calculate_effective_values(file_ctx:ctx()) -> {map(), file_ctx:ctx()}.
calculate_effective_values(FileCtx) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    EffectiveQoSMembership = file_qos:qos_membership(FileDoc),
    {ok, EffectiveDatasetMembership, EffectiveProtectionFlags, FileCtx3} =
        dataset_api:get_effective_membership_and_protection_flags(FileCtx2),
    EffectiveRecallRootGuid = case archive_recall:get_effective_recall(FileDoc) of
        {ok, undefined} -> undefined;
        {ok, Uuid} -> file_id:pack_guid(Uuid, file_ctx:get_space_id_const(FileCtx))
    end,
    {#{
        effective_qos_membership => EffectiveQoSMembership,
        effective_dataset_membership => EffectiveDatasetMembership,
        effective_protection_flags => EffectiveProtectionFlags,
        effective_recall => EffectiveRecallRootGuid
    }, FileCtx3}.
