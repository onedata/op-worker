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
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    get_file_attr/2, get_file_attr_insecure/3,
    get_file_attr_and_conflicts_insecure/3,

    get_file_details/2, get_file_details_insecure/3,

    get_child_attr/3, chmod/3, update_times/5,
    chmod_attrs_only_insecure/2
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
            has_metadata = has_metadata(FileCtx2)
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
    {FileUuid, _SpaceId, ShareId} = file_id:unpack_share_guid(FileGuid),

    {#document{
        key = FileUuid,
        value = #file_meta{
            type = Type,
            mode = Mode,
            provider_id = ProviderId,
            owner = OwnerId,
            shares = Shares
        }
    } = FileDoc, FileCtx2} = case maps:get(allow_deleted_files, Opts, false) of
        true ->
            file_ctx:get_file_doc_including_deleted(FileCtx);
        false ->
            file_ctx:get_file_doc(FileCtx)
    end,

    {{Uid, Gid}, FileCtx3} = file_ctx:get_display_owner(FileCtx2),

    {Size, FileCtx4} = case maps:get(include_size, Opts, true) of
        true -> file_ctx:get_file_size(FileCtx3);
        _ -> {undefined, FileCtx3}
    end,

    {{ATime, CTime, MTime}, FileCtx5} = file_ctx:get_times(FileCtx4),
    {ParentGuid, FileCtx6} = file_ctx:get_parent_guid(FileCtx5, UserCtx),

    ResolveNameConflicts = case maps:get(name_conflicts_resolution_policy, Opts, resolve_name_conflicts) of
        resolve_name_conflicts -> true;
        allow_name_conflicts -> false
    end,

    {FileName, FileCtx7} = file_ctx:get_aliased_name(FileCtx6, UserCtx),
    {FinalName, ConflictingFiles} = case ResolveNameConflicts andalso ParentGuid =/= undefined of
        true ->
            case file_meta:check_name(file_id:guid_to_uuid(ParentGuid), FileName, FileDoc) of
                {conflicting, ExtendedName, Others} ->
                    {ExtendedName, Others};
                _ ->
                    {FileName, []}
            end;
        _ ->
            {FileName, []}
    end,

    FileAttr = #file_attr{
        guid = FileGuid,
        name = FinalName,
        mode = Mode,
        parent_uuid = ParentGuid,
        uid = Uid,                     % TODO VFS-6095
        gid = Gid,                     % TODO VFS-6095
        atime = ATime,
        mtime = MTime,
        ctime = CTime,
        type = Type,
        size = Size,
        shares = filter_visible_shares(ShareId, Shares),
        provider_id = ProviderId,
        owner_id = OwnerId             % TODO VFS-6095
    },
    {FileAttr, FileDoc, ConflictingFiles, FileCtx7}.


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
