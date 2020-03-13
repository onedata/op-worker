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

%% API
-export([
    get_file_attr/2, get_file_attr_insecure/2, get_file_attr_insecure/3,
    get_file_attr_insecure/4, get_file_attr_light/3, get_file_attr_and_conflicts/5,
    get_file_details/2, get_file_details_light/2,
    get_child_attr/3, chmod/3, update_times/5,
    chmod_attrs_only_insecure/2
]).

-type compute_file_attr_opts() :: #{
    % Tells whether to calculate attr even if file was recently removed.
    allow_deleted_files => boolean(),

    % Tells whether to calculate size of file.
    include_size => boolean(),

    % Tells whether to perform a check if file name collide with other files in
    % directory. If it does suffix will be glued to name to differentiate it
    % and conflicting files will be returned.
    verify_name => boolean()
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
    {Ans, _, _} = get_file_attr_and_conflicts(UserCtx, FileCtx, AllowDeletedFiles, IncludeSize, true),
    Ans.

%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes.
%% Internal function - no permissions check, no name verification, no deleted files.
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr_light(user_ctx:ctx(), file_ctx:ctx(),
    IncludeSize :: boolean()) -> fslogic_worker:fuse_response().
get_file_attr_light(UserCtx, FileCtx, IncludeSize) ->
    {Ans, _, _} = get_file_attr_and_conflicts(UserCtx, FileCtx, false, IncludeSize, false),
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
    {fslogic_worker:fuse_response(), Conflicts :: [{file_meta:uuid(), file_meta:name()}], IsDeleted :: boolean()}.
get_file_attr_and_conflicts(UserCtx, FileCtx, AllowDeletedFiles, IncludeSize, VerifyName) ->
    {FileAttr, ConflictingFiles, FileCtx2} = compute_file_attr(
        UserCtx, FileCtx, #{
            allow_deleted_files => AllowDeletedFiles,
            include_size => IncludeSize,
            verify_name => VerifyName
        }
    ),
    {FileDoc, _FileCtx3} = file_ctx:get_file_doc(FileCtx2),
    FuseResponse = #fuse_response{
        status = #status{code = ?OK},
        fuse_response = FileAttr
    },
    {FuseResponse, ConflictingFiles, file_meta:is_deleted(FileDoc)}.


%%--------------------------------------------------------------------
%% @equiv get_file_details_insecure/2 with permission checks
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
        verify_name => true
    }).


%%--------------------------------------------------------------------
%% @equiv get_file_details_insecure/2 with permission checks.
%% For internal use only - no name verification.
%% @end
%%--------------------------------------------------------------------
-spec get_file_details_light(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_file_details_light(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0, [traverse_ancestors], allow_ancestors
    ),
    get_file_details_insecure(UserCtx, FileCtx1, #{
        allow_deleted_files => false,
        include_size => true,
        verify_name => false
    }).


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
%% Returns file attributes and additional information like active permissions
%% type or existence of metadata.
%% @end
%%--------------------------------------------------------------------
-spec get_file_details_insecure(user_ctx:ctx(), file_ctx:ctx(), compute_file_attr_opts()) ->
    fslogic_worker:fuse_response().
get_file_details_insecure(UserCtx, FileCtx, Opts) ->
    {FileAttr, _, FileCtx2} = compute_file_attr(UserCtx, FileCtx, Opts),
    {FileDoc, FileCtx3} = file_ctx:get_file_doc(FileCtx2),
    {ok, ActivePermissionsType} = file_meta:get_active_perms_type(FileDoc),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_details{
            file_attr = FileAttr,
            index_startid = file_meta:get_name(FileDoc),
            active_permissions_type = ActivePermissionsType,
            has_metadata = has_metadata(FileCtx3)
        }
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Computes attributes of a file. Depending on compute_file_attr_opts() set
%% some attributes may be left undefined (see description of individual
%% options).
%% @end
%%--------------------------------------------------------------------
-spec compute_file_attr(user_ctx:ctx(), file_ctx:ctx(), compute_file_attr_opts()) ->
    {#file_attr{}, Conflicts :: [{file_meta:uuid(), file_meta:name()}], file_ctx:ctx()}.
compute_file_attr(UserCtx, FileCtx, Opts) ->
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

    {{Uid, Gid}, FileCtx3} = file_ctx:get_posix_storage_user_context(FileCtx2, UserCtx),

    {Size, FileCtx4} = case maps:get(include_size, Opts, true) of
        true -> file_ctx:get_file_size(FileCtx3);
        _ -> {undefined, FileCtx3}
    end,

    {{ATime, CTime, MTime}, FileCtx5} = file_ctx:get_times(FileCtx4),
    {ParentGuid, FileCtx6} = file_ctx:get_parent_guid(FileCtx5, UserCtx),

    VerifyName = maps:get(verify_name, Opts, true),

    {FileName, FileCtx7} = file_ctx:get_aliased_name(FileCtx6, UserCtx),
    {FinalName, ConflictingFiles} = case VerifyName andalso ParentGuid =/= undefined of
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
    {FileAttr, ConflictingFiles, FileCtx7}.


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
