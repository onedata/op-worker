%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon, Michal Stanisz
%%% @copyright (C) 2016-2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for calculating file_attrs (see file_attrs.hrl).
%%% @end
%%%--------------------------------------------------------------------
-module(file_attr).
-author("Tomasz Lichon").
-author("Michal Stanisz").

-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    resolve/3, 
    should_fetch_xattrs/1
]).

-type name_conflicts_resolution_policy() ::
    resolve_name_conflicts |
    allow_name_conflicts.

-type optional_attr() :: size | replication_status | link_count | {xattrs, [custom_metadata:name()]}.

-type compute_file_attr_opts() :: #{
    % Tells whether to calculate attr even if file was recently removed.
    allow_deleted_files => boolean(),
    % Tells whether to perform a check if file name collide with other files in
    % directory. If it does suffix will be glued to name to differentiate it
    % and conflicting files will be returned.
    name_conflicts_resolution_policy => name_conflicts_resolution_policy(),
    % Tells which optional attributes are to be calculated.
    include_optional_attrs => [optional_attr()]
}.

-export_type([compute_file_attr_opts/0, optional_attr/0]).


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
%% @doc
%% Resolves attributes of a file. Depending on compute_file_attr_opts() set
%% some attributes may be left undefined (see description of optional attributes).
%% @end
%%--------------------------------------------------------------------
-spec resolve(user_ctx:ctx(), file_ctx:ctx(), compute_file_attr_opts()) ->
    {
        #file_attr{},
        FileDoc :: file_meta:doc(),
        Conflicts :: [{file_meta:uuid(), file_meta:name()}],
        file_ctx:ctx()
    }.
resolve(UserCtx, FileCtx, Opts) ->
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    {_FileUuid, _SpaceId, ShareId} = file_id:unpack_share_guid(FileGuid),

    {FileDoc, FileCtx2} = case maps:get(allow_deleted_files, Opts, false) of
        true -> file_ctx:get_file_doc_including_deleted(FileCtx);
        false -> file_ctx:get_file_doc(FileCtx)
    end,
    EffectiveType = file_meta:get_effective_type(FileDoc),

    {{ATime, CTime, MTime}, FileCtx3} = file_ctx:get_times(FileCtx2),
    {ParentGuid, FileCtx4} = file_tree:get_parent_guid_if_not_root_dir(FileCtx3, UserCtx),

    {Mode, Uid, Gid, OwnerId, ProviderId, Shares, FileCtx5} = case ShareId of
        undefined -> get_private_attrs(UserCtx, FileCtx4, FileDoc);
        _ -> get_masked_private_attrs(ShareId, FileCtx4, FileDoc)
    end,
    OptionalAttrs = maps:get(include_optional_attrs , Opts, []),

    {ReplicationStatus, Size, FileCtx6} = 
        resolve_replication_status_and_size(FileCtx5, FileDoc, EffectiveType, OptionalAttrs),
    
    {FileName, ConflictingFiles, FileCtx7} = resolve_file_name(
        UserCtx, FileDoc, FileCtx6, ParentGuid,
        maps:get(name_conflicts_resolution_policy, Opts, resolve_name_conflicts)
    ),

    % TODO VFS-11033 - remove when stats work for archives
    {FinalReplicationStatus, FinalSize, FileCtx9} = case {ShareId, EffectiveType} of
        {undefined, _} ->
            {ReplicationStatus, Size, FileCtx7};
        {_, ?DIRECTORY_TYPE} ->
            {UuidPath, FileCtx8} = file_ctx:get_uuid_based_path(FileCtx7),
            case lists:any(fun(Uuid) -> archivisation_tree:is_special_uuid(Uuid) end, filename:split(UuidPath)) of
                true -> {undefined, undefined, FileCtx8};
                false -> {ReplicationStatus, Size, FileCtx8}
            end;
        {ReplicationStatus, Size} ->
            {ReplicationStatus, Size, FileCtx7}
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
        size = FinalSize,
        shares = Shares,
        provider_id = ProviderId,
        owner_id = OwnerId,
        fully_replicated = FinalReplicationStatus,
        nlink = resolve_link_count(FileCtx7, ShareId, OptionalAttrs),
        index = resolve_index(FileCtx7, FileDoc),
        xattrs = resolve_xattrs(FileCtx7, OptionalAttrs)
    },
    {FileAttr, FileDoc, ConflictingFiles, FileCtx9}.


-spec should_fetch_xattrs([optional_attr()]) -> {true, [optional_attr()]} | false.
should_fetch_xattrs(OptionalAttrs) ->
    case lists:keyfind(xattrs, 1, OptionalAttrs) of
        {xattrs, XattrNames} -> {true, XattrNames};
        false -> false
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

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
    Scope = file_meta:get_scope(FileDoc),
    {ok, FileUuid} = file_meta:get_uuid(FileDoc),

    case file_meta:check_name_and_get_conflicting_files(ParentUuid, FileName, FileUuid, ProviderId, Scope) of
        {conflicting, ExtendedName, ConflictingFiles} ->
            {ExtendedName, ConflictingFiles, FileCtx1};
        _ ->
            {FileName, [], FileCtx1}
    end;
resolve_file_name(UserCtx, _FileDoc, FileCtx0, _ParentGuid, _NameConflictResolutionPolicy) ->
    {FileName, FileCtx1} = file_ctx:get_aliased_name(FileCtx0, UserCtx),
    {FileName, [], FileCtx1}.


%% @private
-spec resolve_replication_status_and_size(file_ctx:ctx(), file_meta:doc(), file_meta:type(), [optional_attr()]) ->
    {undefined | boolean(), undefined | file_meta:size(), file_ctx:ctx()}.
resolve_replication_status_and_size(FileCtx, FileDoc, Type, OptionalAttrs) ->
    case {Type, lists:member(replication_status, OptionalAttrs), lists:member(size, OptionalAttrs)} of
        {?REGULAR_FILE_TYPE, true, true} ->
            file_ctx:get_replication_status_and_size(FileCtx);
        {?REGULAR_FILE_TYPE, true, _} ->
            {RS, _, Ctx} = file_ctx:get_replication_status_and_size(FileCtx),
            {RS, undefined, Ctx};
        {?DIRECTORY_TYPE, _, true} ->
            case dir_size_stats:get_stats(file_ctx:get_logical_guid_const(FileCtx), [?TOTAL_SIZE]) of
                {ok, #{?TOTAL_SIZE := S}} ->
                    {undefined, S, FileCtx};
                ?ERROR_NOT_FOUND ->
                    {undefined, 0, FileCtx};
                ?ERROR_DIR_STATS_DISABLED_FOR_SPACE ->
                    {undefined, undefined, FileCtx};
                ?ERROR_DIR_STATS_NOT_READY ->
                    {undefined, undefined, FileCtx}
            end;
        {?SYMLINK_TYPE, _, true} ->
            {ok, Symlink} = file_meta_symlinks:readlink(FileDoc),
            {undefined, byte_size(Symlink), FileCtx};
        {_, _, true} ->
            {S, Ctx} = file_ctx:get_file_size(FileCtx),
            {undefined, S, Ctx};
        _ ->
            {undefined, undefined, FileCtx}
    end.


%% @private
-spec resolve_link_count(file_ctx:ctx(), od_share:id(), [optional_attr()]) -> 
    non_neg_integer() | undefined.
resolve_link_count(FileCtx, ShareId, OptionalAttrs) ->
    case {ShareId, lists:member(link_count, OptionalAttrs)} of
        {undefined, true} -> 
            {ok, LinkCount} = file_ctx:count_references_const(FileCtx),
            LinkCount;
        _ -> 
            undefined
    end.


%% @private
-spec resolve_index(file_ctx:ctx(), file_meta:doc()) -> file_listing:index().
resolve_index(FileCtx, FileDoc) ->
    case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            % As provider id in space doc is random (depends on which provider called `file_meta:make_space_exist/0`) 
            % use only space id in index (there are no conflicts on spaces between providers, so it is not a problem).
            file_listing:build_index(file_meta:get_name(FileDoc));
        false ->
            file_listing:build_index(
                file_meta:get_name(FileDoc), file_meta:get_provider_id(FileDoc))
    end.


%% @private
-spec resolve_xattrs(file_ctx:ctx(), [optional_attr()]) ->
    #{custom_metadata:name() => custom_metadata:value()}.
resolve_xattrs(FileCtx, OptionalAttrs) ->
    case should_fetch_xattrs(OptionalAttrs) of
        {true, XattrNames} ->
            {ok, AllDirectXattrs} = xattr:get_all_direct_insecure(FileCtx),
            lists:foldl(fun(Xattr, Acc) ->
                Acc#{Xattr => maps:get(Xattr, AllDirectXattrs, undefined)}
            end, #{}, XattrNames);
        false -> 
            #{}
    end.
