%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module providing translation functions for file attr.
%%% @end
%%%--------------------------------------------------------------------
-module(file_attr_translator).
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("modules/fslogic/file_attr.hrl").

-export([
    to_json/1, to_json/2,
    select_attrs/2
]).

-export([attr_name_to_json/1, attr_name_from_json/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec to_json(lfm_attrs:file_attributes()) -> json_utils:json_map().
to_json(#file_attr{
    guid = Guid, name = Name, mode = Mode, parent_guid = ParentGuid, uid = Uid, gid = Gid, atime = Atime,
    mtime = Mtime, ctime = Ctime, type = Type, size = Size, shares = Shares, provider_id = ProviderId,
    owner_id = OwnerId, link_count = HardlinksCount, index = Index, xattrs = Xattrs,
    active_permissions_type = ActivePermissionsType, symlink_value = SymlinkValue,
    conflicting_name = ConflictingName, local_replication_rate = LocalReplicationRate,
    recall_root_id = RecallRootId, eff_protection_flags = EffProtectionFlags, archive_id = ArchiveId,
    eff_dataset_protection_flags = EffDatasetProtectionFlags, eff_dataset_membership = EffDatasetMembership,
    eff_qos_membership = EffQosMembership, qos_status = QosStatus, is_fully_replicated = FullyReplicated,
    has_metadata = HasMetadata, is_deleted = IsDeleted, conflicting_files = ConflictingFiles
}) ->
    BaseMap = #{
        guid => translate_guid(Guid),
        name => Name,
        mode => translate_mode(Mode),
        parent_guid => translate_guid(map_parent_id(ParentGuid)),
        uid => Uid,
        gid => Gid,
        atime => Atime,
        mtime => Mtime,
        ctime => Ctime,
        type => str_utils:to_binary(Type),
        size => Size,
        shares => Shares,
        provider_id => ProviderId,
        owner_id => OwnerId,
        link_count => HardlinksCount,
        index => file_listing:encode_index(Index),
        has_metadata => HasMetadata,
        active_permissions_type => ActivePermissionsType,
        local_replication_rate => LocalReplicationRate,
        is_fully_replicated => FullyReplicated,
        qos_status => translate_qos_status(QosStatus),
        eff_protection_flags => translate_protection_flags(EffProtectionFlags),
        eff_dataset_protection_flags => translate_protection_flags(EffDatasetProtectionFlags),
        eff_qos_membership => translate_membership(EffQosMembership),
        eff_dataset_membership => translate_membership(EffDatasetMembership),
        recall_root_id => RecallRootId,
        symlink_value => SymlinkValue,
        conflicting_name => ConflictingName,
        is_deleted => IsDeleted,
        conflicting_files => ConflictingFiles,
        archive_id => ArchiveId
    },
    BaseJson = maps:fold(fun(Key, Value, Acc) ->
        Acc#{attr_name_to_json(Key) => utils:undefined_to_null(Value)}
    end, #{}, BaseMap),
    maps:fold(fun(XattrName, XattrValue, Acc) ->
        Acc#{<<"xattr.", XattrName/binary>> => utils:undefined_to_null(XattrValue)}
    end, BaseJson, utils:ensure_defined(Xattrs, #{})).


-spec to_json(lfm_attrs:file_attributes(), [binary()]) -> json_utils:json_map().
to_json(FileAttrs, RequestedAttributes) ->
    select_attrs(to_json(FileAttrs), RequestedAttributes).


-spec select_attrs(json_utils:json_term(), [file_attr:attribute()]) -> json_utils:json_term().
select_attrs(FileAttrsJson, RequestedAttributes) ->
    Xattrs = case file_attr:should_fetch_xattrs(RequestedAttributes) of
        {true, XattrNames} -> XattrNames;
        false -> []
    end,
    MappedRequestedAttributes = lists:map(fun attr_name_to_json/1, lists:keydelete(xattrs, 1, RequestedAttributes)),
    maps:with(MappedRequestedAttributes ++ Xattrs, FileAttrsJson).


-spec attr_name_from_json(binary()) -> file_attr:attribute().
attr_name_from_json(<<"conflictingName">>)           -> conflicting_name;
attr_name_from_json(<<"conflictingFiles">>)          -> conflicting_files;
attr_name_from_json(<<"activePermissionsType">>)     -> active_permissions_type;
attr_name_from_json(<<"fileId">>)                    -> guid;
attr_name_from_json(<<"storageUserId">>)             -> uid;
attr_name_from_json(<<"storageGroupId">>)            -> gid;
attr_name_from_json(<<"ownerId">>)                   -> owner_id;
attr_name_from_json(<<"parentId">>)                  -> parent_guid;
attr_name_from_json(<<"providerId">>)                -> provider_id;
attr_name_from_json(<<"symlinkValue">>)              -> symlink_value;
attr_name_from_json(<<"hardlinkCount">>)             -> link_count;
attr_name_from_json(<<"localReplicationRate">>)      -> local_replication_rate;
attr_name_from_json(<<"recallRootId">>)              -> recall_root_id;
attr_name_from_json(<<"archiveId">>)                 -> archive_id;
attr_name_from_json(<<"effDatasetMembership">>)      -> eff_dataset_membership;
attr_name_from_json(<<"effDatasetProtectionFlags">>) -> eff_dataset_protection_flags;
attr_name_from_json(<<"effProtectionFlags">>)        -> eff_protection_flags;
attr_name_from_json(<<"effQosMembership">>)          -> eff_qos_membership;
attr_name_from_json(<<"qosStatus">>)                 -> qos_status;
attr_name_from_json(<<"hasMetadata">>)               -> has_metadata;
attr_name_from_json(<<"posixPermissions">>)          -> mode;
attr_name_from_json(<<"isDeleted">>)                 -> is_deleted;
attr_name_from_json(<<"isFullyReplicated">>)         -> is_fully_replicated;
attr_name_from_json(Other)                           -> binary_to_existing_atom(Other).


-spec attr_name_to_json(file_attr:attribute()) -> binary().
attr_name_to_json(conflicting_name)             -> <<"conflictingName">>;
attr_name_to_json(conflicting_files)            -> <<"conflictingFiles">>;
attr_name_to_json(active_permissions_type)      -> <<"activePermissionsType">>;
attr_name_to_json(guid)                         -> <<"fileId">>;
attr_name_to_json(uid)                          -> <<"storageUserId">>;
attr_name_to_json(gid)                          -> <<"storageGroupId">>;
attr_name_to_json(owner_id)                     -> <<"ownerId">>;
attr_name_to_json(parent_guid)                  -> <<"parentId">>;
attr_name_to_json(provider_id)                  -> <<"providerId">>;
attr_name_to_json(symlink_value)                -> <<"symlinkValue">>;
attr_name_to_json(link_count)                   -> <<"hardlinkCount">>;
attr_name_to_json(local_replication_rate)       -> <<"localReplicationRate">>;
attr_name_to_json(recall_root_id)               -> <<"recallRootId">>;
attr_name_to_json(archive_id)                   -> <<"archiveId">>;
attr_name_to_json(eff_dataset_membership)       -> <<"effDatasetMembership">>;
attr_name_to_json(eff_dataset_protection_flags) -> <<"effDatasetProtectionFlags">>;
attr_name_to_json(eff_protection_flags)         -> <<"effProtectionFlags">>;
attr_name_to_json(eff_qos_membership)           -> <<"effQosMembership">>;
attr_name_to_json(qos_status)                   -> <<"qosStatus">>;
attr_name_to_json(has_metadata)                 -> <<"hasMetadata">>;
attr_name_to_json(mode)                         -> <<"posixPermissions">>;
attr_name_to_json(is_deleted)                   -> <<"isDeleted">>;
attr_name_to_json(is_fully_replicated)          -> <<"isFullyReplicated">>;
attr_name_to_json(Other)                        -> atom_to_binary(Other).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private -spec translate_guid(undefined | file_id:file_guid()) -> undefined | file_id:objectid().
translate_guid(undefined) ->
    undefined;
translate_guid(Guid) ->
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),
    ObjectId.


%% @private
-spec translate_mode(undefined | file_meta:mode()) -> undefined | binary().
translate_mode(undefined) -> undefined;
translate_mode(Mode) -> list_to_binary(string:right(integer_to_list(Mode, 8), 3, $0)).


%% @private
-spec translate_protection_flags(undefined | file_qos:membership() | dataset:membership()) -> undefined | [binary()].
translate_protection_flags(undefined) -> undefined;
translate_protection_flags(ProtectionFlags) -> file_meta:protection_flags_to_json(ProtectionFlags).


%% @private
-spec translate_membership(undefined | file_qos:membership() | dataset:membership()) -> undefined | binary().
translate_membership(?NONE_MEMBERSHIP)                -> <<"none">>;
translate_membership(?DIRECT_MEMBERSHIP)              -> <<"direct">>;
translate_membership(?ANCESTOR_MEMBERSHIP)            -> <<"ancestor">>;
translate_membership(?DIRECT_AND_ANCESTOR_MEMBERSHIP) -> <<"directAndAncestor">>;
translate_membership(undefined)                       -> undefined.


%% @private
-spec translate_qos_status(undefined | qos_status:summary()) -> undefined | binary().
translate_qos_status(undefined) -> undefined;
translate_qos_status(Status) ->    atom_to_binary(Status).


%% @private
-spec map_parent_id(file_id:file_guid() | undefined) -> undefined | file_id:file_guid().
map_parent_id(undefined) ->
    undefined;
map_parent_id(ParentGuid) ->
    % filter out user root dirs
    case fslogic_file_id:is_user_root_dir_uuid(file_id:guid_to_uuid(ParentGuid)) of
        true -> undefined;
        false -> ParentGuid
    end.
