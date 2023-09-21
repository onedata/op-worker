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
    to_json/3
]).

-export([
    attr_name_to_json/2, attr_name_to_json/1,
    attr_name_from_json/2, attr_name_from_json/1
]).

-type attr_type() :: current | deprecated.

-export_type([attr_type/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec to_json(lfm_attrs:file_attributes(), attr_type(), [file_attr:attribute()]) -> json_utils:json_map().
to_json(FileAttrs, AttrType, RequestedAttributes) ->
    select_attrs(to_json(AttrType, FileAttrs), AttrType, RequestedAttributes).


%% @TODO VFS-11377 deprecated, remove when possible
-spec attr_name_from_json(attr_type(), binary()) -> file_attr:attribute().
attr_name_from_json(current, AttrJson) ->
    attr_name_from_json(AttrJson);
attr_name_from_json(deprecated, AttrJson) ->
    attr_name_from_json_deprecated(AttrJson).


%% @TODO VFS-11377 deprecated, remove when possible
-spec attr_name_to_json(attr_type(), file_attr:attribute()) -> binary().
attr_name_to_json(current, Attr) ->
    attr_name_to_json(Attr);
attr_name_to_json(deprecated, Attr) ->
    attr_name_to_json_deprecated(Attr).


-spec attr_name_from_json(binary()) -> file_attr:attribute().
attr_name_from_json(<<"fileId">>)                    -> guid;
attr_name_from_json(<<"name">>)                      -> name;
attr_name_from_json(<<"atime">>)                     -> atime;
attr_name_from_json(<<"mtime">>)                     -> mtime;
attr_name_from_json(<<"ctime">>)                     -> ctime;
attr_name_from_json(<<"type">>)                      -> type;
attr_name_from_json(<<"size">>)                      -> size;
attr_name_from_json(<<"shares">>)                    -> shares;
attr_name_from_json(<<"index">>)                     -> index;
attr_name_from_json(<<"path">>)                      -> path;
attr_name_from_json(<<"conflictingName">>)           -> conflicting_name;
attr_name_from_json(<<"conflictingFiles">>)          -> conflicting_files;
attr_name_from_json(<<"activePermissionsType">>)     -> active_permissions_type;
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
attr_name_from_json(<<"isFullyReplicated">>)         -> is_fully_replicated.


-spec attr_name_to_json(file_attr:attribute()) -> binary().
attr_name_to_json(guid)                         -> <<"fileId">>;
attr_name_to_json(name)                         -> <<"name">>;
attr_name_to_json(atime)                        -> <<"atime">>;
attr_name_to_json(mtime)                        -> <<"mtime">>;
attr_name_to_json(ctime)                        -> <<"ctime">>;
attr_name_to_json(type)                         -> <<"type">>;
attr_name_to_json(size)                         -> <<"size">>;
attr_name_to_json(shares)                       -> <<"shares">>;
attr_name_to_json(index)                        -> <<"index">>;
attr_name_to_json(path)                         -> <<"path">>;
attr_name_to_json(conflicting_name)             -> <<"conflictingName">>;
attr_name_to_json(conflicting_files)            -> <<"conflictingFiles">>;
attr_name_to_json(active_permissions_type)      -> <<"activePermissionsType">>;
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
attr_name_to_json(is_fully_replicated)          -> <<"isFullyReplicated">>.


%% @TODO VFS-11377 deprecated, remove when possible
%% @private
-spec attr_name_from_json_deprecated(binary()) -> file_attr:attribute().
attr_name_from_json_deprecated(<<"file_id">>)            -> guid;
attr_name_from_json_deprecated(<<"name">>)               -> name;
attr_name_from_json_deprecated(<<"atime">>)              -> atime;
attr_name_from_json_deprecated(<<"mtime">>)              -> mtime;
attr_name_from_json_deprecated(<<"ctime">>)              -> ctime;
attr_name_from_json_deprecated(<<"type">>)               -> type;
attr_name_from_json_deprecated(<<"size">>)               -> size;
attr_name_from_json_deprecated(<<"shares">>)             -> shares;
attr_name_from_json_deprecated(<<"index">>)              -> index;
attr_name_from_json_deprecated(<<"storage_user_id">>)    -> uid;
attr_name_from_json_deprecated(<<"storage_group_id">>)   -> gid;
attr_name_from_json_deprecated(<<"owner_id">>)           -> owner_id;
attr_name_from_json_deprecated(<<"parent_id">>)          -> parent_guid;
attr_name_from_json_deprecated(<<"provider_id">>)        -> provider_id;
attr_name_from_json_deprecated(<<"hardlinks_count">>)    -> link_count;
attr_name_from_json_deprecated(<<"mode">>)               -> mode.


%% @TODO VFS-11377 deprecated, remove when possible
%% @private
-spec attr_name_to_json_deprecated(file_attr:attribute()) -> binary().
attr_name_to_json_deprecated(guid)        -> <<"file_id">>;
attr_name_to_json_deprecated(name)        -> <<"name">>;
attr_name_to_json_deprecated(atime)       -> <<"atime">>;
attr_name_to_json_deprecated(mtime)       -> <<"mtime">>;
attr_name_to_json_deprecated(ctime)       -> <<"ctime">>;
attr_name_to_json_deprecated(type)        -> <<"type">>;
attr_name_to_json_deprecated(size)        -> <<"size">>;
attr_name_to_json_deprecated(shares)      -> <<"shares">>;
attr_name_to_json_deprecated(index)       -> <<"index">>;
attr_name_to_json_deprecated(uid)         -> <<"storage_user_id">>;
attr_name_to_json_deprecated(gid)         -> <<"storage_group_id">>;
attr_name_to_json_deprecated(owner_id)    -> <<"owner_id">>;
attr_name_to_json_deprecated(parent_guid) -> <<"parent_id">>;
attr_name_to_json_deprecated(provider_id) -> <<"provider_id">>;
attr_name_to_json_deprecated(link_count)  -> <<"hardlinks_count">>;
attr_name_to_json_deprecated(mode)        -> <<"mode">>.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec to_json(attr_type(), lfm_attrs:file_attributes()) -> json_utils:json_map().
to_json(AttrType, #file_attr{
    guid = Guid,
    name = Name,
    mode = Mode,
    parent_guid = ParentGuid,
    uid = Uid,
    gid = Gid,
    atime = Atime,
    mtime = Mtime,
    ctime = Ctime,
    type = Type,
    size = Size,
    shares = Shares,
    provider_id = ProviderId,
    owner_id = OwnerId,
    link_count = HardlinksCount,
    index = Index,
    xattrs = Xattrs,
    active_permissions_type = ActivePermissionsType,
    symlink_value = SymlinkValue,
    conflicting_name = ConflictingName,
    local_replication_rate = LocalReplicationRate,
    recall_root_id = RecallRootId,
    eff_protection_flags = EffProtectionFlags,
    archive_id = ArchiveId,
    eff_dataset_protection_flags = EffDatasetProtectionFlags,
    eff_dataset_membership = EffDatasetMembership,
    eff_qos_membership = EffQosMembership,
    qos_status = QosStatus,
    is_fully_replicated = FullyReplicated,
    has_metadata = HasMetadata,
    is_deleted = IsDeleted,
    conflicting_files = ConflictingFiles
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
        type => translate_type(Type),
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
        Acc#{attr_name_to_json(AttrType, Key) => utils:undefined_to_null(Value)}
    end, #{}, maps:with(all_attrs(AttrType), BaseMap)),
    maps:fold(fun(XattrName, XattrValue, Acc) ->
        Acc#{<<"xattr.", XattrName/binary>> => utils:undefined_to_null(XattrValue)}
    end, BaseJson, utils:ensure_defined(Xattrs, #{})).


%% @private
-spec all_attrs(attr_type()) -> [file_attr:attribute()].
all_attrs(deprecated) -> ?DEPRECATED_ALL_ATTRS;
all_attrs(current) -> ?ALL_ATTRS.


%% @private
-spec select_attrs(json_utils:json_term(), attr_type(), [file_attr:attribute()]) -> json_utils:json_term().
select_attrs(FileAttrsJson, AttrType, RequestedAttributes) ->
    Xattrs = case file_attr:should_fetch_xattrs(RequestedAttributes) of
        {true, XattrNames} -> XattrNames;
        false -> []
    end,
    MappedRequestedAttributes = lists:map(fun(Attr) ->
        attr_name_to_json(AttrType, Attr)
    end, lists:keydelete(xattrs, 1, RequestedAttributes)),
    maps:with(MappedRequestedAttributes ++ Xattrs, FileAttrsJson).


%% @private
-spec translate_guid(undefined | file_id:file_guid()) -> undefined | file_id:objectid().
translate_guid(undefined) ->
    undefined;
translate_guid(Guid) ->
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),
    ObjectId.


%% @private
-spec translate_type(undefined | file_attr:file_type()) -> undefined | binary().
translate_type(undefined) ->
    undefined;
translate_type(Type) ->
    str_utils:to_binary(Type).


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
