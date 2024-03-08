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

-export([to_json/3]).

%%%===================================================================
%%% API
%%%===================================================================

-spec to_json(lfm_attrs:file_attributes(), onedata_file:attr_generation() | default, [onedata_file:attr_name()]) ->
    json_utils:json_map().
to_json(FileAttrs, default, RequestedAttributes) ->
    maps:merge(
        to_json(FileAttrs, current, RequestedAttributes),
        to_json(FileAttrs, deprecated, RequestedAttributes)
    );
to_json(FileAttrs, AttrGeneration, RequestedAttributes) ->
    to_json_internal(AttrGeneration, FileAttrs, lists_utils:intersect(all_attrs(AttrGeneration), RequestedAttributes)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec to_json_internal(onedata_file:attr_generation(), file_attr:record(), [onedata_file:attr_name()]) ->
    json_utils:json_map().
to_json_internal(AttrGeneration, #file_attr{
    guid = Guid,
    index = Index,
    type = Type,
    active_permissions_type = ActivePermissionsType,
    mode = Mode,
    acl = Acl,
    name = Name,
    conflicting_name = ConflictingName,
    path = Path,
    parent_guid = ParentGuid,
    gid = Gid,
    uid = Uid,
    atime = Atime,
    mtime = Mtime,
    ctime = Ctime,
    size = Size,
    is_fully_replicated = FullyReplicated,
    local_replication_rate = LocalReplicationRate,
    provider_id = ProviderId,
    shares = Shares,
    owner_id = OwnerId,
    hardlink_count = HardlinksCount,
    symlink_value = SymlinkValue,
    has_custom_metadata = HasMetadata,
    eff_protection_flags = EffProtectionFlags,
    eff_dataset_protection_flags = EffDatasetProtectionFlags,
    eff_dataset_inheritance_path = EffDatasetInheritancePath,
    eff_qos_inheritance_path = EffQosInheritancePath,
    qos_status = QosStatus,
    recall_root_id = RecallRootId,
    xattrs = Xattrs
}, RequestedAttrs) ->
    BaseMap = #{
        ?attr_guid => utils:convert_defined(Guid, fun file_id:check_guid_to_objectid/1),
        ?attr_index => file_listing:encode_index(Index),
        ?attr_type => utils:convert_defined(Type, fun onedata_file:type_to_json/1),
        ?attr_active_permissions_type => ActivePermissionsType,
        ?attr_mode => utils:convert_defined(Mode, fun(M) -> list_to_binary(string:right(integer_to_list(M, 8), 3, $0)) end),
        ?attr_acl => utils:convert_defined(Acl, fun(A) -> acl:to_json(A, gui) end),
        ?attr_name => Name,
        ?attr_conflicting_name => ConflictingName,
        ?attr_path => Path,
        ?attr_parent_guid => utils:convert_defined(map_parent_id(ParentGuid), fun file_id:check_guid_to_objectid/1),
        ?attr_gid => Gid,
        ?attr_uid => Uid,
        ?attr_atime => Atime,
        ?attr_mtime => Mtime,
        ?attr_ctime => Ctime,
        ?attr_size => Size,
        ?attr_is_fully_replicated => FullyReplicated,
        ?attr_local_replication_rate => LocalReplicationRate,
        ?attr_provider_id => ProviderId,
        ?attr_shares => Shares,
        ?attr_owner_id => OwnerId,
        ?attr_hardlink_count => HardlinksCount,
        ?attr_symlink_value => SymlinkValue,
        ?attr_has_custom_metadata => HasMetadata,
        ?attr_eff_protection_flags => utils:convert_defined(EffProtectionFlags, fun file_meta:protection_flags_to_json/1),
        ?attr_eff_dataset_protection_flags => utils:convert_defined(EffDatasetProtectionFlags, fun file_meta:protection_flags_to_json/1),
        ?attr_eff_dataset_inheritance_path => utils:convert_defined(EffDatasetInheritancePath, fun inheritance_path_to_json/1),
        ?attr_eff_qos_inheritance_path => utils:convert_defined(EffQosInheritancePath, fun inheritance_path_to_json/1),
        ?attr_qos_status => utils:convert_defined(QosStatus, fun atom_to_binary/1),
        ?attr_recall_root_id => RecallRootId
    },
    BaseJson = maps:fold(fun(Key, Value, Acc) ->
        Acc#{onedata_file:attr_name_to_json(AttrGeneration, Key) => utils:undefined_to_null(Value)}
    end, #{}, maps:with(RequestedAttrs, BaseMap)),
    maps:fold(fun(XattrName, XattrValue, Acc) ->
        Acc#{<<"xattr.", XattrName/binary>> => utils:undefined_to_null(XattrValue)}
    end, BaseJson, utils:ensure_defined(Xattrs, #{})).


%% @private
-spec all_attrs(onedata_file:attr_generation()) -> [onedata_file:attr_name()].
all_attrs(deprecated) -> [?attr_path | ?DEPRECATED_ALL_FILE_ATTRS]; % path is allowed in deprecated recursive listing
all_attrs(current) -> ?ALL_FILE_ATTRS.


%% @private
-spec inheritance_path_to_json(file_qos:inheritance_path() | dataset:inheritance_path()) -> json_utils:json_term().
inheritance_path_to_json(?none_inheritance_path)                -> <<"none">>;
inheritance_path_to_json(?direct_inheritance_path)              -> <<"direct">>;
inheritance_path_to_json(?ancestor_inheritance)                 -> <<"ancestor">>;
inheritance_path_to_json(?direct_and_ancestor_inheritance_path) -> <<"directAndAncestor">>.


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
