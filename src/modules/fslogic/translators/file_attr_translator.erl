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
to_json_internal(AttrGeneration, FileAttr, RequestedAttrs) ->
    BaseJson = maps_utils:generate_from_list(fun(AttrName) ->
        {
            onedata_file:attr_name_to_json(AttrGeneration, AttrName),
            utils:undefined_to_null(get_attr_as_json(AttrName, FileAttr))
        }
    end, RequestedAttrs),
    maps:fold(fun(XattrName, XattrValue, Acc) ->
        Acc#{<<"xattr.", XattrName/binary>> => utils:undefined_to_null(XattrValue)}
    end, BaseJson, utils:ensure_defined(FileAttr#file_attr.xattrs, #{})).


%% @private
-spec all_attrs(onedata_file:attr_generation()) -> [onedata_file:attr_name()].
all_attrs(deprecated) -> [?attr_path | ?DEPRECATED_ALL_FILE_ATTRS]; % path is allowed in deprecated recursive listing
all_attrs(current) -> ?ALL_FILE_ATTRS.


%% @private
-spec get_attr_as_json(onedata_file:attr_name(), file_attr:record()) -> undefined | json_utils:json_term().
get_attr_as_json(?attr_guid, #file_attr{guid = Guid}) ->
    file_id:check_guid_to_objectid(Guid);
get_attr_as_json(?attr_index, #file_attr{index = Index}) ->
    file_listing:encode_index(Index);
get_attr_as_json(?attr_type, #file_attr{type = Type}) ->
    onedata_file:type_to_json(Type);
get_attr_as_json(?attr_active_permissions_type, #file_attr{active_permissions_type = ActivePermissionsType}) ->
    atom_to_binary(ActivePermissionsType);
get_attr_as_json(?attr_mode, #file_attr{mode = Mode}) ->
    list_to_binary(string:right(integer_to_list(Mode, 8), 3, $0));
get_attr_as_json(?attr_acl, #file_attr{acl = Acl}) ->
    acl:to_json(Acl, gui);
get_attr_as_json(?attr_name, #file_attr{name = Name}) ->
    Name;
get_attr_as_json(?attr_conflicting_name, #file_attr{conflicting_name = ConflictingName}) ->
    ConflictingName;
get_attr_as_json(?attr_path, #file_attr{path = Path}) ->
    Path;
get_attr_as_json(?attr_parent_guid, #file_attr{parent_guid = ParentGuid}) ->
    utils:convert_defined(map_parent_id(ParentGuid), fun file_id:check_guid_to_objectid/1);
get_attr_as_json(?attr_gid, #file_attr{gid = Gid}) ->
    Gid;
get_attr_as_json(?attr_uid, #file_attr{uid = Uid}) ->
    Uid;
get_attr_as_json(?attr_creation_time, #file_attr{creation_time = CreationTime}) ->
    CreationTime;
get_attr_as_json(?attr_atime, #file_attr{atime = ATime}) ->
    ATime;
get_attr_as_json(?attr_mtime, #file_attr{mtime = MTime}) ->
    MTime;
get_attr_as_json(?attr_ctime, #file_attr{ctime = CTime}) ->
    CTime;
get_attr_as_json(?attr_size, #file_attr{size = Size}) ->
    Size;
get_attr_as_json(?attr_is_fully_replicated, #file_attr{is_fully_replicated = FullyReplicated}) ->
    FullyReplicated;
get_attr_as_json(?attr_local_replication_rate, #file_attr{local_replication_rate = LocalReplicationRate}) ->
    LocalReplicationRate;
get_attr_as_json(?attr_provider_id, #file_attr{provider_id = ProviderId}) ->
    ProviderId;
get_attr_as_json(?attr_shares, #file_attr{shares = Shares}) ->
    Shares;
get_attr_as_json(?attr_owner_id, #file_attr{owner_id = OwnerId}) ->
    OwnerId;
get_attr_as_json(?attr_hardlink_count, #file_attr{hardlink_count = HardlinkCount}) ->
    HardlinkCount;
get_attr_as_json(?attr_symlink_value, #file_attr{symlink_value = SymlinkValue}) ->
    SymlinkValue;
get_attr_as_json(?attr_has_custom_metadata, #file_attr{has_custom_metadata = HasMetadata}) ->
    HasMetadata;
get_attr_as_json(?attr_eff_protection_flags, #file_attr{eff_protection_flags = EffProtectionFlags}) ->
    protection_flags_to_json(EffProtectionFlags);
get_attr_as_json(?attr_eff_dataset_protection_flags, #file_attr{eff_dataset_protection_flags = EffDatasetProtectionFlags}) ->
    protection_flags_to_json(EffDatasetProtectionFlags);
get_attr_as_json(?attr_eff_dataset_inheritance_path, #file_attr{eff_dataset_inheritance_path = EffDatasetInheritancePath}) ->
    inheritance_path_to_json(EffDatasetInheritancePath);
get_attr_as_json(?attr_eff_qos_inheritance_path, #file_attr{eff_qos_inheritance_path = EffQosInheritancePath}) ->
    inheritance_path_to_json(EffQosInheritancePath);
get_attr_as_json(?attr_qos_status, #file_attr{qos_status = QosStatus}) ->
    utils:convert_defined(QosStatus, fun atom_to_binary/1);
get_attr_as_json(?attr_recall_root_id, #file_attr{recall_root_id = RecallRootId}) ->
    RecallRootId.


%% @private
-spec inheritance_path_to_json(file_qos:inheritance_path() | dataset:inheritance_path() | undefined) ->
    json_utils:json_term().
inheritance_path_to_json(undefined)                             -> null; % possible for user root dir
inheritance_path_to_json(?none_inheritance_path)                -> <<"none">>;
inheritance_path_to_json(?direct_inheritance_path)              -> <<"direct">>;
inheritance_path_to_json(?ancestor_inheritance)                 -> <<"ancestor">>;
inheritance_path_to_json(?direct_and_ancestor_inheritance_path) -> <<"directAndAncestor">>.


%% @private
-spec protection_flags_to_json(undefined | data_access_control:bitmask()) -> json_utils:json_term().
protection_flags_to_json(undefined) -> null; % possible for user root dir
protection_flags_to_json(ProtectionFlags) -> file_meta:protection_flags_to_json(ProtectionFlags).


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
