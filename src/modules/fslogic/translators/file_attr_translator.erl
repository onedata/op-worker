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
    attr_name_from_json/2, attr_name_from_json/1,
    sanitize_requested_attrs/3
]).

-type attr_type() :: current | deprecated.

-export_type([attr_type/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec to_json(lfm_attrs:file_attributes(), attr_type() | default, [file_attr:attribute()]) -> json_utils:json_map().
to_json(FileAttrs, default, RequestedAttributes) ->
    maps:merge(
        to_json(FileAttrs, current, RequestedAttributes),
        to_json(FileAttrs, deprecated, RequestedAttributes)
    );
to_json(FileAttrs, AttrType, RequestedAttributes) ->
    to_json_internal(AttrType, FileAttrs, lists_utils:intersect(all_attrs(AttrType), RequestedAttributes)).


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
attr_name_from_json(<<"fileId">>)                    -> ?attr_guid;
attr_name_from_json(<<"index">>)                     -> ?attr_index;
attr_name_from_json(<<"type">>)                      -> ?attr_type;
attr_name_from_json(<<"activePermissionsType">>)     -> ?attr_active_permissions_type;
attr_name_from_json(<<"posixPermissions">>)          -> ?attr_mode;
attr_name_from_json(<<"acl">>)                       -> ?attr_acl;
attr_name_from_json(<<"name">>)                      -> ?attr_name;
attr_name_from_json(<<"conflictingName">>)           -> ?attr_conflicting_name;
attr_name_from_json(<<"path">>)                      -> ?attr_path;
attr_name_from_json(<<"parentFileId">>)              -> ?attr_parent_guid;
attr_name_from_json(<<"displayGid">>)                -> ?attr_gid;
attr_name_from_json(<<"displayUid">>)                -> ?attr_uid;
attr_name_from_json(<<"atime">>)                     -> ?attr_atime;
attr_name_from_json(<<"mtime">>)                     -> ?attr_mtime;
attr_name_from_json(<<"ctime">>)                     -> ?attr_ctime;
attr_name_from_json(<<"size">>)                      -> ?attr_size;
attr_name_from_json(<<"isFullyReplicatedLocally">>)  -> ?attr_is_fully_replicated;
attr_name_from_json(<<"localReplicationRate">>)      -> ?attr_local_replication_rate;
attr_name_from_json(<<"originProviderId">>)          -> ?attr_provider_id;
attr_name_from_json(<<"directShareIds">>)            -> ?attr_shares;
attr_name_from_json(<<"ownerUserId">>)               -> ?attr_owner_id;
attr_name_from_json(<<"hardlinkCount">>)             -> ?attr_hardlink_count;
attr_name_from_json(<<"symlinkValue">>)              -> ?attr_symlink_value;
attr_name_from_json(<<"hasCustomMetadata">>)         -> ?attr_has_custom_metadata;
attr_name_from_json(<<"effProtectionFlags">>)        -> ?attr_eff_protection_flags;
attr_name_from_json(<<"effDatasetProtectionFlags">>) -> ?attr_eff_dataset_protection_flags;
attr_name_from_json(<<"effDatasetInheritancePath">>) -> ?attr_eff_dataset_inheritance_path;
attr_name_from_json(<<"effQosInheritancePath">>)     -> ?attr_eff_qos_inheritance_path;
attr_name_from_json(<<"aggregateQosStatus">>)        -> ?attr_qos_status;
attr_name_from_json(<<"archiveRecallRootFileId">>)   -> ?attr_recall_root_id.


-spec attr_name_to_json(file_attr:attribute()) -> binary().
attr_name_to_json(?attr_guid)                         -> <<"fileId">>;
attr_name_to_json(?attr_index)                        -> <<"index">>;
attr_name_to_json(?attr_type)                         -> <<"type">>;
attr_name_to_json(?attr_active_permissions_type)      -> <<"activePermissionsType">>;
attr_name_to_json(?attr_mode)                         -> <<"posixPermissions">>;
attr_name_to_json(?attr_acl)                          -> <<"acl">>;
attr_name_to_json(?attr_name)                         -> <<"name">>;
attr_name_to_json(?attr_conflicting_name)             -> <<"conflictingName">>;
attr_name_to_json(?attr_path)                         -> <<"path">>;
attr_name_to_json(?attr_parent_guid)                  -> <<"parentFileId">>;
attr_name_to_json(?attr_gid)                          -> <<"displayGid">>;
attr_name_to_json(?attr_uid)                          -> <<"displayUid">>;
attr_name_to_json(?attr_atime)                        -> <<"atime">>;
attr_name_to_json(?attr_mtime)                        -> <<"mtime">>;
attr_name_to_json(?attr_ctime)                        -> <<"ctime">>;
attr_name_to_json(?attr_size)                         -> <<"size">>;
attr_name_to_json(?attr_is_fully_replicated)          -> <<"isFullyReplicatedLocally">>;
attr_name_to_json(?attr_local_replication_rate)       -> <<"localReplicationRate">>;
attr_name_to_json(?attr_provider_id)                  -> <<"originProviderId">>;
attr_name_to_json(?attr_shares)                       -> <<"directShareIds">>;
attr_name_to_json(?attr_owner_id)                     -> <<"ownerUserId">>;
attr_name_to_json(?attr_hardlink_count)               -> <<"hardlinkCount">>;
attr_name_to_json(?attr_symlink_value)                -> <<"symlinkValue">>;
attr_name_to_json(?attr_has_custom_metadata)          -> <<"hasCustomMetadata">>;
attr_name_to_json(?attr_eff_protection_flags)         -> <<"effProtectionFlags">>;
attr_name_to_json(?attr_eff_dataset_protection_flags) -> <<"effDatasetProtectionFlags">>;
attr_name_to_json(?attr_eff_dataset_inheritance_path) -> <<"effDatasetInheritancePath">>;
attr_name_to_json(?attr_eff_qos_inheritance_path)     -> <<"effQosInheritancePath">>;
attr_name_to_json(?attr_qos_status)                   -> <<"aggregateQosStatus">>;
attr_name_to_json(?attr_recall_root_id)               -> <<"archiveRecallRootFileId">>.


%% @TODO VFS-11377 deprecated, remove when possible
%% @private
-spec attr_name_from_json_deprecated(binary()) -> file_attr:attribute().
attr_name_from_json_deprecated(<<"file_id">>)             -> ?attr_guid;
attr_name_from_json_deprecated(<<"path">>)                -> ?attr_path;
attr_name_from_json_deprecated(<<"name">>)                -> ?attr_name;
attr_name_from_json_deprecated(<<"atime">>)               -> ?attr_atime;
attr_name_from_json_deprecated(<<"mtime">>)               -> ?attr_mtime;
attr_name_from_json_deprecated(<<"ctime">>)               -> ?attr_ctime;
attr_name_from_json_deprecated(<<"type">>)                -> ?attr_type;
attr_name_from_json_deprecated(<<"size">>)                -> ?attr_size;
attr_name_from_json_deprecated(<<"shares">>)              -> ?attr_shares;
attr_name_from_json_deprecated(<<"index">>)               -> ?attr_index;
attr_name_from_json_deprecated(<<"storage_user_id">>)     -> ?attr_uid;
attr_name_from_json_deprecated(<<"storage_group_id">>)    -> ?attr_gid;
attr_name_from_json_deprecated(<<"owner_id">>)            -> ?attr_owner_id;
attr_name_from_json_deprecated(<<"parent_id">>)           -> ?attr_parent_guid;
attr_name_from_json_deprecated(<<"provider_id">>)         -> ?attr_provider_id;
attr_name_from_json_deprecated(<<"hardlinks_count">>)     -> ?attr_hardlink_count;
attr_name_from_json_deprecated(<<"is_fully_replicated">>) -> ?attr_is_fully_replicated;
attr_name_from_json_deprecated(<<"mode">>)                -> ?attr_mode.


%% @TODO VFS-11377 deprecated, remove when possible
%% @private
-spec attr_name_to_json_deprecated(file_attr:attribute()) -> binary().
attr_name_to_json_deprecated(?attr_guid)                -> <<"file_id">>;
attr_name_to_json_deprecated(?attr_path)                -> <<"path">>;
attr_name_to_json_deprecated(?attr_name)                -> <<"name">>;
attr_name_to_json_deprecated(?attr_atime)               -> <<"atime">>;
attr_name_to_json_deprecated(?attr_mtime)               -> <<"mtime">>;
attr_name_to_json_deprecated(?attr_ctime)               -> <<"ctime">>;
attr_name_to_json_deprecated(?attr_type)                -> <<"type">>;
attr_name_to_json_deprecated(?attr_size)                -> <<"size">>;
attr_name_to_json_deprecated(?attr_shares)              -> <<"shares">>;
attr_name_to_json_deprecated(?attr_index)               -> <<"index">>;
attr_name_to_json_deprecated(?attr_uid)                 -> <<"storage_user_id">>;
attr_name_to_json_deprecated(?attr_gid)                 -> <<"storage_group_id">>;
attr_name_to_json_deprecated(?attr_owner_id)            -> <<"owner_id">>;
attr_name_to_json_deprecated(?attr_parent_guid)         -> <<"parent_id">>;
attr_name_to_json_deprecated(?attr_provider_id)         -> <<"provider_id">>;
attr_name_to_json_deprecated(?attr_hardlink_count)      -> <<"hardlinks_count">>;
attr_name_to_json_deprecated(?attr_is_fully_replicated) -> <<"is_fully_replicated">>;
attr_name_to_json_deprecated(?attr_mode)                -> <<"mode">>.


-spec sanitize_requested_attrs([binary()], attr_type(), [file_attr:attribute()]) ->
    {ok, [file_attr:attribute()]} | {error, [binary()]}.
sanitize_requested_attrs(Attributes, AttrType, AllowedAttributes) ->
    Result = lists_utils:foldl_while(fun
        (<<"xattr.", XattrName/binary>>, {ok, AttrAcc, XattrAcc}) ->
            {cont, {ok, AttrAcc, [XattrName | XattrAcc]}};
        (Attr, {ok, AttrAcc, XattrAcc}) ->
            try
                TranslatedAttr = attr_name_from_json(AttrType, Attr),
                true = lists:member(TranslatedAttr, AllowedAttributes),
                {cont, {ok, [TranslatedAttr | AttrAcc], XattrAcc}}
            catch _:_ ->
                AllowedValuesJson = [attr_name_to_json(AttrType, A) || A <- AllowedAttributes],
                % add xattr.* to end of list, so allowed values are printed in correct order
                {halt, {error, AllowedValuesJson ++ [<<"xattr.*">>]}}
            end
    end, {ok, [], []}, utils:ensure_list(Attributes)),
    case Result of
        {ok, TranslatedAttrs, []} -> {ok, TranslatedAttrs};
        {ok, TranslatedAttrs, Xattrs} -> {ok, [?attr_xattrs(Xattrs) | TranslatedAttrs]};
        {error, AllowedValuesJson} -> {error, AllowedValuesJson}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec to_json_internal(attr_type(), file_attr:record(), [file_attr:attribute()]) -> json_utils:json_map().
to_json_internal(AttrType, #file_attr{
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
        ?attr_guid => translate_guid(Guid),
        ?attr_index => file_listing:encode_index(Index),
        ?attr_type => translate_type(Type),
        ?attr_active_permissions_type => ActivePermissionsType,
        ?attr_mode => translate_mode(Mode),
        ?attr_acl => translate_acl(Acl),
        ?attr_name => Name,
        ?attr_conflicting_name => ConflictingName,
        ?attr_path => Path,
        ?attr_parent_guid => translate_guid(map_parent_id(ParentGuid)),
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
        ?attr_eff_protection_flags => translate_protection_flags(EffProtectionFlags),
        ?attr_eff_dataset_protection_flags => translate_protection_flags(EffDatasetProtectionFlags),
        ?attr_eff_dataset_inheritance_path => translate_membership(EffDatasetInheritancePath),
        ?attr_eff_qos_inheritance_path => translate_membership(EffQosInheritancePath),
        ?attr_qos_status => translate_qos_status(QosStatus),
        ?attr_recall_root_id => RecallRootId
    },
    BaseJson = maps:fold(fun(Key, Value, Acc) ->
        Acc#{attr_name_to_json(AttrType, Key) => utils:undefined_to_null(Value)}
    end, #{}, maps:with(RequestedAttrs, BaseMap)),
    maps:fold(fun(XattrName, XattrValue, Acc) ->
        Acc#{<<"xattr.", XattrName/binary>> => utils:undefined_to_null(XattrValue)}
    end, BaseJson, utils:ensure_defined(Xattrs, #{})).


%% @private
-spec all_attrs(attr_type()) -> [file_attr:attribute()].
all_attrs(deprecated) -> [?attr_path | ?DEPRECATED_ALL_ATTRS]; % path is allowed in deprecated recursive listing
all_attrs(current) -> ?ALL_ATTRS.


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
-spec translate_acl(undefined | acl:acl()) -> undefined | json_utils:json_term().
translate_acl(undefined) -> undefined;
translate_acl(Acl) -> acl:to_json(Acl, gui).


%% @private
-spec translate_protection_flags(undefined | file_qos:inheritance_path() | dataset:inheritance_path()) -> undefined | [binary()].
translate_protection_flags(undefined) -> undefined;
translate_protection_flags(ProtectionFlags) -> file_meta:protection_flags_to_json(ProtectionFlags).


%% @private
-spec translate_membership(undefined | file_qos:inheritance_path() | dataset:inheritance_path()) -> undefined | binary().
translate_membership(?none_inheritance_path)                -> <<"none">>;
translate_membership(?direct_inheritance_path)              -> <<"direct">>;
translate_membership(?ancestor_inheritance)                 -> <<"ancestor">>;
translate_membership(?direct_and_ancestor_inheritance_path) -> <<"directAndAncestor">>;
translate_membership(undefined)                             -> undefined.


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
