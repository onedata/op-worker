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
attr_name_from_json(<<"fileId">>)                    -> guid;
attr_name_from_json(<<"index">>)                     -> index;
attr_name_from_json(<<"type">>)                      -> type;
attr_name_from_json(<<"activePermissionsType">>)     -> active_permissions_type;
attr_name_from_json(<<"posixPermissions">>)          -> mode;
attr_name_from_json(<<"acl">>)                       -> acl;
attr_name_from_json(<<"name">>)                      -> name;
attr_name_from_json(<<"conflictingName">>)           -> conflicting_name;
attr_name_from_json(<<"path">>)                      -> path;
attr_name_from_json(<<"parentFileId">>)              -> parent_guid;
attr_name_from_json(<<"displayGid">>)                -> gid;
attr_name_from_json(<<"displayUid">>)                -> uid;
attr_name_from_json(<<"atime">>)                     -> atime;
attr_name_from_json(<<"mtime">>)                     -> mtime;
attr_name_from_json(<<"ctime">>)                     -> ctime;
attr_name_from_json(<<"size">>)                      -> size;
attr_name_from_json(<<"isFullyReplicatedLocally">>)  -> is_fully_replicated;
attr_name_from_json(<<"localReplicationRate">>)      -> local_replication_rate;
attr_name_from_json(<<"originProviderId">>)          -> provider_id;
attr_name_from_json(<<"directShareIds">>)            -> shares;
attr_name_from_json(<<"ownerUserId">>)               -> owner_id;
attr_name_from_json(<<"hardlinkCount">>)             -> hardlink_count;
attr_name_from_json(<<"symlinkValue">>)              -> symlink_value;
attr_name_from_json(<<"hasCustomMetadata">>)         -> has_custom_metadata;
attr_name_from_json(<<"effProtectionFlags">>)        -> eff_protection_flags;
attr_name_from_json(<<"effDatasetProtectionFlags">>) -> eff_dataset_protection_flags;
attr_name_from_json(<<"effDatasetInheritancePath">>) -> eff_dataset_inheritance_path;
attr_name_from_json(<<"effQosInheritancePath">>)     -> eff_qos_inheritance_path;
attr_name_from_json(<<"aggregateQosStatus">>)        -> qos_status;
attr_name_from_json(<<"archiveRecallRootFileId">>)   -> recall_root_id.


-spec attr_name_to_json(file_attr:attribute()) -> binary().
attr_name_to_json(guid)                         -> <<"fileId">>;
attr_name_to_json(index)                        -> <<"index">>;
attr_name_to_json(type)                         -> <<"type">>;
attr_name_to_json(active_permissions_type)      -> <<"activePermissionsType">>;
attr_name_to_json(mode)                         -> <<"posixPermissions">>;
attr_name_to_json(acl)                          -> <<"acl">>;
attr_name_to_json(name)                         -> <<"name">>;
attr_name_to_json(conflicting_name)             -> <<"conflictingName">>;
attr_name_to_json(path)                         -> <<"path">>;
attr_name_to_json(parent_guid)                  -> <<"parentFileId">>;
attr_name_to_json(gid)                          -> <<"displayGid">>;
attr_name_to_json(uid)                          -> <<"displayUid">>;
attr_name_to_json(atime)                        -> <<"atime">>;
attr_name_to_json(mtime)                        -> <<"mtime">>;
attr_name_to_json(ctime)                        -> <<"ctime">>;
attr_name_to_json(size)                         -> <<"size">>;
attr_name_to_json(is_fully_replicated)          -> <<"isFullyReplicatedLocally">>;
attr_name_to_json(local_replication_rate)       -> <<"localReplicationRate">>;
attr_name_to_json(provider_id)                  -> <<"originProviderId">>;
attr_name_to_json(shares)                       -> <<"directShareIds">>;
attr_name_to_json(owner_id)                     -> <<"ownerUserId">>;
attr_name_to_json(hardlink_count)               -> <<"hardlinkCount">>;
attr_name_to_json(symlink_value)                -> <<"symlinkValue">>;
attr_name_to_json(has_custom_metadata)          -> <<"hasCustomMetadata">>;
attr_name_to_json(eff_protection_flags)         -> <<"effProtectionFlags">>;
attr_name_to_json(eff_dataset_protection_flags) -> <<"effDatasetProtectionFlags">>;
attr_name_to_json(eff_dataset_inheritance_path)       -> <<"effDatasetInheritancePath">>;
attr_name_to_json(eff_qos_inheritance_path)           -> <<"effQosInheritancePath">>;
attr_name_to_json(qos_status)                   -> <<"aggregateQosStatus">>;
attr_name_to_json(recall_root_id)               -> <<"archiveRecallRootFileId">>.


%% @TODO VFS-11377 deprecated, remove when possible
%% @private
-spec attr_name_from_json_deprecated(binary()) -> file_attr:attribute().
attr_name_from_json_deprecated(<<"file_id">>)             -> guid;
attr_name_from_json_deprecated(<<"path">>)                -> path;
attr_name_from_json_deprecated(<<"name">>)                -> name;
attr_name_from_json_deprecated(<<"atime">>)               -> atime;
attr_name_from_json_deprecated(<<"mtime">>)               -> mtime;
attr_name_from_json_deprecated(<<"ctime">>)               -> ctime;
attr_name_from_json_deprecated(<<"type">>)                -> type;
attr_name_from_json_deprecated(<<"size">>)                -> size;
attr_name_from_json_deprecated(<<"shares">>)              -> shares;
attr_name_from_json_deprecated(<<"index">>)               -> index;
attr_name_from_json_deprecated(<<"storage_user_id">>)     -> uid;
attr_name_from_json_deprecated(<<"storage_group_id">>)    -> gid;
attr_name_from_json_deprecated(<<"owner_id">>)            -> owner_id;
attr_name_from_json_deprecated(<<"parent_id">>)           -> parent_guid;
attr_name_from_json_deprecated(<<"provider_id">>)         -> provider_id;
attr_name_from_json_deprecated(<<"hardlinks_count">>)     -> hardlink_count;
attr_name_from_json_deprecated(<<"is_fully_replicated">>) -> is_fully_replicated;
attr_name_from_json_deprecated(<<"mode">>)                -> mode.


%% @TODO VFS-11377 deprecated, remove when possible
%% @private
-spec attr_name_to_json_deprecated(file_attr:attribute()) -> binary().
attr_name_to_json_deprecated(guid)                -> <<"file_id">>;
attr_name_to_json_deprecated(path)                -> <<"path">>;
attr_name_to_json_deprecated(name)                -> <<"name">>;
attr_name_to_json_deprecated(atime)               -> <<"atime">>;
attr_name_to_json_deprecated(mtime)               -> <<"mtime">>;
attr_name_to_json_deprecated(ctime)               -> <<"ctime">>;
attr_name_to_json_deprecated(type)                -> <<"type">>;
attr_name_to_json_deprecated(size)                -> <<"size">>;
attr_name_to_json_deprecated(shares)              -> <<"shares">>;
attr_name_to_json_deprecated(index)               -> <<"index">>;
attr_name_to_json_deprecated(uid)                 -> <<"storage_user_id">>;
attr_name_to_json_deprecated(gid)                 -> <<"storage_group_id">>;
attr_name_to_json_deprecated(owner_id)            -> <<"owner_id">>;
attr_name_to_json_deprecated(parent_guid)         -> <<"parent_id">>;
attr_name_to_json_deprecated(provider_id)         -> <<"provider_id">>;
attr_name_to_json_deprecated(hardlink_count)      -> <<"hardlinks_count">>;
attr_name_to_json_deprecated(is_fully_replicated) -> <<"is_fully_replicated">>;
attr_name_to_json_deprecated(mode)                -> <<"mode">>.


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
        {ok, TranslatedAttrs, Xattrs} -> {ok, [{xattrs, Xattrs} | TranslatedAttrs]};
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
        guid => translate_guid(Guid),
        index => file_listing:encode_index(Index),
        type => translate_type(Type),
        active_permissions_type => ActivePermissionsType,
        mode => translate_mode(Mode),
        acl => translate_acl(Acl),
        name => Name,
        conflicting_name => ConflictingName,
        path => Path,
        parent_guid => translate_guid(map_parent_id(ParentGuid)),
        gid => Gid,
        uid => Uid,
        atime => Atime,
        mtime => Mtime,
        ctime => Ctime,
        size => Size,
        is_fully_replicated => FullyReplicated,
        local_replication_rate => LocalReplicationRate,
        provider_id => ProviderId,
        shares => Shares,
        owner_id => OwnerId,
        hardlink_count => HardlinksCount,
        symlink_value => SymlinkValue,
        has_custom_metadata => HasMetadata,
        eff_protection_flags => translate_protection_flags(EffProtectionFlags),
        eff_dataset_protection_flags => translate_protection_flags(EffDatasetProtectionFlags),
        eff_dataset_inheritance_path => translate_membership(EffDatasetInheritancePath),
        eff_qos_inheritance_path => translate_membership(EffQosInheritancePath),
        qos_status => translate_qos_status(QosStatus),
        recall_root_id => RecallRootId
    },
    BaseJson = maps:fold(fun(Key, Value, Acc) ->
        Acc#{attr_name_to_json(AttrType, Key) => utils:undefined_to_null(Value)}
    end, #{}, maps:with(RequestedAttrs, BaseMap)),
    maps:fold(fun(XattrName, XattrValue, Acc) ->
        Acc#{<<"xattr.", XattrName/binary>> => utils:undefined_to_null(XattrValue)}
    end, BaseJson, utils:ensure_defined(Xattrs, #{})).


%% @private
-spec all_attrs(attr_type()) -> [file_attr:attribute()].
all_attrs(deprecated) -> [path | ?DEPRECATED_ALL_ATTRS]; % path is allowed in deprecated recursive listing
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
translate_membership(?NONE_INHERITANCE_PATH)                -> <<"none">>;
translate_membership(?DIRECT_INHERITANCE_PATH)              -> <<"direct">>;
translate_membership(?ANCESTOR_INHERITANCE)                 -> <<"ancestor">>;
translate_membership(?DIRECT_AND_ANCESTOR_INHERITANCE_PATH) -> <<"directAndAncestor">>;
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
