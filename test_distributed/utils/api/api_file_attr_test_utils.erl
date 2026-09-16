%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Builds the JSON representation of file attributes as expected to be
%%% returned by the op-worker API (REST and graph sync).
%%% @end
%%%-------------------------------------------------------------------
-module(api_file_attr_test_utils).
-author("Bartosz Walkowicz").

-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/onedata_file.hrl").

-export([
    file_attr_to_json/4,
    replace_attrs_with_deprecated/1
]).


%%%===================================================================
%%% API
%%%===================================================================


%% @TODO VFS-11376 Use file_attr_translator after it is properly unit tested
-spec file_attr_to_json(undefined | od_share:id(), rest | gs, od_provider:id(), #file_attr{}) -> map().
file_attr_to_json(undefined, ApiType, CheckingProviderId, #file_attr{
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
    creation_time = CreationTime,
    atime = Atime,
    mtime = Mtime,
    ctime = Ctime,
    size = Size,
    provider_id = ProviderId,
    shares = Shares,
    owner_id = OwnerId,
    hardlink_count = HardlinksCount,
    symlink_value = SymlinkValue,
    has_custom_metadata = HasMetadata,
    has_json_metadata = HasJsonMetadata,
    eff_protection_flags = EffProtectionFlags,
    eff_dataset_protection_flags = EffDatasetProtectionFlags,
    eff_dataset_inheritance_path = EffDatasetInheritancePath,
    eff_qos_inheritance_path = EffQosInheritancePath,
    qos_status = QosStatus,
    recall_root_id = RecallRootId,
    json_metadata = JsonMetadata,
    xattrs = Xattrs
}) ->
    % NOTE: this assumes that there were no remote file readings between creation and attrs check
    LocalReplicationRate = case {Type, CheckingProviderId, Size} of
        {_, _, 0} -> 1.0;
        {?REGULAR_FILE_TYPE, ProviderId, _} -> 1.0;
        {?DIRECTORY_TYPE, ProviderId, _} -> 1.0;
        _ -> 0.0
    end,
    
    LocalReplicationRate2 = case CheckingProviderId == undefined orelse
        opw_test_rpc:supports_space(CheckingProviderId, file_id:guid_to_space_id(Guid))
    of
        true -> LocalReplicationRate;
        false -> null
    end,
    
    % is_fully_replicated is set only for regular files
    IsFullyReplicatedLocally = case Type of
        ?REGULAR_FILE_TYPE -> LocalReplicationRate2 == 1.0;
        _ -> null
    end,
    
    BaseJson = #{
        <<"fileId">> => map_file_id_for_api_type(ApiType, Guid),
        <<"index">> => file_listing:encode_index(Index),
        <<"type">> => str_utils:to_binary(Type),
        <<"activePermissionsType">> => case ActivePermissionsType of
            undefined -> undefined;
            _ -> atom_to_binary(ActivePermissionsType)
        end,
        <<"posixPermissions">> => list_to_binary(string:right(integer_to_list(Mode, 8), 3, $0)),
        <<"acl">> => case Acl of
            undefined -> null;
            _ -> acl:to_json(Acl, gui)
        end,
        <<"name">> => Name,
        <<"conflictingName">> => ConflictingName,
        <<"path">> => utils:undefined_to_null(Path),
        <<"parentFileId">> => map_file_id_for_api_type(ApiType, ParentGuid),
        <<"displayGid">> => Gid,
        <<"displayUid">> => Uid,
        <<"creationTime">> => CreationTime,
        <<"atime">> => Atime,
        <<"mtime">> => Mtime,
        <<"ctime">> => Ctime,
        <<"size">> => utils:undefined_to_null(Size),
        <<"isFullyReplicatedLocally">> => IsFullyReplicatedLocally,
        <<"localReplicationRate">> => LocalReplicationRate2,
        <<"originProviderId">> => ProviderId,
        <<"directShareIds">> => Shares,
        <<"ownerUserId">> => map_owner_for_api_type(ApiType, OwnerId),
        <<"hardlinkCount">> => utils:undefined_to_null(HardlinksCount),
        <<"symlinkValue">> => SymlinkValue,
        <<"hasCustomMetadata">> => HasMetadata,
        <<"hasJsonMetadata">> => HasJsonMetadata,
        <<"effProtectionFlags">> => case EffProtectionFlags of
            undefined -> undefined;
            _ -> file_meta:protection_flags_to_json(EffProtectionFlags)
        end,
        <<"effDatasetProtectionFlags">> => case EffDatasetProtectionFlags of
            undefined -> undefined;
            _ -> file_meta:protection_flags_to_json(EffDatasetProtectionFlags)
        end,
        <<"effDatasetInheritancePath">> => translate_membership(EffDatasetInheritancePath),
        <<"effQosInheritancePath">> => translate_membership(EffQosInheritancePath),
        <<"aggregateQosStatus">> => translate_qos_status(QosStatus),
        <<"archiveRecallRootFileId">> => RecallRootId,
        <<"jsonMetadata">> => utils:undefined_to_null(JsonMetadata)
    },
    FinalJson = maps:fold(fun(XattrName, XattrValue, Acc) ->
        Acc#{<<"xattr.", XattrName/binary>> => utils:undefined_to_null(XattrValue)}
    end, BaseJson, utils:ensure_defined(Xattrs, #{})),
    maps_utils:undefined_to_null(FinalJson);
file_attr_to_json(ShareId, ApiType, CheckingProviderId, #file_attr{
    guid = FileGuid,
    mode = Mode,
    parent_guid = ParentGuid,
    shares = Shares
} = FileAttr) ->
    IsShareRoot = lists:member(ShareId, Shares),
    
    BaseJson = file_attr_to_json(undefined, ApiType, CheckingProviderId, FileAttr),
    
    maps:with(lists:map(fun onedata_file:attr_name_to_json/1, ?PUBLIC_API_FILE_ATTRS), BaseJson#{
        <<"fileId">> => map_file_id_for_api_type(ApiType, file_id:guid_to_share_guid(FileGuid, ShareId)),
        <<"parentFileId">> => case IsShareRoot of
            true -> null;
            false -> map_file_id_for_api_type(ApiType, file_id:guid_to_share_guid(ParentGuid, ShareId))
        end,
        <<"posixPermissions">> => list_to_binary(string:right(integer_to_list(Mode band 2#111, 8), 3, $0)),
        <<"directShareIds">> => case IsShareRoot of
            true -> [ShareId];
            false -> []
        end
    }).


-spec replace_attrs_with_deprecated(json_utils:json_map()) -> json_utils:json_map().
replace_attrs_with_deprecated(JsonAttrs) ->
    maps:fold(fun
        (<<"xattr.", _/binary>> = K, V, Acc) ->
            Acc#{K => V};
        (K, V, Acc) ->
            A = onedata_file:attr_name_from_json(K),
            case lists:member(A, ?DEPRECATED_ALL_FILE_ATTRS) of
                true ->
                    DeprecatedKey = onedata_file:attr_name_to_json(deprecated, A),
                    Acc#{DeprecatedKey => V};
                false ->
                    Acc
            end
    end, #{}, JsonAttrs).


-spec map_file_id_for_api_type(gs | rest, file_id:file_guid() | undefined) -> file_id:objectid() | file_id:file_guid() | null.
map_file_id_for_api_type(_, undefined) ->
    null;
map_file_id_for_api_type(gs, Guid) ->
    Guid;
map_file_id_for_api_type(rest, Guid) ->
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),
    ObjectId.


-spec map_owner_for_api_type(gs | rest, od_user:id() | undefined) -> od_user:id() | null.
map_owner_for_api_type(_, undefined) ->
    null;
map_owner_for_api_type(gs, ?SPACE_OWNER_ID(_)) ->
    null;
map_owner_for_api_type(_, UserId) ->
    UserId.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
translate_membership(undefined)                             -> undefined;
translate_membership(?none_inheritance_path)                -> <<"none">>;
translate_membership(?direct_inheritance_path)              -> <<"direct">>;
translate_membership(?ancestor_inheritance)                 -> <<"ancestor">>;
translate_membership(?direct_and_ancestor_inheritance_path) -> <<"directAndAncestor">>.


%% @private
translate_qos_status(undefined) -> undefined;
translate_qos_status(Status) ->    atom_to_binary(Status).
