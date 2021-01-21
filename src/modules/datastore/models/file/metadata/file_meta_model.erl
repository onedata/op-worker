%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains datastore callbacks for file_meta model.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_model).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").


-define(FILE_META_MODEL, file_meta).

%% datastore_model callbacks
-export([get_record_struct/1, upgrade_record/2, resolve_conflict/3]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {uid, string},
        {size, integer},
        {version, integer},
        {is_scope, boolean},
        {scope, string},
        {provider_id, string},
        {link_value, string},
        {shares, [string]}
    ]};
get_record_struct(2) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {owner, string},
        {size, integer},
        {version, integer},
        {is_scope, boolean},
        {scope, string},
        {provider_id, string},
        {link_value, string},
        {shares, [string]}
    ]};
get_record_struct(3) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {owner, string},
        {size, integer},
        {version, integer},
        {is_scope, boolean},
        {scope, string},
        {provider_id, string},
        {link_value, string},
        {shares, [string]},
        {deleted, boolean},
        {storage_sync_info, {record, [
            {children_attrs_hash, #{integer => binary}},
            {last_synchronized_mtime, integer}
        ]}}
    ]};
get_record_struct(4) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {owner, string},
        {size, integer},
        {version, integer},
        {is_scope, boolean},
        {scope, string},
        {provider_id, string},
        {link_value, string},
        {shares, [string]},
        {deleted, boolean},
        {storage_sync_info, {record, [
            {children_attrs_hash, #{integer => binary}},
            {last_synchronized_mtime, integer}
        ]}},
        {parent_uuid, string}
    ]};
get_record_struct(5) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {owner, string},
        {group_owner, string},
        {size, integer},
        {version, integer},
        {is_scope, boolean},
        {scope, string},
        {provider_id, string},
        {link_value, string},
        {shares, [string]},
        {deleted, boolean},
        {storage_sync_info, {record, [
            {children_attrs_hash, #{integer => binary}},
            {last_synchronized_mtime, integer}
        ]}},
        {parent_uuid, string}
    ]};
get_record_struct(6) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {owner, string},
        {group_owner, string},
        {size, integer},
        {version, integer},
        {is_scope, boolean},
        {scope, string},
        {provider_id, string},
        {link_value, string},
        {shares, [string]},
        {deleted, boolean},
        {parent_uuid, string}
    ]};
get_record_struct(7) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {owner, string},
        {group_owner, string},
        % size field  has been removed in this version
        {is_scope, boolean},
        {scope, string},
        {provider_id, string},
        {shares, [string]},
        {deleted, boolean},
        {parent_uuid, string}
    ]};
get_record_struct(8) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        % acl has been added in this version
        {acl, [{record, [
            {acetype, integer},
            {aceflags, integer},
            {identifier, string},
            {name, string},
            {acemask, integer}
        ]}]},
        {owner, string},
        {group_owner, string},
        {is_scope, boolean},
        {scope, string},
        {provider_id, string},
        {shares, [string]},
        {deleted, boolean},
        {parent_uuid, string}
    ]};
get_record_struct(9) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {acl, [{record, [
            {acetype, integer},
            {aceflags, integer},
            {identifier, string},
            {name, string},
            {acemask, integer}
        ]}]},
        {owner, string},
        {group_owner, string},
        {is_scope, boolean},
        % scope field has been deleted in this version
        {provider_id, string},
        {shares, [string]},
        {deleted, boolean},
        {parent_uuid, string}
    ]};
get_record_struct(10) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {acl, [{record, [
            {acetype, integer},
            {aceflags, integer},
            {identifier, string},
            {name, string},
            {acemask, integer}
        ]}]},
        {owner, string},
        % field group_owner has been deleted in this version
        {is_scope, boolean},
        {provider_id, string},
        {shares, [string]},
        {deleted, boolean},
        {parent_uuid, string}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?FILE_META_MODEL, Name, Type, Mode, Uid, Size, Version, IsScope,
    Scope, ProviderId, LinkValue, Shares}
) ->
    {2, {?FILE_META_MODEL, Name, Type, Mode, Uid, Size, Version, IsScope, Scope,
        ProviderId, LinkValue, Shares}};
upgrade_record(2, {?FILE_META_MODEL, Name, Type, Mode, Owner, Size, Version, IsScope,
    Scope, ProviderId, LinkValue, Shares}
) ->
    {3, {?FILE_META_MODEL, Name, Type, Mode, Owner, Size, Version, IsScope,
        Scope, ProviderId, LinkValue, Shares, false, {storage_sync_info, #{}, undefined}}};
upgrade_record(3, {?FILE_META_MODEL, Name, Type, Mode, Owner, Size, Version, IsScope,
    Scope, ProviderId, LinkValue, Shares, Deleted, StorageSyncInfo}
) ->
    {4, {?FILE_META_MODEL, Name, Type, Mode, Owner, Size, Version, IsScope,
        Scope, ProviderId, LinkValue, Shares, Deleted, StorageSyncInfo, undefined}
    };
upgrade_record(4, {?FILE_META_MODEL, Name, Type, Mode, Owner, Size, Version, IsScope,
    Scope, ProviderId, LinkValue, Shares, Deleted, StorageSyncInfo, ParentUuid}
) ->
    {5, {?FILE_META_MODEL, Name, Type, Mode, Owner, undefined, Size, Version, IsScope,
        Scope, ProviderId, LinkValue, Shares, Deleted, StorageSyncInfo, ParentUuid}
    };
upgrade_record(5, {?FILE_META_MODEL, Name, Type, Mode, Owner, GroupOwner, Size, Version, IsScope,
    Scope, ProviderId, LinkValue, Shares, Deleted, _StorageSyncInfo, ParentUuid}
) ->
    {6, {?FILE_META_MODEL, Name, Type, Mode, Owner, GroupOwner, Size, Version, IsScope,
        Scope, ProviderId, LinkValue, Shares, Deleted, ParentUuid}
    };
upgrade_record(6, {?FILE_META_MODEL, Name, Type, Mode, Owner, GroupOwner, _Size, _Version, IsScope,
    Scope, ProviderId, _LinkValue, Shares, Deleted, ParentUuid}
) ->
    {7, {?FILE_META_MODEL, Name, Type, Mode, Owner, GroupOwner, IsScope,
        Scope, ProviderId, Shares, Deleted, ParentUuid}
    };
upgrade_record(7, {
    ?FILE_META_MODEL, Name, Type, Mode, Owner, GroupOwner, IsScope,
    Scope, ProviderId, Shares, Deleted, ParentUuid
}) ->
    {8, {?FILE_META_MODEL, Name, Type, Mode, [],
        Owner, GroupOwner, IsScope, Scope,
        ProviderId, Shares, Deleted, ParentUuid
    }};
upgrade_record(8, {
    ?FILE_META_MODEL, Name, Type, Mode, ACL, Owner, GroupOwner, IsScope,
    _Scope, ProviderId, Shares, Deleted, ParentUuid
}) ->
    {9, {?FILE_META_MODEL, Name, Type, Mode, ACL, Owner, GroupOwner, IsScope,
        ProviderId, Shares, Deleted, ParentUuid
    }};
upgrade_record(9, {
    ?FILE_META_MODEL, Name, Type, Mode, ACL, Owner, _GroupOwner, IsScope,
    ProviderId, Shares, Deleted, ParentUuid
}) ->
    {10, {?FILE_META_MODEL, Name, Type, Mode, ACL, Owner, IsScope,
        ProviderId, Shares, Deleted, ParentUuid
    }}.


%%--------------------------------------------------------------------
%% @doc
%% Function called when saving changes from other providers
%% (checks conflicts: local doc vs. remote changes).
%% It is used to check if file has been renamed remotely to send appropriate event.
%% TODO - VFS-5962 - delete when event emission is possible in dbsync_events.
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(datastore_model:ctx(), file_meta:doc(), file_meta:doc()) -> default.
resolve_conflict(_Ctx,
    NewDoc = #document{key = Uuid, value = #file_meta{name = NewName, parent_uuid = NewParentUuid}, scope = SpaceId},
    PrevDoc = #document{value = #file_meta{name = PrevName, parent_uuid = PrevParentUuid}}
) ->
    spawn(fun() ->
        invalidate_qos_bounded_cache_if_moved_to_trash(NewDoc, PrevDoc)
    end),
    case NewName =/= PrevName of
        true ->
            spawn(fun() ->
                FileCtx = file_ctx:new_by_guid(file_id:pack_guid(Uuid, SpaceId)),
                OldParentGuid = file_id:pack_guid(PrevParentUuid, SpaceId),
                NewParentGuid = file_id:pack_guid(NewParentUuid, SpaceId),
                fslogic_event_emitter:emit_file_renamed_no_exclude(
                    FileCtx, OldParentGuid, NewParentGuid, NewName, PrevName)
            end);
        _ ->
            ok
    end,

    default.



%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec invalidate_qos_bounded_cache_if_moved_to_trash(file_meta:doc(), file_meta:doc()) -> ok.
invalidate_qos_bounded_cache_if_moved_to_trash(
    #document{value = #file_meta{parent_uuid = NewParentUuid}, scope = SpaceId},
    #document{value = #file_meta{parent_uuid = PrevParentUuid}
}) ->
    case PrevParentUuid =/= NewParentUuid andalso fslogic_uuid:is_trash_dir_uuid(NewParentUuid) of
        true ->
            % the file has been moved to trash
            qos_bounded_cache:invalidate_on_all_nodes(SpaceId);
        false ->
            ok
    end.