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

%% datastore_model callbacks
-export([
    get_record_version/0, get_record_struct/1,
    upgrade_record/2, resolve_conflict/3, on_remote_doc_created/2
]).

-define(FILE_META_MODEL, file_meta).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    12.


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
    ]};
get_record_struct(11) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        % field 'protection_flags' has been added in this version
        {protection_flags, integer},
        {acl, [{record, [
            {acetype, integer},
            {aceflags, integer},
            {identifier, string},
            {name, string},
            {acemask, integer}
        ]}]},
        {owner, string},
        {is_scope, boolean},
        {provider_id, string},
        {shares, [string]},
        {deleted, boolean},
        {parent_uuid, string}
    ]};
get_record_struct(12) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {protection_flags, integer},
        {acl, [{record, [
            {acetype, integer},
            {aceflags, integer},
            {identifier, string},
            {name, string},
            {acemask, integer}
        ]}]},
        {owner, string},
        {is_scope, boolean},
        {provider_id, string},
        {shares, [string]},
        {deleted, boolean},
        {parent_uuid, string},
        % fields: references and symlink_value have been added in this version
        {references, #{string => [string]}},
        {symlink_value, string},
        % field dataset_status has been added in this version
        {dataset_status, atom}
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
    }};
upgrade_record(10, {
    ?FILE_META_MODEL, Name, Type, Mode, ACL, Owner, IsScope,
    ProviderId, Shares, Deleted, ParentUuid
}) ->
    {11, {?FILE_META_MODEL, Name, Type, Mode, 0, ACL, Owner, IsScope,
        ProviderId, Shares, Deleted, ParentUuid
    }};
% NOTE: there are 2 function clauses upgrading from version 11 to 12
% because 21.02-alpha7 introduced a bug which could result in documents persisted with wrong number of fields
upgrade_record(11, {?FILE_META_MODEL, Name, Type, Mode, ProtectionFlags, ACL, Owner, IsScope,
    ProviderId, Shares, Deleted, ParentUuid, References, SymlinkValue
}) ->
    {12, {?FILE_META_MODEL, Name, Type, Mode, ProtectionFlags, ACL, Owner, IsScope,
        ProviderId, Shares, Deleted, ParentUuid,
        % fields: references and symlink_value have been added in this version
        References, SymlinkValue,
        % field dataset_status has been added in this version
        undefined
    }};
upgrade_record(11, {?FILE_META_MODEL, Name, Type, Mode, ProtectionFlags, ACL, Owner, IsScope,
    ProviderId, Shares, Deleted, ParentUuid
}) ->
    {12, {?FILE_META_MODEL, Name, Type, Mode, ProtectionFlags, ACL, Owner, IsScope,
        ProviderId, Shares, Deleted, ParentUuid,
        % fields: references and symlink_value have been added in this version
        #{}, undefined,
        % field dataset_status has been added in this version
        undefined
    }}.



%%--------------------------------------------------------------------
%% @doc
%% Function called when saving changes from other providers
%% (checks conflicts: local doc vs. remote changes).
%% It is used to check if file has been renamed remotely to send appropriate event.
%% TODO - VFS-5962 - delete when event emission is possible in dbsync_events.
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(datastore_model:ctx(), file_meta:doc(), file_meta:doc()) ->
    default | {true, file_meta:doc()}.
resolve_conflict(_Ctx,
    NewDoc = #document{
        key = Uuid,
        value = #file_meta{
            name = NewName,
            parent_uuid = NewParentUuid,
            type = Type,
            mode = Mode,
            acl = Acl,
            shares = Shares
        },
        revs = [NewRev | _],
        scope = SpaceId
    }, PrevDoc = #document{
        value = #file_meta{
            name = PrevName,
            parent_uuid = PrevParentUuid,
            mode = PrevMode,
            acl = PrevAcl,
            shares = PrevShares
        },
        revs = [PrevlRev | _]
    }
) ->
    case datastore_rev:is_greater(NewRev, PrevlRev) of
        true ->
            invalidate_effective_caches_if_moved(NewDoc, PrevDoc),
            invalidate_dataset_eff_cache_if_needed(NewDoc, PrevDoc),
            spawn(fun() ->
                timer:sleep(200), % Invalidation of cache must occur after doc is saved
                invalidate_qos_bounded_cache_if_moved_to_trash(NewDoc, PrevDoc),

                case (NewName =/= PrevName) orelse (NewParentUuid =/= PrevParentUuid) of
                    true ->
                        FileCtx = file_ctx:new_by_uuid(Uuid, SpaceId),
                        OldParentGuid = file_id:pack_guid(PrevParentUuid, SpaceId),
                        NewParentGuid = file_id:pack_guid(NewParentUuid, SpaceId),
                        paths_cache:invalidate_on_all_nodes(SpaceId),
                        permissions_cache:invalidate(),
                        fslogic_event_emitter:emit_file_renamed_no_exclude(
                            FileCtx, OldParentGuid, NewParentGuid, NewName, PrevName),

                        case NewParentUuid =/= PrevParentUuid of
                            true ->
                                qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
                                qos_logic:reconcile_qos(FileCtx),
                                dir_stats_collector:report_file_moved(Type, file_ctx:get_logical_guid_const(FileCtx),
                                    OldParentGuid, NewParentGuid);
                            false ->
                                ok
                        end;
                    false ->
                        case (Mode =/= PrevMode) orelse (Acl =/= PrevAcl) orelse (Shares =/= PrevShares) of
                            true -> permissions_cache:invalidate();
                            false -> ok
                        end
                end,

                case file_meta:is_deleted(NewDoc) andalso not file_meta:is_deleted(PrevDoc) of
                    true ->
                        dir_size_stats:report_file_deleted(Type, file_id:pack_guid(NewParentUuid, SpaceId));
                    false ->
                        ok
                end,

                case (Mode =/= PrevMode) orelse (Acl =/= PrevAcl) of
                    true ->
                        Ctx = file_ctx:new_by_uuid(Uuid, SpaceId),
                        fslogic_event_emitter:emit_sizeless_file_attrs_changed(Ctx),
                        fslogic_event_emitter:emit_file_perm_changed(Ctx);
                    false ->
                        ok
                end
            end);
        false ->
            ok
    end,

    case file_meta_hardlinks:merge_references(NewDoc, PrevDoc) of
        not_mutated ->
            default;
        {mutated, MergedReferences} ->
            DocBase = #document{value = RecordBase} = case datastore_rev:is_greater(NewRev, PrevlRev) of
                true -> NewDoc;
                false -> PrevDoc
            end,
            {true, DocBase#document{value = RecordBase#file_meta{references = MergedReferences}}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Function called when new record appears from remote provider.
%% @end
%%--------------------------------------------------------------------
-spec on_remote_doc_created(datastore_model:ctx(), file_meta:doc()) -> ok.
on_remote_doc_created(_Ctx, #document{value = #file_meta{deleted = true}}) ->
    ok;
on_remote_doc_created(_Ctx, #document{deleted = true}) ->
    ok;
on_remote_doc_created(_Ctx, #document{
    key = Key,
    value = #file_meta{type = Type, parent_uuid = ParentUuid}, scope = SpaceId
}) ->
    spawn(fun() ->
        case fslogic_file_id:is_space_dir_uuid(Key) orelse fslogic_file_id:is_trash_dir_uuid(Key) of
            true -> ok;
            false -> dir_size_stats:report_file_created(Type, file_id:pack_guid(ParentUuid, SpaceId))
        end
    end).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec invalidate_dataset_eff_cache_if_needed(file_meta:doc(), file_meta:doc()) -> ok.
invalidate_dataset_eff_cache_if_needed(
    #document{value = #file_meta{
        protection_flags = NewFlags,
        parent_uuid = NewParentUuid,
        dataset_state = NewDatasetState
    }, scope = SpaceId},
    #document{value = #file_meta{
        protection_flags = OldFlags,
        parent_uuid = PrevParentUuid,
        dataset_state = OldDatasetState
    }}
) ->
    % TODO VFS-7518 resolve conflicts on creating datasets
    case OldFlags =/= NewFlags
        orelse PrevParentUuid =/= NewParentUuid
        orelse NewDatasetState =/= OldDatasetState
    of
        true ->
            spawn(fun() ->
                dataset_eff_cache:invalidate_on_all_nodes(SpaceId)
            end),
            ok;
        false ->
            ok
    end.


%% @private
-spec invalidate_qos_bounded_cache_if_moved_to_trash(file_meta:doc(), file_meta:doc()) -> ok.
invalidate_qos_bounded_cache_if_moved_to_trash(
    #document{key = Uuid, value = #file_meta{parent_uuid = NewParentUuid}, scope = SpaceId}, #document{value = #file_meta{parent_uuid = PrevParentUuid}
}) ->
    case PrevParentUuid =/= NewParentUuid andalso fslogic_file_id:is_trash_dir_uuid(NewParentUuid) of
        true ->
            % the file has been moved to trash
            FileCtx = file_ctx:new_by_uuid(Uuid, SpaceId),
            PrevParentCtx = file_ctx:new_by_uuid(PrevParentUuid, SpaceId),
            file_qos:cleanup_reference_related_documents(FileCtx, PrevParentCtx),
            qos_bounded_cache:invalidate_on_all_nodes(SpaceId);
        false ->
            ok
    end.


%% @private
-spec invalidate_effective_caches_if_moved(file_meta:doc(), file_meta:doc()) -> ok.
invalidate_effective_caches_if_moved(
    #document{value = #file_meta{name = NewName, parent_uuid = NewParentUuid}, scope = SpaceId},
    #document{value = #file_meta{name = OldName, parent_uuid = PrevParentUuid}}
) ->
    case NewName =/= OldName orelse PrevParentUuid =/= NewParentUuid of
        true ->
            spawn(fun() ->
                paths_cache:invalidate_on_all_nodes(SpaceId),
                archive_recall_cache:invalidate_on_all_nodes(SpaceId),
                file_meta_sync_status_cache:invalidate_on_all_nodes(SpaceId)
            end),
            ok;
        false ->
            ok
    end.
