%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an access to the event specific data.
%%% @end
%%%-------------------------------------------------------------------
-module(event_type).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").
-include("modules/events/routing.hrl").
-include("modules/monitoring/events.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_routing_key/2, get_attr_routing_keys/2, get_replica_status_routing_keys/2,
    get_attr_routing_keys_without_replica_status_changes/2,
    get_stream_key/1, get_aggregation_key/1]).
-export([get_context/1]).
-export([get_reference_based_prefix/1,
    get_attr_changed_reference_based_prefix/0, get_replica_status_reference_based_prefix/0]).

-type aggregation_key() :: term().
-type ctx() :: undefined | {file, file_id:file_guid()}.
-type routing_ctx() :: #{
    file_ctx => file_ctx:ctx(),
    % allows explicit setting of parent e.g. when file is moved and old parent should be used
    parent => fslogic_worker:file_guid()
} | undefined. % if routing_ctx is undefined it is automatically created basing on event content
-type auth_check_type() :: attrs | location | rename.

-export_type([aggregation_key/0, ctx/0, routing_ctx/0, auth_check_type/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a routing key that will be used to query globally cached event routing
%% table for subscribers interested in receiving this event.
%% @end
%%--------------------------------------------------------------------
-spec get_routing_key(Evt :: event:base() | event:type(), RoutingCtx :: routing_ctx()) ->
    {ok, subscription_manager:event_routing_keys()} | {error, session_only}.
get_routing_key(#event{type = Type}, RoutingCtx) ->
    get_routing_key(Type, RoutingCtx);
get_routing_key(#file_attr_changed_event{file_attr = FileAttr}, RoutingCtx) ->
    {ok, check_links_and_get_parent_connected_routing_key(<<"file_attr_changed.">>, FileAttr#file_attr.guid, RoutingCtx)};
get_routing_key(#file_location_changed_event{file_location = FileLocation}, _RoutingCtx) ->
    FileUuid = FileLocation#file_location.uuid,
    SpaceId = FileLocation#file_location.space_id,
    {ok, References} = file_meta_hardlinks:list_references(fslogic_file_id:ensure_referenced_uuid(FileUuid)),
    AdditionalKeys = lists:map(fun(Uuid) ->
        {{uuid, Uuid, SpaceId}, <<"file_location_changed.", Uuid/binary>>}
    end, References -- [FileUuid]),
    {ok, #event_routing_keys{
        file_ctx = file_ctx:new_by_uuid(FileUuid, SpaceId),
        main_key = <<"file_location_changed.", FileUuid/binary>>,
        additional_keys = AdditionalKeys,
        auth_check_type = location
    }};
get_routing_key(#file_perm_changed_event{file_guid = FileGuid}, _RoutingCtx) ->
    FileUuid = file_id:guid_to_uuid(FileGuid),
    SpaceId = file_id:guid_to_space_id(FileGuid),
    {ok, References} = file_meta_hardlinks:list_references(fslogic_file_id:ensure_referenced_uuid(FileUuid)),
    AdditionalKeys = lists:map(fun(Uuid) ->
        {{guid, file_id:pack_guid(Uuid, SpaceId)}, <<"file_perm_changed.", Uuid/binary>>}
    end, References -- [FileUuid]),
    {ok, #event_routing_keys{
        file_ctx = file_ctx:new_by_guid(FileGuid),
        main_key = <<"file_perm_changed.", FileUuid/binary>>,
        additional_keys = AdditionalKeys
    }};
get_routing_key(#file_removed_event{file_guid = FileGuid}, RoutingCtx) ->
    {ok, get_parent_connected_routing_key(<<"file_removed.">>, FileGuid, RoutingCtx)};
get_routing_key(#file_renamed_event{top_entry = Entry}, RoutingCtx) ->
    Ans = get_parent_connected_routing_key(<<"file_renamed.">>, Entry#file_renamed_entry.old_guid, RoutingCtx),
    {ok, Ans#event_routing_keys{auth_check_type = rename}};
get_routing_key(#quota_exceeded_event{}, _RoutingCtx) ->
    {ok, #event_routing_keys{main_key = <<"quota_exceeded">>}};
get_routing_key(#helper_params_changed_event{storage_id = StorageId}, _RoutingCtx) ->
    {ok, #event_routing_keys{main_key = <<"helper_params_changed.", StorageId/binary>>}};
get_routing_key(_, _) ->
    {error, session_only}.

%%--------------------------------------------------------------------
%% @doc
%% Returns a routing keys (see get_routing_key fun) for events connected with file_attrs.
%% Warning: It is only temporary solution as currently events framework does not allow to parametrize subscriptions.
%% Multiple routing keys for event should not be used.
%% @end
%%--------------------------------------------------------------------
-spec get_attr_routing_keys(Guid :: fslogic_worker:file_guid(), RoutingCtx :: routing_ctx()) ->
    {subscription_manager:event_routing_keys(), subscription_manager:event_routing_keys()}.
get_attr_routing_keys(Guid, RoutingCtx) ->
    {check_links_and_get_parent_connected_routing_key(<<"file_attr_changed.">>, Guid, RoutingCtx),
        check_links_and_get_parent_connected_routing_key(<<"replica_status_changed.">>, Guid, RoutingCtx)}.

%%--------------------------------------------------------------------
%% @doc
%% Returns a routing keys (see get_routing_key fun) for events connected with change of replication status.
%% Warning: It is only temporary solution as currently events framework does not allow to parametrize subscriptions.
%% Usually, get_routing_key function should be used.
%% @end
%%--------------------------------------------------------------------
-spec get_replica_status_routing_keys(Guid :: fslogic_worker:file_guid(), RoutingCtx :: routing_ctx()) ->
    subscription_manager:event_routing_keys().
get_replica_status_routing_keys(Guid, RoutingCtx) ->
    check_links_and_get_parent_connected_routing_key(<<"replica_status_changed.">>, Guid, RoutingCtx).

%%--------------------------------------------------------------------
%% @doc
%% Returns a routing keys (see get_routing_key fun) for events connected with file_attrs excluding events
%% connected only with change of replication status.
%% Warning: It is only temporary solution as currently events framework does not allow to parametrize subscriptions.
%% Usually, get_routing_key function should be used.
%% @end
%%--------------------------------------------------------------------
-spec get_attr_routing_keys_without_replica_status_changes(Guid :: fslogic_worker:file_guid(),
    RoutingCtx :: routing_ctx()) -> subscription_manager:event_routing_keys().
get_attr_routing_keys_without_replica_status_changes(Guid, RoutingCtx) ->
    check_links_and_get_parent_connected_routing_key(<<"file_attr_changed.">>, Guid, RoutingCtx).

%%--------------------------------------------------------------------
%% @doc
%% Returns a key of a stream responsible for processing a given event.
%% @end
%%--------------------------------------------------------------------
-spec get_stream_key(Evt :: event:base() | event:type()) ->
    Key :: event_stream:key().
get_stream_key(#event{type = Type}) -> get_stream_key(Type);
get_stream_key(#file_read_event{}) -> file_read;
get_stream_key(#file_written_event{}) -> file_written;
get_stream_key(#file_attr_changed_event{}) -> file_attr_changed;
get_stream_key(#file_location_changed_event{}) -> file_location_changed;
get_stream_key(#file_perm_changed_event{}) -> file_perm_changed;
get_stream_key(#file_removed_event{}) -> file_removed;
get_stream_key(#file_renamed_event{}) -> file_renamed;
get_stream_key(#quota_exceeded_event{}) -> quota_exceeded;
get_stream_key(#helper_params_changed_event{}) -> helper_params_changed;
get_stream_key(#monitoring_event{}) -> monitoring.

%%--------------------------------------------------------------------
%% @doc
%% Returns an arbitrary value that distinguish two events in a stream, i.e.
%% events with the same aggregation key will be aggregated.
%% @end
%%--------------------------------------------------------------------
-spec get_aggregation_key(Evt :: event:base() | event:type()) ->
    Key :: aggregation_key().
get_aggregation_key(#event{type = Type}) ->
    get_aggregation_key(Type);
get_aggregation_key(#file_read_event{file_guid = FileGuid}) ->
    FileGuid;
get_aggregation_key(#file_written_event{file_guid = FileGuid}) ->
    FileGuid;
get_aggregation_key(#file_attr_changed_event{file_attr = FileAttr}) ->
    FileAttr#file_attr.guid;
get_aggregation_key(#file_location_changed_event{file_location = FileLocation}) ->
    file_id:pack_guid(
        FileLocation#file_location.uuid,
        FileLocation#file_location.space_id
    );
get_aggregation_key(#file_perm_changed_event{file_guid = FileGuid}) ->
    FileGuid;
get_aggregation_key(#file_removed_event{file_guid = FileGuid}) ->
    FileGuid;
get_aggregation_key(#file_renamed_event{top_entry = Entry}) ->
    Entry#file_renamed_entry.old_guid;
get_aggregation_key(#quota_exceeded_event{}) ->
    <<>>;
get_aggregation_key(#helper_params_changed_event{storage_id = StorageId}) ->
    StorageId;
get_aggregation_key(#monitoring_event{type = #storage_used_updated{} = Type}) ->
    #storage_used_updated{space_id = SpaceId, user_id = UserId} = Type,
    {SpaceId, UserId, <<"storage_used_updated">>};
get_aggregation_key(#monitoring_event{type = #od_space_updated{} = Type}) ->
    #od_space_updated{space_id = SpaceId} = Type,
    {SpaceId, <<"od_space_updated">>};
get_aggregation_key(#monitoring_event{type = #file_operations_statistics{} = Type}) ->
    #file_operations_statistics{space_id = SpaceId, user_id = UserId} = Type,
    {SpaceId, UserId, <<"file_operations_statistics">>};
get_aggregation_key(#monitoring_event{type = #rtransfer_statistics{} = Type}) ->
    #rtransfer_statistics{space_id = SpaceId, user_id = UserId} = Type,
    {SpaceId, UserId, <<"rtransfer_statistics">>}.

%%--------------------------------------------------------------------
%% @doc
%% Returns an event context.
%% @end
%%--------------------------------------------------------------------
-spec get_context(Evt :: event:base() | event:type()) -> Ctx :: ctx().
get_context(#event{type = Type}) ->
    get_context(Type);
get_context(#file_read_event{file_guid = FileGuid}) ->
    {file, FileGuid};
get_context(#file_written_event{file_guid = FileGuid}) ->
    {file, FileGuid};
get_context(#file_attr_changed_event{file_attr = FileAttr}) ->
    {file, FileAttr#file_attr.guid};
get_context(#file_location_changed_event{file_location = FileLocation}) ->
    {file, file_id:pack_guid(FileLocation#file_location.uuid, FileLocation#file_location.space_id)};
get_context(#file_perm_changed_event{file_guid = FileGuid}) ->
    {file, FileGuid};
get_context(#file_removed_event{file_guid = FileGuid}) ->
    {file, FileGuid};
get_context(#file_renamed_event{top_entry = Entry}) ->
    {file, Entry#file_renamed_entry.old_guid};
get_context(_) ->
    undefined.


-spec get_reference_based_prefix(event:base() | event:type() | binary()) ->
    {ok, binary()} | {error, not_reference_based}.
get_reference_based_prefix(#event{type = Type}) ->
    get_reference_based_prefix(Type);
get_reference_based_prefix(#file_attr_changed_event{}) ->
    {ok, <<"file_attr_changed.">>};
get_reference_based_prefix(#file_location_changed_event{}) ->
    {ok, <<"file_location_changed.">>};
get_reference_based_prefix(#file_perm_changed_event{}) ->
    {ok, <<"file_perm_changed.">>};
get_reference_based_prefix(<<"file_attr_changed.", _/binary>>) ->
    {ok, <<"file_attr_changed.">>};
get_reference_based_prefix(<<"replica_status_changed.", _/binary>>) ->
    {ok, <<"replica_status_changed.">>};
get_reference_based_prefix(<<"file_location_changed.", _/binary>>) ->
    {ok, <<"file_location_changed.">>};
get_reference_based_prefix(<<"file_perm_changed.", _/binary>>) ->
    {ok, <<"file_perm_changed.">>};
get_reference_based_prefix(_) ->
    {error, not_reference_based}.

-spec get_attr_changed_reference_based_prefix() -> binary().
get_attr_changed_reference_based_prefix() ->
    <<"file_attr_changed.">>.

-spec get_replica_status_reference_based_prefix() -> binary().
get_replica_status_reference_based_prefix() ->
    <<"replica_status_changed.">>.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets routing keys for events where it bases on file's parent guid or routing
%% information. If file has links, several keys inside record can be returned.
%% @end
%%--------------------------------------------------------------------
-spec check_links_and_get_parent_connected_routing_key(binary(), fslogic_worker:file_guid(), routing_ctx()) ->
    subscription_manager:event_routing_keys().
check_links_and_get_parent_connected_routing_key(Prefix, FileGuid, #{file_ctx := FileCtx} = Ctx) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    % TODO VFS-7444 - maybe check type and do not get references for dir?
    {ok, References} = file_ctx:list_references_const(FileCtx),
    BasicAns = get_parent_connected_routing_key(Prefix, FileGuid, Ctx),
    AdditionalKeys = lists:foldl(fun(Uuid, Acc) ->
        try
            Guid = file_id:pack_guid(Uuid, SpaceId),
            AnsForGuid = get_parent_connected_routing_key(Prefix, Guid, undefined),
            [{{guid, Guid}, AnsForGuid#event_routing_keys.main_key} | Acc]
        catch
            Error:Reason ->
                % It is possible that some documents for additional keys are not found
                % (e.g. race with delete)
                ?debug("error getting parent connected key ~tp:~tp for uuid ~tp", [Error, Reason, Uuid]),
                Acc
        end
    end, [], References -- [file_ctx:get_logical_uuid_const(FileCtx)]),
    BasicAns#event_routing_keys{additional_keys = AdditionalKeys};
check_links_and_get_parent_connected_routing_key(Prefix, FileGuid, _) ->
    FileCtx = file_ctx:new_by_guid(FileGuid),
    check_links_and_get_parent_connected_routing_key(Prefix, FileGuid, #{file_ctx => FileCtx}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets routing key for events where it bases on file's parent guid or routing information.
%% @end
%%--------------------------------------------------------------------
-spec get_parent_connected_routing_key(binary(), fslogic_worker:file_guid(), routing_ctx()) ->
    subscription_manager:event_routing_keys().
get_parent_connected_routing_key(Prefix, FileGuid, #{file_ctx := FileCtx, parent := Parent}) ->
    case {Parent, file_ctx:is_space_dir_const(FileCtx)} of
        {undefined, _} -> % for user's dir parent is undefined (it has no parent)
            Uuid = file_id:guid_to_uuid(FileGuid),
            #event_routing_keys{file_ctx = FileCtx, main_key = <<Prefix/binary, Uuid/binary>>};
        {_, true} ->
            ParentUuid = file_id:guid_to_uuid(Parent),
            #event_routing_keys{
                file_ctx = FileCtx,
                main_key = <<Prefix/binary, ParentUuid/binary>>,
                space_id_filter = file_ctx:get_space_id_const(FileCtx)
            };
        _ ->
            Uuid = file_id:guid_to_uuid(Parent),
            #event_routing_keys{file_ctx = FileCtx, main_key = <<Prefix/binary, Uuid/binary>>}
    end;
get_parent_connected_routing_key(Prefix, FileGuid, #{file_ctx := FileCtx}) ->
    {ParentGuid, _} = file_tree:get_parent_guid_if_not_root_dir(FileCtx, undefined),
    get_parent_connected_routing_key(Prefix, FileGuid, #{file_ctx => FileCtx, parent => ParentGuid});
get_parent_connected_routing_key(Prefix, FileGuid, #{parent := Parent}) ->
    FileCtx = file_ctx:new_by_guid(FileGuid),
    get_parent_connected_routing_key(Prefix, FileGuid, #{file_ctx => FileCtx, parent => Parent});
get_parent_connected_routing_key(Prefix, FileGuid, _) ->
    FileCtx = file_ctx:new_by_guid(FileGuid),
    {ParentGuid, _} = file_tree:get_parent_guid_if_not_root_dir(FileCtx, undefined),
    get_parent_connected_routing_key(Prefix, FileGuid, #{file_ctx => FileCtx, parent => ParentGuid}).