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
-include("modules/monitoring/events.hrl").

%% API
-export([get_routing_key/2, get_stream_key/1, get_aggregation_key/1]).
-export([get_context/1, update_context/2]).

-type aggregation_key() :: term().
-type ctx() :: undefined | {file, file_ctx:ctx()}.

-export_type([aggregation_key/0, ctx/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a routing key that will be used to query globally cached event routing
%% table for subscribers interested in receiving this event.
%% @end
%%--------------------------------------------------------------------
-spec get_routing_key(Evt :: event:base() | event:type(), RoutingBase :: fslogic_worker:file_guid() | undefined) ->
    {ok, Key :: subscription_manager:key()} |
    {ok, Key :: subscription_manager:key(), SpaceIDFilter :: od_space:id()} | {error, session_only}.
get_routing_key(#event{type = Type}, RoutingBase) ->
    get_routing_key(Type, RoutingBase);
get_routing_key(#file_attr_changed_event{file_attr = FileAttr}, RoutingBase) ->
    get_routing_key_with_parent(<<"file_attr_changed.">>, FileAttr#file_attr.guid, RoutingBase);
get_routing_key(#file_location_changed_event{file_location = FileLocation}, _RoutingBase) ->
    FileGuid = file_id:pack_guid(
        FileLocation#file_location.uuid,
        FileLocation#file_location.space_id
    ),
    {ok, <<"file_location_changed.", FileGuid/binary>>};
get_routing_key(#file_perm_changed_event{file_guid = FileGuid}, _RoutingBase) ->
    {ok, <<"file_perm_changed.", FileGuid/binary>>};
get_routing_key(#file_removed_event{file_guid = FileGuid}, RoutingBase) ->
    get_routing_key_with_parent(<<"file_removed.">>, FileGuid, RoutingBase);
get_routing_key(#file_renamed_event{top_entry = Entry}, RoutingBase) ->
    get_routing_key_with_parent(<<"file_renamed.">>, Entry#file_renamed_entry.old_guid, RoutingBase);
get_routing_key(#quota_exceeded_event{}, _RoutingBase) ->
    {ok, <<"quota_exceeded">>};
get_routing_key(#helper_params_changed_event{storage_id = StorageId}, _RoutingBase) ->
    {ok, <<"helper_params_changed.", StorageId/binary>>};
get_routing_key(_, _) ->
    {error, session_only}.

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
    {file, file_ctx:new_by_guid(FileGuid)};
get_context(#file_written_event{file_guid = FileGuid}) ->
    {file, file_ctx:new_by_guid(FileGuid)};
get_context(#file_attr_changed_event{file_attr = FileAttr}) ->
    {file, file_ctx:new_by_guid(FileAttr#file_attr.guid)};
get_context(#file_location_changed_event{file_location = FileLocation}) ->
    FileGuid = file_id:pack_guid(
        FileLocation#file_location.uuid,
        FileLocation#file_location.space_id
    ),
    {file, file_ctx:new_by_guid(FileGuid)};
get_context(#file_perm_changed_event{file_guid = FileGuid}) ->
    {file, file_ctx:new_by_guid(FileGuid)};
get_context(#file_removed_event{file_guid = FileGuid}) ->
    {file, file_ctx:new_by_guid(FileGuid)};
get_context(#file_renamed_event{top_entry = Entry}) ->
    {file, file_ctx:new_by_guid(Entry#file_renamed_entry.old_guid)};
get_context(_) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% Updates the event context.
%% @end
%%--------------------------------------------------------------------
-spec update_context(Evt :: event:base() | event:type(), Ctx :: ctx()) ->
    NewEvt :: event:base() | event:type().
update_context(#event{type = Type} = Evt, Ctx) ->
    Evt#event{type = update_context(Type, Ctx)};
update_context(#file_read_event{} = Evt, {file, FileCtx}) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    Evt#file_read_event{file_guid = FileGuid};
update_context(#file_written_event{} = Evt, {file, FileCtx}) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    Evt#file_written_event{file_guid = FileGuid};
update_context(#file_attr_changed_event{file_attr = A} = Evt, {file, FileCtx}) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    Evt#file_attr_changed_event{file_attr = A#file_attr{guid = FileGuid}};
update_context(#file_location_changed_event{file_location = L} = Evt, {file, FileCtx}) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    Evt#file_location_changed_event{file_location = L#file_location{uuid = FileGuid}};
update_context(#file_perm_changed_event{} = Evt, {file, FileCtx}) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    Evt#file_perm_changed_event{file_guid = FileGuid};
update_context(#file_removed_event{} = Evt, {file, FileCtx}) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    Evt#file_removed_event{file_guid = FileGuid};
update_context(#file_renamed_event{top_entry = E} = Evt, {file, FileCtx}) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    Evt#file_renamed_event{top_entry = E#file_renamed_entry{old_guid = FileGuid}};
update_context(Evt, _Ctx) ->
    Evt.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets routing key for events where it bases on file's parent guid.
%% @end
%%--------------------------------------------------------------------
-spec get_routing_key_with_parent(binary(), fslogic_worker:file_guid(), fslogic_worker:file_guid() | undefined) ->
    {ok, Key :: subscription_manager:key()} |
    {ok, Key :: subscription_manager:key(), SpaceIDFilter :: od_space:id()} | {error, session_only}.
get_routing_key_with_parent(Base, FileGuid, undefined) ->
    FileCtx = file_ctx:new_by_guid(FileGuid),
    {ParentGuid, _} = file_ctx:get_parent_guid(FileCtx, undefined),
    case {ParentGuid, file_ctx:is_space_dir_const(FileCtx)} of
        {undefined, _} -> {ok, <<Base/binary, FileGuid/binary>>}; % user's dir
        {_, true} -> {ok, <<Base/binary, ParentGuid/binary>>, file_ctx:get_space_id_const(FileCtx)};
        _ -> {ok, <<Base/binary, ParentGuid/binary>>}
    end;
get_routing_key_with_parent(Base, FileGuid, ParentGUID) ->
    FileCtx = file_ctx:new_by_guid(FileGuid),
    case file_ctx:is_space_dir_const(FileCtx) of
        true -> {ok, <<Base/binary, ParentGUID/binary>>, file_ctx:get_space_id_const(FileCtx)};
        _ -> {ok, <<Base/binary, ParentGUID/binary>>}
    end.