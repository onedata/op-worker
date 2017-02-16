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
-export([get_routing_key/1, get_stream_key/1, get_aggregation_key/1]).
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
-spec get_routing_key(Evt :: event:base() | event:type()) ->
    {ok, Key :: event_router:key()} | {error, session_only}.
get_routing_key(#event{type = Type}) ->
    get_routing_key(Type);
get_routing_key(#file_attr_changed_event{file_attr = FileAttr}) ->
    {ok, <<"file_attr_changed.", (FileAttr#file_attr.uuid)/binary>>};
get_routing_key(#file_location_changed_event{file_location = FileLocation}) ->
    {ok, <<"file_location_changed.", (FileLocation#file_location.uuid)/binary>>};
get_routing_key(#file_perm_changed_event{file_uuid = FileGuid}) ->
    {ok, <<"file_perm_changed.", FileGuid/binary>>};
get_routing_key(#file_removed_event{file_uuid = FileGuid}) ->
    {ok, <<"file_removed.", FileGuid/binary>>};
get_routing_key(#file_renamed_event{top_entry = Entry}) ->
    {ok, <<"file_renamed.", (Entry#file_renamed_entry.old_uuid)/binary>>};
get_routing_key(#quota_exceeded_event{}) ->
    {ok, <<"quota_exceeded">>};
get_routing_key(_) ->
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
get_aggregation_key(#file_read_event{file_uuid = FileGuid}) ->
    FileGuid;
get_aggregation_key(#file_written_event{file_uuid = FileGuid}) ->
    FileGuid;
get_aggregation_key(#file_attr_changed_event{file_attr = FileAttr}) ->
    FileAttr#file_attr.uuid;
get_aggregation_key(#file_location_changed_event{file_location = FileLocation}) ->
    FileLocation#file_location.uuid;
get_aggregation_key(#file_perm_changed_event{file_uuid = FileGuid}) ->
    FileGuid;
get_aggregation_key(#file_removed_event{file_uuid = FileGuid}) ->
    FileGuid;
get_aggregation_key(#file_renamed_event{top_entry = Entry}) ->
    Entry#file_renamed_entry.old_uuid;
get_aggregation_key(#quota_exceeded_event{}) ->
    <<>>;
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
get_context(#file_read_event{file_uuid = FileGuid}) ->
    {file, file_ctx:new_by_guid(FileGuid)};
get_context(#file_written_event{file_uuid = FileGuid}) ->
    {file, file_ctx:new_by_guid(FileGuid)};
get_context(#file_attr_changed_event{file_attr = FileAttr}) ->
    {file, file_ctx:new_by_guid(FileAttr#file_attr.uuid)};
get_context(#file_location_changed_event{file_location = FileLocation}) ->
    {file, file_ctx:new_by_guid(FileLocation#file_location.uuid)};
get_context(#file_perm_changed_event{file_uuid = FileGuid}) ->
    {file, file_ctx:new_by_guid(FileGuid)};
get_context(#file_removed_event{file_uuid = FileGuid}) ->
    {file, file_ctx:new_by_guid(FileGuid)};
get_context(#file_renamed_event{top_entry = Entry}) ->
    {file, file_ctx:new_by_guid(Entry#file_renamed_entry.old_uuid)};
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
    Evt#file_read_event{file_uuid = FileGuid};
update_context(#file_written_event{} = Evt, {file, FileCtx}) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    Evt#file_written_event{file_uuid = FileGuid};
update_context(#file_attr_changed_event{file_attr = A} = Evt, {file, FileCtx}) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    Evt#file_attr_changed_event{file_attr = A#file_attr{uuid = FileGuid}};
update_context(#file_location_changed_event{file_location = L} = Evt, {file, FileCtx}) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    Evt#file_location_changed_event{file_location = L#file_location{uuid = FileGuid}};
update_context(#file_perm_changed_event{} = Evt, {file, FileCtx}) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    Evt#file_perm_changed_event{file_uuid = FileGuid};
update_context(#file_removed_event{} = Evt, {file, FileCtx}) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    Evt#file_removed_event{file_uuid = FileGuid};
update_context(#file_renamed_event{top_entry = E} = Evt, {file, FileCtx}) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    Evt#file_renamed_event{top_entry = E#file_renamed_entry{old_uuid = FileGuid}};
update_context(Evt, _Ctx) ->
    Evt.