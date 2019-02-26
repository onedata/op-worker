%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_harvester records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by Onezone.
%%% NOTE: This is the only valid way to interact with od_harvester records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(harvester_logic).
-author("Jakub Kudzia").

-include("graph_sync/provider_graph_sync.hrl").
-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([get/1]).
-export([submit_entry/3, delete_entry/2]).
-export([prepare_payload/2]).

-define(ENTRY(FileId), {entry, FileId}).
-define(PAYLOAD_KEY(Type), <<Type/binary, "_payload">>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves harvester doc by given SpaceId.
%% @end
%%--------------------------------------------------------------------
-spec get(od_space:id()) -> {ok, od_space:doc()} | gs_protocol:error().
get(HarvesterId) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_harvester, id = HarvesterId, aspect = instance, scope = protected},
        subscribe = true
    }).

%%--------------------------------------------------------------------
%% @doc
%% Pushes entry with metadata for given HarvesterId and FileId to Onezone.
%% @end
%%--------------------------------------------------------------------
-spec submit_entry(od_harvester:id(), cdmi_id:objectid(), gs_protocol:data()) ->
    ok | gs_protocol:error().
submit_entry(HarvesterId, FileId, Data) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_harvester, id = HarvesterId,
            aspect = ?ENTRY(FileId), scope = private},
        data = Data
    }).

%%--------------------------------------------------------------------
%% @doc
%% Removes entry for given HarvesterId and FileId in Onezone.
%% @end
%%--------------------------------------------------------------------
-spec delete_entry(od_harvester:id(), cdmi_id:objectid()) -> ok | gs_protocol:error().
delete_entry(HarvesterId, FileId) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_harvester, id = HarvesterId, aspect = ?ENTRY(FileId),
            scope = private}
    }).

-spec prepare_payload(maps:map(), od_harvester:id()) ->
    {ok, gs_protocol:data()} | gs_protocol:error().
prepare_payload(JSON, HarvesterId) ->
    case get_and_validate_type(JSON, HarvesterId) of
        {ok, Type} ->
            Payload = json_utils:encode(#{
                <<"type">> => Type,
                ?PAYLOAD_KEY(Type) => JSON
            }),
            {ok, #{<<"payload">> => Payload}};
        Error ->
            Error
    end.
%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec entry_type_field(od_harvester:id()) -> {ok, od_harvester:entry_type()} | {error, term()}.
entry_type_field(HarvesterId) ->
    case harvester_logic:get(HarvesterId) of
        {ok, #document{value = #od_harvester{entry_type_field = EntryTypeField}}} ->
            {ok,EntryTypeField};
        {error, _} = Error ->
            Error
    end.

-spec default_entry_type(od_harvester:id()) ->
    {ok, undefined | od_harvester:entry_type()} | {error, term()}.
default_entry_type(HarvesterId) ->
    case harvester_logic:get(HarvesterId) of
        {ok, #document{value = #od_harvester{default_entry_type = DefaultEntryType}}} ->
            {ok, DefaultEntryType};
        {error, _} = Error ->
            Error
    end.


-spec accepted_entry_types(od_harvester:id()) ->
    {ok, [od_harvester:entry_type()]} | {error, term()}.
accepted_entry_types(HarvesterId) ->
    case harvester_logic:get(HarvesterId) of
        {ok, #document{value = #od_harvester{accepted_entry_types = AcceptedEntryTypes}}} ->
            {ok, AcceptedEntryTypes};
        {error, _} = Error ->
            Error
    end.

-spec get_and_validate_type(maps:map(), od_harvester:id()) ->
    {ok, od_harvester:entry_type()} | {error, term()}.
get_and_validate_type(JSON, HarvesterId) ->
    {ok, AcceptedEntryTypes} = accepted_entry_types(HarvesterId),
    case get_type(JSON, HarvesterId) of
        undefined ->
            {error, undefined_type};
        Type ->
            case lists:member(Type, AcceptedEntryTypes) of
                true -> {ok, Type};
                false -> {error, type_not_accepted}
            end
    end.

-spec get_type(maps:map(), od_harvester:id()) -> od_harvester:entry_type() | undefined.
get_type(JSON, HarvesterId) ->
    {ok, EntryTypeField} = entry_type_field(HarvesterId),
    {ok, DefaultEntryType} = default_entry_type(HarvesterId),
    maps:get(EntryTypeField, JSON, DefaultEntryType).