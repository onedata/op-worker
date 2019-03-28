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
-export([get_spaces/1, get_indices/1]).
-export([is_in_harvester/2, is_harvested/2]).
-export([delete_entry/5]).
-export([submit_entry/6]).

% exported for CT tests
-export([]).


-define(CREATE_ENTRY(FileId), {create_entry, FileId}).
-define(DELETE_ENTRY(FileId), {delete_entry, FileId}).
-define(PAYLOAD_KEY(Type), <<Type/binary, "_payload">>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves harvester doc by given HarvesterId.
%% @end
%%--------------------------------------------------------------------
-spec get(od_harvester:id()) -> {ok, od_harvester:doc()} | gs_protocol:error().
get(HarvesterId) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_harvester, id = HarvesterId, aspect = instance, scope = private},
        subscribe = true
    }).


-spec get_spaces(od_harvester:doc()) -> {ok, [od_space:id()]}.
get_spaces(#document{value = #od_harvester{spaces = Spaces}}) ->
    {ok, Spaces}.

-spec get_indices(od_harvester:doc()) -> {ok, [od_harvester:index()]}.
get_indices(#document{value = #od_harvester{indices = Indices}}) ->
    {ok, Indices}.


%%--------------------------------------------------------------------
%% @doc
%% Removes entry for given HarvesterId and FileId in Onezone.
%% @end
%%--------------------------------------------------------------------
-spec delete_entry(od_harvester:id(), cdmi_id:objectid(), [od_harvester:index()],
    non_neg_integer(), non_neg_integer()) -> ok | gs_protocol:error().
delete_entry(HarvesterId, FileId, Indices, Seq, MaxSeq) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_harvester, id = HarvesterId,
            aspect = ?DELETE_ENTRY(FileId), scope = private
        },
        data = prepare_delete_payload(Indices, Seq, MaxSeq)
    }).

%%--------------------------------------------------------------------
%% @doc
%% Prepares payload and pushes entry with metadata for given
%% HarvesterId and FileId to Onezone.
%% @end
%%--------------------------------------------------------------------
-spec submit_entry(od_harvester:id(), cdmi_id:objectid(), gs_protocol:json_map(),
    [od_harvester:index()], non_neg_integer(), non_neg_integer()) -> ok | gs_protocol:error().
submit_entry(HarvesterId, FileId, JSON, Indices, Seq, MaxSeq) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_harvester, id = HarvesterId,
            aspect = ?CREATE_ENTRY(FileId), scope = private
        },
        data = prepare_create_payload(JSON, Indices, Seq, MaxSeq)
    }).

is_in_harvester(HarvesterId, IndexId) ->
    case harvester_logic:get(HarvesterId) of
        {ok, #document{value = #od_harvester{indices = Indices}}} ->
            lists:member(IndexId, Indices);
        _ ->
            false
    end.

is_harvested(HarvesterId, SpaceId) ->
    case harvester_logic:get(HarvesterId) of
        {ok, #document{value = #od_harvester{spaces = Spaces}}} ->
            lists:member(SpaceId, Spaces);
        _ ->
            false
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec prepare_create_payload(gs_protocol:json_map() ,[od_harvester:index()], non_neg_integer(),
    non_neg_integer()) -> gs_protocol:data() | gs_protocol:error().
prepare_create_payload(JSON, Indices, Seq, MaxSeq) ->
    GenPayload = prepare_generic_payload(Indices, Seq, MaxSeq),
    GenPayload#{
        <<"json">> => json_utils:encode(JSON)
    }.

-spec prepare_delete_payload([od_harvester:index()], non_neg_integer(),
    non_neg_integer()) -> gs_protocol:data() | gs_protocol:error().
prepare_delete_payload(Indices, Seq, MaxSeq) ->
    prepare_generic_payload(Indices, Seq, MaxSeq).

-spec prepare_generic_payload([od_harvester:index()], non_neg_integer(),
    non_neg_integer()) -> gs_protocol:data() | gs_protocol:error().
prepare_generic_payload(Indices, Seq, MaxSeq) ->
    #{
        <<"indices">> => Indices,
        <<"seq">> => Seq,
        <<"maxSeq">> => MaxSeq
    }.
