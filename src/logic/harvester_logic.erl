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
-export([delete_entry/5]).
-export([submit_entry/6]).

-define(SUBMIT_ENTRY(FileId), {submit_entry, FileId}).
-define(DELETE_ENTRY(FileId), {delete_entry, FileId}).

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
%% Prepares payload and pushes entry with metadata for given
%% HarvesterId, FileId and Indices to Onezone.
%% Seq and MaxSeq are sent to allow for tracking progress of harvesting.
%% Call to onezone returns list of Indices for which request failed and
%% must be repeated.
%% @end
%%--------------------------------------------------------------------
-spec submit_entry(od_harvester:id(), cdmi_id:objectid(), gs_protocol:json_map(),
    [od_harvester:index()], non_neg_integer(), non_neg_integer()) ->
    {ok, #{binary() => [od_harvester:index()]}} | gs_protocol:error().
submit_entry(HarvesterId, FileId, JSON, Indices, Seq, MaxSeq) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_harvester, id = HarvesterId,
            aspect = ?SUBMIT_ENTRY(FileId), scope = private
        },
        data = submit_payload(JSON, Indices, Seq, MaxSeq)
    }).

%%--------------------------------------------------------------------
%% @doc
%% Removes entry for given HarvesterId, FileId and Indices in Onezone.
%% Seq and MaxSeq are sent to allow for tracking progress of harvesting.
%% Call to onezone returns list of Indices for which request failed and
%% must be repeated.
%% @end
%%--------------------------------------------------------------------
-spec delete_entry(od_harvester:id(), cdmi_id:objectid(), [od_harvester:index()],
    non_neg_integer(), non_neg_integer()) -> ok | gs_protocol:error().
delete_entry(HarvesterId, FileId, Indices, Seq, MaxSeq) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create, % intentional !!!, GS does not support sending data in `delete`
        gri = #gri{type = od_harvester, id = HarvesterId,
            aspect = ?DELETE_ENTRY(FileId), scope = private
        },
        data = delete_payload(Indices, Seq, MaxSeq)
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec submit_payload(gs_protocol:json_map() ,[od_harvester:index()], non_neg_integer(),
    non_neg_integer()) -> gs_protocol:data() | gs_protocol:error().
submit_payload(JSON, Indices, Seq, MaxSeq) ->
    EntryPayload = entry_payload(Indices, Seq, MaxSeq),
    EntryPayload#{
        <<"json">> => json_utils:encode(JSON)
    }.

-spec delete_payload([od_harvester:index()], non_neg_integer(),
    non_neg_integer()) -> gs_protocol:data() | gs_protocol:error().
delete_payload(Indices, Seq, MaxSeq) ->
    entry_payload(Indices, Seq, MaxSeq).

-spec entry_payload([od_harvester:index()], non_neg_integer(),
    non_neg_integer()) -> gs_protocol:data() | gs_protocol:error().
entry_payload(Indices, Seq, MaxSeq) ->
    #{
        <<"indices">> => Indices,
        <<"seq">> => Seq,
        <<"maxSeq">> => MaxSeq
    }.
