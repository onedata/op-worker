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
%%% Harvester record is associated with an  external entity that is responsible
%%% for collecting and processing files' metadata, stored in
%%% custom_metadata model.
%%% Harvester can collect metadata from many spaces.
%%% Harvester can handle many different metadata schemas, in such case,
%%% many indices should be declared, each associated with separate schema.
%%% Harvesting progress is tracked per triple {HarvesterId, SpaceId, IndexId}
%%% which allows to dynamically add/delete indices.
%%% All metadata changes from given spaces are submitted to all indices,
%%% it is harvester's responsibility to accept/reject suitable schemas.
%%% harvesting_stream processes are responsible for collecting metadata changes
%%% and pushing them to Onezone.
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

-export([get/1, get_spaces/1, get_indices/1]).

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

-spec get_indices(od_harvester:doc() |od_harvester:record() | od_harvester:id()) ->
    {ok, [od_harvester:index()]} | gs_protocol:error().
get_indices(#document{value = HarvesterRecord}) ->
    get_indices(HarvesterRecord);
get_indices(#od_harvester{indices = Indices}) ->
    {ok, Indices};
get_indices(HarvesterId) ->
    case harvester_logic:get(HarvesterId) of
        {ok, Doc} ->
            get_indices(Doc);
        {error, _} = Error ->
            ?error("harvester_logic:get_indices(~p) failed due to ~p", [HarvesterId, Error]),
            Error
    end.