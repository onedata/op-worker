%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module providing functions for finding data in external replicas.
%%% @end
%%%--------------------------------------------------------------------
-module(replica_finder).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

%% API
-export([get_blocks_for_sync/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if given blocks are synchronized in local file locations. If not,
%% returns list with tuples informing where to fetch data: {ProviderId, BlocksToFetch}
%% @end
%%--------------------------------------------------------------------
-spec get_blocks_for_sync([file_location:doc()], fslogic_blocks:blocks()) ->
    [{oneprovider:id(), fslogic_blocks:blocks()}].
get_blocks_for_sync(_, []) ->
    [];
get_blocks_for_sync(Locations, Blocks) ->
    LocalProviderId = oneprovider:get_provider_id(),
    LocalLocations = [Loc || Loc = #document{value = #file_location{provider_id = Id}} <- Locations, Id =:= LocalProviderId],
    RemoteLocations = Locations -- LocalLocations,
    [LocalBlocks] = [LocalBlocks || #document{value = #file_location{blocks = LocalBlocks}} <- LocalLocations], %todo allow multi location
    BlocksToSync = fslogic_blocks:invalidate(Blocks, LocalBlocks),

    case BlocksToSync of
        [] -> [];
        _ ->
            [Loc | _] = RemoteLocations,
            RemoteProvider = Loc#document.value#file_location.provider_id,
            [{RemoteProvider, BlocksToSync}] %todo handle multiprovider case
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================