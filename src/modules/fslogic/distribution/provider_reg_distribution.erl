%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for performing operations on regular file distribution 
%%% in scope of single provider.
%%% @end
%%%--------------------------------------------------------------------
-module(provider_reg_distribution).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/data_distribution.hrl").
-include("proto/oneprovider/provider_rpc_messages.hrl").

%% API
-export([get/1, get_storage_locations/1]).

-compile([{no_auto_import, [get/1]}]).

-type provider_reg_storage_location_result() :: #provider_reg_storage_locations_result{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec get(file_ctx:ctx()) -> {ok, data_distribution:provider_reg_distribution()}.
get(FileCtx0) ->
    {FileSize, FileCtx1} = file_ctx:get_file_size(FileCtx0),
    {Location, FileCtx2} = file_ctx:get_local_file_location_doc(FileCtx1),
    {StorageDistribution, StorageLocations} = case Location of
        #document{value = #file_location{storage_file_created = true} = FL} = Doc ->
            #file_location{storage_id = StorageId, file_id = FileId} = FL,
            {#{StorageId => fslogic_location_cache:get_blocks(Doc)}, #{StorageId => FileId}};
        _ ->
            SpaceId = file_ctx:get_space_id_const(FileCtx2), 
            {ok, StorageId} = space_logic:get_local_supporting_storage(SpaceId),
            {#{StorageId => []}, #{StorageId => undefined}}
    end,
    {ok, #provider_reg_distribution_get_result{
        virtual_size = FileSize,
        blocks_per_storage = StorageDistribution,
        locations_per_storage = StorageLocations
    }}.


-spec get_storage_locations(file_ctx:ctx()) -> {ok, provider_reg_storage_location_result()}.
get_storage_locations(FileCtx) ->
    {ok, #provider_reg_distribution_get_result{
        locations_per_storage = LocationsPerStorage
    }} = get(FileCtx),
    {ok, #provider_reg_storage_locations_result{locations_per_storage = LocationsPerStorage}}.
