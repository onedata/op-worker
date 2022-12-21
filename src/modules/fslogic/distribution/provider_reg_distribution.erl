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
-include("modules/fslogic/file_distribution.hrl").

%% API
-export([get/1]).


%%%===================================================================
%%% API
%%%===================================================================

-spec get(file_ctx:ctx()) -> {ok, file_distribution:provider_reg_distribution()}.
get(FileCtx0) ->
    {FileSize, FileCtx1} = file_ctx:get_file_size(FileCtx0),
    {Location, FileCtx2} = file_ctx:get_local_file_location_doc(FileCtx1),
    StorageDistribution = case Location of
        #document{deleted = false, value = #file_location{storage_id = StorageId}} = FL ->
            #{StorageId => fslogic_location_cache:get_blocks(FL)};
        _ ->
            SpaceId = file_ctx:get_space_id_const(FileCtx2), 
            {ok, StorageId} = space_logic:get_local_supporting_storage(SpaceId),
            #{StorageId => []}
    end,
    {ok, #provider_reg_distribution_get_result{
        logical_size = FileSize,
        blocks_per_storage = StorageDistribution
    }}.
