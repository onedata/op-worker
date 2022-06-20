%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for performing operations on local regular file distribution.
%%% @end
%%%--------------------------------------------------------------------
-module(local_reg_file_distribution).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/file_distribution.hrl").

%% API
-export([get/1]).


%%%===================================================================
%%% API
%%%===================================================================

-spec get(file_ctx:ctx()) -> file_distribution:provider_reg_distribution().
get(FileCtx0) ->
    {FileSize, FileCtx1} = file_ctx:get_file_size(FileCtx0),
    {Location, FileCtx2} = file_ctx:get_local_file_location_doc(FileCtx1),
    StorageDistribution = case Location of
        #document{value = #file_location{storage_id = StorageId}} = FL ->
            #storage_reg_distribution{
                storage_id = StorageId,
                blocks = fslogic_location_cache:get_blocks(FL)
            };
        undefined ->
            SpaceId = file_ctx:get_space_id_const(FileCtx2), 
            {ok, StorageId} = space_logic:get_local_supporting_storage(SpaceId),
            #storage_reg_distribution{
                storage_id = StorageId,
                blocks = []
            }
    end,
    #provider_reg_distribution{
        logical_size = FileSize,
        blocks_per_storage = [StorageDistribution]
    }.
