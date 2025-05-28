%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for performing operations on directory file distribution
%%% in scope of single provider.
%%% @end
%%%--------------------------------------------------------------------
-module(provider_dir_distribution).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/data_distribution.hrl").
-include("proto/oneprovider/provider_rpc_messages.hrl").

%% API
-export([get/2]).

-type current_dir_size_stats_result() :: #provider_current_dir_size_stats_browse_result{}.
-type provider_dir_distribution_get_result() :: #provider_dir_distribution_get_result{}.
-type provider_dir_distribution_get_request() :: #provider_dir_distribution_get_request{}.

-export_type([current_dir_size_stats_result/0, provider_dir_distribution_get_result/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec get(file_ctx:ctx(), provider_dir_distribution_get_request()) -> {ok, data_distribution:provider_reg_distribution()}.
get(FileCtx, #provider_dir_distribution_get_request{
    stats_request = #provider_current_dir_size_stats_browse_request{stat_names = StatNames}
}) ->
    StorageLocation = case file_ctx:get_dir_location_doc_const(FileCtx) of
        undefined -> undefined;
        DirLocationDoc -> dir_location:get_storage_file_id(DirLocationDoc)
    end,
    {StorageId, _} = file_ctx:get_storage_id(FileCtx),
    {ok, #provider_dir_distribution_get_result{
        current_dir_size_stats = get_stats(FileCtx, StatNames),
        locations_per_storage = #{StorageId => StorageLocation}
    }}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_stats(file_ctx:ctx(), [dir_stats_collection:stat_name()]) -> current_dir_size_stats_result().
get_stats(_FileCtx, []) ->
    #provider_current_dir_size_stats_browse_result{stats = #{}};
get_stats(FileCtx, StatNames) ->
    case dir_size_stats:get_stats(file_ctx:get_logical_guid_const(FileCtx), StatNames) of
        {ok, Stats} -> #provider_current_dir_size_stats_browse_result{stats = Stats};
        {error, _} = Error -> throw(Error)
    end.
