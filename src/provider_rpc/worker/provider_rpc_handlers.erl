%%%-------------------------------------------------------------------
%%% @author Michał Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module routes provider RPC operations to corresponding handler modules.
%%% @end
%%%-------------------------------------------------------------------
-module(provider_rpc_handlers).
-author("Michał Stanisz").

-include("middleware/middleware.hrl").
-include("proto/oneprovider/provider_rpc_messages.hrl").

%% API
-export([execute/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec execute(file_ctx:ctx(), provider_rpc_worker:request()) -> 
    {ok, provider_rpc_worker:result()} | errors:error().
execute(FileCtx, #provider_reg_distribution_get_request{}) ->
    provider_reg_distribution:get(FileCtx);

execute(FileCtx, #provider_reg_storage_locations_get_request{}) ->
    provider_reg_distribution:get_storage_locations(FileCtx);

execute(FileCtx, #provider_dir_distribution_get_request{stats_request = StatsRequest}) ->
    provider_dir_distribution:get(FileCtx, StatsRequest);

execute(FileCtx, #provider_historical_dir_size_stats_browse_request{request = Request}) ->
    dir_size_stats:browse_historical_stats_collection(file_ctx:get_logical_guid_const(FileCtx), Request);

execute(FileCtx, #provider_qos_status_get_request{qos_entry_id = QosEntryId}) ->
    {ok, #provider_qos_status_get_result{status = qos_status:check_local(FileCtx, QosEntryId)}}.
