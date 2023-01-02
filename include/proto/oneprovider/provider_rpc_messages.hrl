%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Protocol messages for provider RPC communication.
%%% @end
%%%-------------------------------------------------------------------


-ifndef(PROVIDER_RPC_MESSAGES_HRL).
-define(PROVIDER_RPC_MESSAGES_HRL, 1).


%%%===================================================================
%%% Generic provider RPC messages
%%%===================================================================

-record(provider_rpc_call, {
    file_guid :: file_id:file_guid(),
    request :: provider_rpc:request()
}).

-record(provider_rpc_response, {
    status :: ok | error,
    result :: provider_rpc:result() | errors:error()
}).


%%%===================================================================
%%% Provider RPC requests
%%%===================================================================

-record(provider_reg_distribution_get_request, {}).

-record(provider_reg_storage_location_get_request, {}).

-record(provider_historical_dir_size_stats_browse_request, {
    request :: ts_browse_request:record()
}).

-record(provider_current_dir_size_stats_browse_request, {
    stat_names = [] :: [dir_stats_collection:stat_name()]
}).


%%%===================================================================
%%% Provider RPC responses
%%%===================================================================

-record(provider_current_dir_size_stats_browse_result, {
    stats :: dir_size_stats:current_stats()
}).

-record(provider_reg_storage_location_result, {
    locations :: data_distribution:locations_per_storage()
}).

-endif.
