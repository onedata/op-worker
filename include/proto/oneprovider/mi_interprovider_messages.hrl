%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Protocol messages for middleware interprovider communication.
%%% @end
%%%-------------------------------------------------------------------


-ifndef(MI_INTEPROVIDER_MESSAGES_HRL).
-define(MI_INTEPROVIDER_MESSAGES_HRL, 1).

-record(mi_interprovider_request, {
    file_guid :: file_id:file_guid(),
    operation :: middleware_worker:interprovider_operation()
}).

-record(mi_interprovider_response, {
    status :: ok | error,
    result :: middleware_worker:interprovider_result() | errors:error()
}).

%% Requests

-record(local_reg_file_distribution_get_request, {}).

-record(browse_local_time_dir_size_stats, {
    request :: ts_browse_request:record()
}).

-record(browse_local_current_dir_size_stats, {
    stat_names = [] :: [dir_stats_collection:stat_name()]
}).


%% Responses

-record(local_current_dir_size_stats, {
    stats :: dir_size_stats:current_stats()
}).

-endif.