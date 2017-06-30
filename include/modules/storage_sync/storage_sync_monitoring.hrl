%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Definitions of macros and records for storage_sync_monitoring
%%% @end
%%%-------------------------------------------------------------------

-record(metric_info, {
    metric :: list(),
    values = [] :: list(),
    timestamp :: calendar:datetime()
}).

-define(DEFAULT_RESOLUTION, 12).