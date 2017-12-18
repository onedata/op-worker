%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used by transfer module.
%%% @end
%%%-------------------------------------------------------------------

-include("global_definitions.hrl").

-define(SUCCESSFUL_TRANSFERS_KEY, <<"SUCCESSFUL_TRANSFERS_KEY">>).
-define(FAILED_TRANSFERS_KEY, <<"FAILED_TRANSFERS_KEY">>).
-define(UNFINISHED_TRANSFERS_KEY, <<"UNFINISHED_TRANSFERS_KEY">>).

% Time windows and lengths of histograms. They offer one extra, historical value,
% as the leading one is not yet measured - this way there are always full
% measurements from the whole histogram span.
-define(FIVE_SEC_TIME_WINDOW, 5).
-define(MIN_HIST_LENGTH, 13).

-define(MIN_TIME_WINDOW, 60).
-define(HOUR_HIST_LENGTH, 61).

-define(HOUR_TIME_WINDOW, 3600).
-define(DAY_HIST_LENGTH, 25).

-define(DAY_TIME_WINDOW, 86400).
-define(MONTH_HIST_LENGTH, 31).

-define(REPLICATION_POOL, replication_pool).
-define(REPLICATION_POOL_SIZE, application:get_env(?APP_NAME, replication_pool_size, 10)).