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

-define(SCHEDULED_TRANSFERS_KEY, <<"SCHEDULED_TRANSFERS_KEY">>).
-define(CURRENT_TRANSFERS_KEY, <<"CURRENT_TRANSFERS_KEY">>).
-define(PAST_TRANSFERS_KEY, <<"PAST_TRANSFERS_KEY">>).

% Time windows and lengths of histograms. They offer one extra, historical value,
% as the leading one is not yet measured - this way there are always full
% measurements from the whole histogram span.
% Speed charts avoid fluctuation of charts, they are generated with 30s delay.
% For this it is required that minute bytes histogram had additional 6 slots.
% The same goes for other stats types albeit required additional slots are smaller.
-define(FIVE_SEC_TIME_WINDOW, 5).
-define(MIN_HIST_LENGTH, 19).
-define(MIN_SPEED_HIST_LENGTH, 13).

-define(MIN_TIME_WINDOW, 60).
-define(HOUR_HIST_LENGTH, 63).
-define(HOUR_SPEED_HIST_LENGTH, 62).

-define(HOUR_TIME_WINDOW, 3600).
-define(DAY_HIST_LENGTH, 27).
-define(DAY_SPEED_HIST_LENGTH, 26).

-define(DAY_TIME_WINDOW, 86400).
-define(MONTH_HIST_LENGTH, 33).
-define(MONTH_SPEED_HIST_LENGTH, 32).

-define(MINUTE_STAT_TYPE, <<"minute">>).
-define(HOUR_STAT_TYPE, <<"hour">>).
-define(DAY_STAT_TYPE, <<"day">>).
-define(MONTH_STAT_TYPE, <<"month">>).

-define(TRANSFER_WORKERS_POOL, transfer_workers_pool).
-define(TRANSFER_WORKERS_NUM, application:get_env(?APP_NAME, transfer_workers_num, 50)).

-define(TRANSFER_CONTROLLERS_POOL, transfer_controllers_pool).
-define(TRANSFER_CONTROLLERS_NUM, application:get_env(?APP_NAME, transfer_controllers_num, 10)).

-define(INVALIDATION_WORKERS_POOL, invalidation_workers_pool).
-define(INVALIDATION_WORKERS_NUM, application:get_env(?APP_NAME, invalidation_workers_num, 10)).
