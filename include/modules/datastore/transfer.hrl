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

-define(WAITING_TRANSFERS_KEY, <<"SCHEDULED_TRANSFERS_KEY">>).
-define(ONGOING_TRANSFERS_KEY, <<"CURRENT_TRANSFERS_KEY">>).
-define(ENDED_TRANSFERS_KEY, <<"PAST_TRANSFERS_KEY">>).

-define(JOB_TRANSFERS_TYPE, <<"job">>).
-define(ON_THE_FLY_TRANSFERS_TYPE, <<"onTheFly">>).
-define(ALL_TRANSFERS_TYPE, <<"all">>).

% Time windows and lengths of histograms. They offer one extra, historical value,
% as the leading one is not yet measured - this way there are always full
% measurements from the whole histogram span.
% To avoid fluctuation of speed charts, they are generated with 30s delay.
% For this it is required that minute bytes histogram has additional 6 slots.
% As for other stats types 1 additional slot is enough.
-define(FIVE_SEC_TIME_WINDOW, 5).
-define(MIN_HIST_LENGTH, 19).
-define(MIN_SPEED_HIST_LENGTH, 13).

-define(MIN_TIME_WINDOW, 60).
-define(HOUR_HIST_LENGTH, 62).
-define(HOUR_SPEED_HIST_LENGTH, 61).

-define(HOUR_TIME_WINDOW, 3600).
-define(DAY_HIST_LENGTH, 26).
-define(DAY_SPEED_HIST_LENGTH, 25).

-define(DAY_TIME_WINDOW, 86400).
-define(MONTH_HIST_LENGTH, 32).
-define(MONTH_SPEED_HIST_LENGTH, 31).

-define(MINUTE_STAT_TYPE, <<"minute">>).
-define(HOUR_STAT_TYPE, <<"hour">>).
-define(DAY_STAT_TYPE, <<"day">>).
-define(MONTH_STAT_TYPE, <<"month">>).


-define(TRANSFER_WORKER, transfer_worker).
-define(TRANSFER_WORKERS_POOL, transfer_workers_pool).
-define(TRANSFER_WORKERS_NUM, application:get_env(?APP_NAME, transfer_workers_num, 50)).

-define(TRANSFER_CONTROLLER, transfer_controller).
-define(TRANSFER_CONTROLLERS_POOL, transfer_controllers_pool).
-define(TRANSFER_CONTROLLERS_NUM, application:get_env(?APP_NAME, transfer_controllers_num, 10)).

-define(INVALIDATION_WORKER, invalidation_worker).
-define(INVALIDATION_WORKERS_POOL, invalidation_workers_pool).
-define(INVALIDATION_WORKERS_NUM, application:get_env(?APP_NAME, invalidation_workers_num, 10)).

-define(REPLICA_DELETION_WORKER, replica_deletion_worker).
-define(REPLICA_DELETION_WORKERS_POOL, replica_deletion_workers_pool).
-define(REPLICA_DELETION_WORKERS_NUM,
    application:get_env(?APP_NAME, replica_deletion_workers_num, 10)).
