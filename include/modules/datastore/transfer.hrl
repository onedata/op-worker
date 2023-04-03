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

-ifndef(TRANSFER_HRL).
-define(TRANSFER_HRL, 1).

-include("global_definitions.hrl").

-define(SCHEDULED_STATUS, scheduled).
-define(ENQUEUED_STATUS, enqueued).
-define(ACTIVE_STATUS, active).
-define(COMPLETED_STATUS, completed).
-define(ABORTING_STATUS, aborting).
-define(FAILED_STATUS, failed).
-define(CANCELLED_STATUS, cancelled).
-define(SKIPPED_STATUS, skipped).
-define(EVICTING_STATUS, evicting).
-define(REPLICATING_STATUS, replicating).

-define(WAITING_TRANSFERS_STATE, <<"waiting">>).
-define(ONGOING_TRANSFERS_STATE, <<"ongoing">>).
-define(ENDED_TRANSFERS_STATE, <<"ended">>).

-define(WAITING_TRANSFERS_KEY, <<"SCHEDULED_TRANSFERS_KEY">>).
-define(ONGOING_TRANSFERS_KEY, <<"CURRENT_TRANSFERS_KEY">>).
-define(ENDED_TRANSFERS_KEY, <<"PAST_TRANSFERS_KEY">>).

-define(JOB_TRANSFERS_TYPE, <<"job">>).
-define(ON_THE_FLY_TRANSFERS_TYPE, <<"onTheFly">>).
-define(ALL_TRANSFERS_TYPE, <<"all">>).

% Time windows and lengths of histograms. They offer one extra, historical
% value, as the leading one is not yet measured - this way there are always
% full measurements from the whole histogram span.
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

-define(MINUTE_PERIOD, <<"minute">>).
-define(HOUR_PERIOD, <<"hour">>).
-define(DAY_PERIOD, <<"day">>).
-define(MONTH_PERIOD, <<"month">>).


-define(REPLICATION_CONTROLLER, replication_controller).
-define(REPLICATION_CONTROLLERS_POOL, replication_controllers_pool).
-define(REPLICATION_CONTROLLERS_NUM, op_worker:get_env(replication_controllers_num, 10)).

-endif.