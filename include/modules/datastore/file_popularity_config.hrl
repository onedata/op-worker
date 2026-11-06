%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This modules contains macros used in file-popularity modules.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(FILE_POPULARITY_CONFIG_HRL).
-define(FILE_POPULARITY_CONFIG_HRL, 1).

-include("global_definitions.hrl").

-define(DEFAULT_LAST_OPEN_HOUR_WEIGHT,
    op_worker:get_env(default_last_open_hour_weight, 1.0)).

-define(DEFAULT_AVG_OPEN_COUNT_PER_DAY_WEIGHT,
    op_worker:get_env(default_avg_open_count_per_day_weight, 20.0)).

-define(DEFAULT_MAX_AVG_OPEN_COUNT_PER_DAY,
    op_worker:get_env(default_max_avg_open_count_per_day, 100.0)).

-endif.