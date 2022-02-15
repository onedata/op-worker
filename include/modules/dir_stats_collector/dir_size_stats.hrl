%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros used by dir_size_stats collection.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DIR_SIZE_STATS_HRL).
-define(DIR_SIZE_STATS_HRL, 1).


%%%===================================================================
%%% Names of statistics.
%%%===================================================================

-define(REG_FILE_AND_LINK_COUNT, <<"reg_file_and_link_count">>).
-define(DIR_COUNT, <<"dir_count">>).
-define(TOTAL_SIZE, <<"total_size">>).
-define(SIZE_ON_STORAGE(StorageId), <<"size_on_storage_", StorageId/binary>>).


%%%===================================================================
%%% Metric ids for each statistic.
%%%===================================================================

-define(MINUTE_METRIC, <<"minute">>).
-define(HOUR_METRIC, <<"hour">>).
-define(DAY_METRIC, <<"day">>).
-define(MONTH_METRIC, <<"month">>).


-endif.