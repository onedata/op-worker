%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros used in stress tests of harvesting.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(HARVESTING_STRESS_TEST_UTILS_HRL).
-define(HARVESTING_STRESS_TEST_UTILS_HRL, 1).

-define(SPACE_ID, <<"space1">>).
-define(HARVESTER_ID, <<"harvester1">>).
-define(INDEX_ID, <<"index1">>).

-define(HARVEST_METADATA(BatchSize), {harvest_metadata, BatchSize}).

-define(TIMEOUT, timer:minutes(30)).

-endif.