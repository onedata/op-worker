%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% This file contains definitions of macros used in space_sync_worker.
%%% @end
%%%-------------------------------------------------------------------

%% Interval between successive check strategies.
-define(SPACE_STRATEGIES_CHECK_INTERVAL, check_strategies_interval).

-define(STORAGE_IMPORT_POOL_NAME, storage_import_worker_pool).
-define(STORAGE_IMPORT_WORKERS_NUM, storage_import_workers_num).
-define(STORAGE_UPDATE_POOL_NAME, storage_update_worker_pool).
-define(STORAGE_UPDATE_WORKERS_NUM, storage_update_workers_num).
-define(GENERIC_STRATEGY_POOL_NAME, generic_strategy_worker_pool).
-define(GENERIC_STRATEGY_WORKERS_NUM, generic_strategy_workers_num).

-define(SPACE_SYNC_WORKER_POOLS, [
    {?STORAGE_IMPORT_POOL_NAME, ?STORAGE_IMPORT_WORKERS_NUM},
    {?STORAGE_UPDATE_POOL_NAME, ?STORAGE_UPDATE_WORKERS_NUM},
    {?GENERIC_STRATEGY_POOL_NAME, ?GENERIC_STRATEGY_WORKERS_NUM}
]).