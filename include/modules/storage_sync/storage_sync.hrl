%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Macro definitions for storage_sync.
%%% @end
%%%-------------------------------------------------------------------

-include("global_definitions.hrl").

-define(STORAGE_SYNC_FILE_POOL_NAME, storage_sync_file_worker_pool).
-define(STORAGE_SYNC_DIR_POOL_NAME, storage_sync_dir_worker_pool).
-define(STORAGE_SYNC_FILE_WORKERS_NUM_KEY, storage_sync_file_workers_num).
-define(STORAGE_SYNC_DIR_WORKERS_NUM_KEY, storage_sync_dir_workers_num).

-define(FILES_IMPORT_TIMEOUT, timer:hours(24)).

-define(DIR_BATCH,
    begin
        {ok, _Batch} = application:get_env(?APP_NAME, dir_batch_size),
        _Batch
    end
).