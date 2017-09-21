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
-define(STORAGE_SYNC_FILE_WORKERS_NUM, application:get_env(?APP_NAME, storage_sync_file_workers_num, 8)).
-define(STORAGE_SYNC_DIR_WORKERS_NUM, application:get_env(?APP_NAME, storage_sync_dir_workers_num, 8)).

-define(FILES_IMPORT_TIMEOUT, timer:hours(24)).

-define(DIR_BATCH,
    begin
        {ok, __Batch} = application:get_env(?APP_NAME, dir_batch_size),
        __Batch
    end
).