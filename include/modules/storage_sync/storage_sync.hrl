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

-ifndef(STORAGE_SYNC_HRL).
-define(STORAGE_SYNC_HRL, 1).

-include("global_definitions.hrl").

-define(DEFAULT_DELETE_ENABLE, false).
-define(DEFAULT_WRITE_ONCE, false).
-define(DEFAULT_SCAN_INTERVAL, 10).
-define(DEFAULT_SYNC_ACL, false).
-define(DEFAULT_SYNC_MAX_DEPTH, 65535).
-define(SYNC_DIR_BATCH_SIZE, application:get_env(?APP_NAME, storage_sync_dir_batch_size, 100)).

-endif.
