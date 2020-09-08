%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Macro definitions for storage import.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(STORAGE_IMPORT_HRL).
-define(STORAGE_IMPORT_HRL, 1).

-include("global_definitions.hrl").

% storage import modes
-define(AUTO_IMPORT, auto).
-define(MANUAL_IMPORT, manual).

% storage_import_engine results
-define(FILE_IMPORTED, imported).
-define(FILE_UPDATED, updated).
-define(FILE_PROCESSED, processed).
-define(FILE_PROCESSING_FAILED, failed).

% storage import scan statuses
-define(ENQUEUED, enqueued).
-define(RUNNING, running).
-define(ABORTING, aborting).
-define(COMPLETED, completed).
-define(FAILED, failed).
-define(ABORTED, aborted).

-endif.
