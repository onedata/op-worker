%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------

-ifndef(REPLICA_DELETION_HRL).
-define(REPLICA_DELETION_HRL, 1).

% replica deletion actions
-define(REQUEST_DELETION_SUPPORT, request_deletion_support).
-define(CONFIRM_DELETION_SUPPORT, confirm_deletion_support).
-define(REFUSE_DELETION_SUPPORT, refuse_deletion_support).
-define(RELEASE_DELETION_LOCK, release_deletion_lock).

% replica deletion job types
-define(AUTOCLEANING_JOB, autocleaning_job).
-define(EVICTION_JOB, eviction_job).

-endif.