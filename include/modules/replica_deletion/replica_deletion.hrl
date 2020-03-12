%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Definitions of macros used in modules associated with
%%% replica_deletion mechanism.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(REPLICA_DELETION_HRL).
-define(REPLICA_DELETION_HRL, 1).

-include("global_definitions.hrl").

% replica deletion actions
-define(REQUEST_DELETION_SUPPORT, request_deletion_support).
-define(CONFIRM_DELETION_SUPPORT, confirm_deletion_support).
-define(REFUSE_DELETION_SUPPORT, refuse_deletion_support).
-define(RELEASE_DELETION_LOCK, release_deletion_lock).

% replica deletion job types
-define(AUTOCLEANING_JOB, autocleaning_job).
-define(EVICTION_JOB, eviction_job).

-define(REPLICA_DELETION_WORKER, replica_deletion_worker).
-define(REPLICA_DELETION_WORKERS_POOL, replica_deletion_workers_pool).
-define(REPLICA_DELETION_WORKERS_NUM, application:get_env(?APP_NAME, replica_deletion_workers_num, 10)).

-endif.