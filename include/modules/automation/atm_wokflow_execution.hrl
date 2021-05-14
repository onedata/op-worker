%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used by atm_workflow_execution
%%% machinery.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_WORKFLOW_EXECUTION_HRL).
-define(ATM_WORKFLOW_EXECUTION_HRL, 1).


-include("modules/automation/atm_tmp.hrl").
-include("global_definitions.hrl").

-record(atm_store_iterator_config, {
    store_id :: atm_store:id(),
    strategy :: atm_store_iterator_strategy()
}).

-record(atm_parallel_box_execution, {
    status :: atm_task_execution:status(), %% TODO status
    schema_id :: automation:id(),
    name :: automation:name(),
    tasks :: [atm_task_execution:id()]
}).

-record(atm_lane_execution, {
    status :: atm_task_execution:status(), %% TODO status
    schema_id :: automation:id(),
    name :: automation:name(),
    store_iterator_config :: atm_store_iterator_config:record(),
    parallel_boxes :: [#atm_parallel_box_execution{}]
}).

-define(WAITING_STATE, waiting).
-define(ONGOING_STATE, ongoing).
-define(ENDED_STATE, ended).

-define(WAITING_TREE, <<"waiting">>).
-define(ONGOING_TREE, <<"ongoing">>).
-define(ENDED_TREE, <<"ended">>).

-define(SCHEDULED_STATUS, scheduled).
-define(INITIALIZING_STATUS, initializing).
-define(ENQUEUED_STATUS, enqueued).
-define(PENDING_STATUS, pending).
-define(ACTIVE_STATUS, active).
-define(FINISHED_STATUS, finished).
-define(FAILED_STATUS, failed).


-endif.
