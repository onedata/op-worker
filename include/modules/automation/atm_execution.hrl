%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used by automation execution
%%% machinery.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_EXECUTION_HRL).
-define(ATM_EXECUTION_HRL, 1).


-include("modules/automation/atm_tmp.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/automation/automation.hrl").


-record(atm_task_execution_ctx, {
    item :: json_utils:json_term()
}).

-record(atm_task_execution_argument_spec, {
    name :: automation:name(),
    value_builder :: atm_task_argument_value_builder:record(),
    data_spec :: atm_data_spec:record(),
    is_batch :: boolean()
}).

-record(atm_parallel_box_execution, {
    schema_id :: automation:id(),
    name :: automation:name(),
    status :: atm_parallel_box_execution:status(),
    tasks :: #{atm_task_execution:id() => atm_task_execution:status()}
}).

-record(atm_store_iterator_config, {
    store_id :: atm_store:id(),
    strategy :: atm_store_iterator_spec:strategy()
}).

-record(atm_lane_execution, {
    schema_id :: automation:id(),
    name :: automation:name(),
    status :: atm_lane_execution:status(),
    parallel_boxes :: [atm_parallel_box_execution:record()],
    store_iterator_spec :: atm_store_iterator_spec:record()
}).


-define(WAITING_STATE, waiting).
-define(ONGOING_STATE, ongoing).
-define(ENDED_STATE, ended).

-define(WAITING_TREE, <<"waiting">>).
-define(ONGOING_TREE, <<"ongoing">>).
-define(ENDED_TREE, <<"ended">>).

-define(SCHEDULED_STATUS, scheduled).
-define(PREPARING_STATUS, preparing).
-define(ENQUEUED_STATUS, enqueued).
-define(PENDING_STATUS, pending).
-define(ACTIVE_STATUS, active).
-define(FINISHED_STATUS, finished).
-define(FAILED_STATUS, failed).


-endif.
