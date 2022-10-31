%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Common includes and definitions used in automation workflow execution tests.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_WORKFLOW_EXECUTION_TEST).
-define(ATM_WORKFLOW_EXECUTION_TEST, 1).


-include("atm/atm_test_schema.hrl").
-include("atm/atm_test_schema_drafts.hrl").
-include("atm/atm_test_store.hrl").
-include("atm_workflow_execution_test_runner.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


-define(STOPPING_REASONS, [crash, cancel, failure, interrupt, pause]).


% Lane run scheduled to prepare in advance but deferred until workflow execution stopped - should immediately fail
-define(UNSCHEDULED_LANE_RUN_TEST_SPEC(__LANE_RUN_SELECTOR, __DEFER_AFTER), #atm_lane_run_execution_test_spec{
    selector = __LANE_RUN_SELECTOR,
    prepare_lane = #atm_step_mock_spec{
        defer_after = __DEFER_AFTER,
        after_step_exp_state_diff = no_diff
    }
}).


-endif.
