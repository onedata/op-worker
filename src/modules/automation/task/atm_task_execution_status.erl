%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utility functions for management of automation
%%% task execution status.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_status).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([is_transition_allowed/2, is_ended/1]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec is_transition_allowed(atm_task_execution:status(), atm_task_execution:status()) ->
    boolean().
is_transition_allowed(?PENDING_STATUS, ?ACTIVE_STATUS) -> true;
is_transition_allowed(?PENDING_STATUS, ?SKIPPED_STATUS) -> true;
is_transition_allowed(?ACTIVE_STATUS, ?FINISHED_STATUS) -> true;
is_transition_allowed(?ACTIVE_STATUS, ?FAILED_STATUS) -> true;
is_transition_allowed(_, _) -> false.


-spec is_ended(atm_task_execution:status()) -> boolean().
is_ended(?FINISHED_STATUS) -> true;
is_ended(?FAILED_STATUS) -> true;
is_ended(?SKIPPED_STATUS) -> true;
is_ended(_) -> false.
