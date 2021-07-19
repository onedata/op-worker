%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides helper functions for management of automation workflows,
%%% lanes, parallel boxes and task statuses.
%%% TODO VFS-7853 find better name for status/utils that are shared by tasks, pboxes and lanes
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_status_utils).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    is_transition_allowed/2,
    is_ended/1,
    converge/1
]).

-type status() :: atm_task_execution:status().

-export_type([status/0]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec is_transition_allowed(atm_task_execution:status(), atm_task_execution:status()) ->
    boolean().
is_transition_allowed(?PENDING_STATUS, ?ACTIVE_STATUS) -> true;
is_transition_allowed(?PENDING_STATUS, ?FINISHED_STATUS) -> true;  % possible e.g. for empty store
is_transition_allowed(?PENDING_STATUS, ?SKIPPED_STATUS) -> true;
is_transition_allowed(?ACTIVE_STATUS, ?FINISHED_STATUS) -> true;
is_transition_allowed(?ACTIVE_STATUS, ?FAILED_STATUS) -> true;
is_transition_allowed(_, _) -> false.


-spec is_ended(atm_task_execution:status()) -> boolean().
is_ended(?FINISHED_STATUS) -> true;
is_ended(?FAILED_STATUS) -> true;
is_ended(?SKIPPED_STATUS) -> true;
is_ended(_) -> false.


-spec converge([atm_task_execution:status()]) -> atm_task_execution:status().
converge(Statuses) ->
    converge_unique(lists:usort(Statuses)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec converge_unique([atm_task_execution:status()]) ->
    atm_task_execution:status().
converge_unique([Status]) ->
    Status;
converge_unique(Statuses) ->
    [LowestStatusPresent | _] = lists:dropwhile(
        fun(Status) -> not lists:member(Status, Statuses) end,
        [?ACTIVE_STATUS, ?PENDING_STATUS, ?FAILED_STATUS, ?FINISHED_STATUS]
    ),

    case LowestStatusPresent of
        ?PENDING_STATUS ->
            % Some elements must have ended execution while others are still
            % pending - converged/overall status is active
            ?ACTIVE_STATUS;
        Status ->
            Status
    end.
