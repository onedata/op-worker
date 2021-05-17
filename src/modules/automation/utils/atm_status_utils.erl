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
%%% @end
%%%-------------------------------------------------------------------
-module(atm_status_utils).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_wokflow_execution.hrl").

%% API
-export([
    replace_at/3,
    is_transition_allowed/2,
    converge/1
]).


%%%===================================================================
%%% API functions
%%%===================================================================


% TODO VFS-7660 mv to ctool lists_utils
-spec replace_at(term(), pos_integer(), [term()]) -> [term()].
replace_at(NewValue, 1, [_ | Rest]) ->
    [NewValue | Rest];
replace_at(NewValue, Index, [Element | Rest]) ->
    [Element | replace_at(Rest, Index - 1, NewValue)].


-spec is_transition_allowed(
    atm_workflow_execution:status(),
    atm_workflow_execution:status()
) ->
    boolean().
is_transition_allowed(?PENDING_STATUS, _) -> true;
is_transition_allowed(_, _) -> false.


-spec converge([atm_workflow_execution:status()]) ->
    atm_workflow_execution:status().
converge([Status]) -> Status;
converge([?ACTIVE_STATUS | _]) -> ?ACTIVE_STATUS;
converge([?PENDING_STATUS | _]) -> ?ACTIVE_STATUS;
converge([?FAILED_STATUS, ?FINISHED_STATUS]) -> ?FAILED_STATUS.
