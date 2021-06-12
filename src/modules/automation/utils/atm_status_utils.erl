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

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    replace_at/3,
    is_transition_allowed/2,
    converge/1
]).

-type status() :: atm_workflow_execution:status() | atm_task_execution:status().

-export_type([status/0]).


%%%===================================================================
%%% API functions
%%%===================================================================


% TODO VFS-7660 mv to ctool lists_utils
-spec replace_at(term(), pos_integer(), [term()]) -> [term()].
replace_at(NewValue, 1, [_ | Rest]) ->
    [NewValue | Rest];
replace_at(NewValue, Index, [Element | Rest]) ->
    [Element | replace_at(Rest, Index - 1, NewValue)].


%% TODO VFS-7674 add missing transitions
-spec is_transition_allowed(status(), status()) ->
    boolean().
is_transition_allowed(?SCHEDULED_STATUS, ?PREPARING_STATUS) -> true;
is_transition_allowed(?PREPARING_STATUS, ?ENQUEUED_STATUS) -> true;
is_transition_allowed(?PREPARING_STATUS, ?FAILED_STATUS) -> true;
is_transition_allowed(?ENQUEUED_STATUS, ?ACTIVE_STATUS) -> true;
is_transition_allowed(?ENQUEUED_STATUS, ?ACTIVE_STATUS) -> true;
is_transition_allowed(?PENDING_STATUS, ?ACTIVE_STATUS) -> true;
is_transition_allowed(_, _) -> false.


-spec converge([status()]) -> status().
converge(Statuses) ->
    converge_uniquely_sorted(lists:usort(Statuses)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% TODO VFS-7674 handle all combination of statuses
%% @private
-spec converge_uniquely_sorted([status()]) -> status().
converge_uniquely_sorted([Status]) -> Status;
converge_uniquely_sorted([?ACTIVE_STATUS | _]) -> ?ACTIVE_STATUS;
converge_uniquely_sorted([?PENDING_STATUS | _]) -> ?ACTIVE_STATUS;
converge_uniquely_sorted([?FAILED_STATUS, ?FINISHED_STATUS]) -> ?FAILED_STATUS.
