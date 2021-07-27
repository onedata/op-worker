%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides helper functions for management of automation workflow
%%% block status.
%%% An automation workflow execution block is an entity which consists of
%%% one or many execution elements (e.g atm lane is made of atm parallel boxes
%%% which are themselves made of atm tasks). The status of such entity is naturally
%%% inferred based on statuses of all it's elements and may only change
%%% if one of it's underlying element changes status.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_block_execution_status).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([is_ended/1, infer/1]).

-type status() :: atm_task_execution:status().

-export_type([status/0]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec is_ended(atm_task_execution:status()) -> boolean().
is_ended(?FINISHED_STATUS) -> true;
is_ended(?FAILED_STATUS) -> true;
is_ended(?SKIPPED_STATUS) -> true;
is_ended(_) -> false.


-spec infer([atm_task_execution:status()] | [status()]) -> status().
infer(ElementsStatuses) ->
    infer_from_unique_statuses(lists:usort(ElementsStatuses)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec infer_from_unique_statuses([atm_task_execution:status()] | [status()]) ->
    status().
infer_from_unique_statuses([Status]) ->
    Status;
infer_from_unique_statuses(Statuses) ->
    [LowestStatusPresent | _] = lists:dropwhile(
        fun(Status) -> not lists:member(Status, Statuses) end,
        [?ACTIVE_STATUS, ?PENDING_STATUS, ?FAILED_STATUS, ?FINISHED_STATUS]
    ),

    case LowestStatusPresent of
        ?PENDING_STATUS ->
            % Some elements must have ended execution while others are still
            % pending - block status is active
            ?ACTIVE_STATUS;
        Status ->
            Status
    end.
