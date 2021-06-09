%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Datastore model persisting information needed to schedule workflows
%%% on workflow_engine. It stores list of ongoing workflow execution ids
%%% and number of used slots. It is used by processes of workflow_engine
%%% that synchronize on model's documents update dividing between
%%% themselves jobs to execute.
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_engine_state).
-author("Michal Wrzeszcz").

-include("workflow_engine.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([init/2, add_execution_id/2, remove_execution_id/2, poll_next_execution_id/1, get_execution_ids/1,
    increment_slot_usage/1, decrement_slot_usage/1]).
%% Test API
-export([get_slots_used/1]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(workflow_engine:id(), non_neg_integer()) -> ok.
init(EngineId, SlotsLimit) ->
    Doc = #document{key = EngineId, value = #workflow_engine_state{slots_limit = SlotsLimit}},
    {ok, _} = datastore_model:create(?CTX, Doc),
    ok.

% TODO VFS-7551 - acquire slot if it is free (optimization - one call instead of two)
-spec add_execution_id(workflow_engine:id(), workflow_engine:execution_id()) -> ok.
add_execution_id(EngineId, ExecutionId) ->
    Diff = fun
        (#workflow_engine_state{executions = ExecutionIds} = Record) ->
            {ok, Record#workflow_engine_state{executions = [ExecutionId | ExecutionIds]}}
    end,
    {ok, _} = datastore_model:update(?CTX, EngineId, Diff),
    ok.

-spec remove_execution_id(workflow_engine:id(), workflow_engine:execution_id()) -> ok | ?WF_ERROR_ALREADY_REMOVED.
remove_execution_id(EngineId, ExecutionId) ->
    Diff = fun
        (#workflow_engine_state{executions = ExecutionIds} = Record) ->
            case lists:member(ExecutionId, ExecutionIds) of
                true -> {ok, Record#workflow_engine_state{executions = ExecutionIds -- [ExecutionId]}};
                false -> ?WF_ERROR_ALREADY_REMOVED
            end
    end,
    case datastore_model:update(?CTX, EngineId, Diff) of
        {ok, _} -> ok;
        ?WF_ERROR_ALREADY_REMOVED -> ?WF_ERROR_ALREADY_REMOVED
    end.

-spec poll_next_execution_id(workflow_engine:id()) -> {ok, workflow_engine:execution_id()} | ?ERROR_NOT_FOUND.
poll_next_execution_id(EngineId) ->
    % TODO VFS-7551 add groups/list/spaces management - we use priorities here
    % Provide execution ids using round robin algorithm due to update of executions list on each poll
    % (do not update document if list has only one element)
    Diff = fun
        (#workflow_engine_state{executions = []}) ->
            ?ERROR_NOT_FOUND;
        (#workflow_engine_state{executions = [ExecutionId]}) ->
            {error, {single_execution, ExecutionId}};
        (#workflow_engine_state{executions = ExecutionIds} = Record) ->
            Next = lists:last(ExecutionIds),
            ExecutionIds2 = [Next | lists:droplast(ExecutionIds)],
            {ok, Record#workflow_engine_state{executions = ExecutionIds2}}
    end,
    case datastore_model:update(?CTX, EngineId, Diff) of
        {ok, #document{value = #workflow_engine_state{executions = [Next | _]}}} ->
            {ok, Next};
        {error, {single_execution, ExecutionId}} ->
            {ok, ExecutionId};
        ?ERROR_NOT_FOUND = NotFound ->
            NotFound
    end.

-spec get_execution_ids(workflow_engine:id()) -> [workflow_engine:execution_id()].
get_execution_ids(EngineId) ->
    {ok, #document{value = #workflow_engine_state{executions = ExecutionIds}}} = datastore_model:get(?CTX, EngineId),
    ExecutionIds.

-spec increment_slot_usage(workflow_engine:id()) -> ok | ?WF_ERROR_ALL_SLOTS_USED.
increment_slot_usage(EngineId) ->
    Diff = fun
        (#workflow_engine_state{slots_used = Limit, slots_limit = Limit}) ->
            ?WF_ERROR_ALL_SLOTS_USED;
        (#workflow_engine_state{slots_used = Used} = Record) ->
            {ok, Record#workflow_engine_state{slots_used = Used + 1}}
    end,
    case datastore_model:update(?CTX, EngineId, Diff) of
        {ok, _} -> ok;
        ?WF_ERROR_ALL_SLOTS_USED = Error -> Error
    end.

-spec decrement_slot_usage(workflow_engine:id()) -> ok.
decrement_slot_usage(EngineId) ->
    Diff = fun
        (#workflow_engine_state{slots_used = Used} = Record) ->
            {ok, Record#workflow_engine_state{slots_used = Used - 1}}
    end,
    {ok, _} = datastore_model:update(?CTX, EngineId, Diff),
    ok.

%%%===================================================================
%%% Test API
%%%===================================================================

-spec get_slots_used(workflow_engine:id()) -> non_neg_integer().
get_slots_used(EngineId) ->
    {ok, #document{value = #workflow_engine_state{slots_used = SlotsUsed}}} = datastore_model:get(?CTX, EngineId),
    SlotsUsed.