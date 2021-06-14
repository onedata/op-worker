%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for workflow_execution_state processing information
%%% about iteration progress. Keeps information about items currently
%%% being processed and iterators connected with them.
%%% TODO VFS-7784 Add eunit tests
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_iteration_state).
-author("Michal Wrzeszcz").

-include("workflow_engine.hrl").

%% API
-export([init/1, handle_iteration_finished/1, get_last_registered_step/1, register_new_step/4,
    handle_step_finish/2]).
%% Helper API operating on #iteration_step record
-export([get_iterator/1, get_item_id/2]).

% Internal record to store information about single iterator and item connected with it
-record(iteration_step, {
    cached_item_id :: workflow_cached_item:id() | undefined, % undefined for initial iterator
    iterator :: iterator:iterator()
}).

% Internal record to store information about all items and iterators currently being used.
% Iteration step is considered registered when item and iterator are obtained (item is to be processed)
% and is considered as finished when all tasks for item are executed.
-record(iteration_state, {
    pending_steps :: #{workflow_execution_state:index() => iteration_step()},
    last_registered_step :: iteration_step() | undefined, % undefined when iteration is finished (nothing has been
                                                          % returned trying to obtain next item and iterator)
    last_registered_step_index = 0 :: workflow_execution_state:index(),
    last_finished_step_index = 1 :: workflow_execution_state:index(), % TODO VFS-7787 - maybe init as undefined?
    steps_finished_ahead = gb_trees:empty() :: steps_finished_ahead()
}).

-type iteration_step() :: #iteration_step{}.
-type state() :: #iteration_state{}.
% Tree storing ranges of items which processing already finished.
% Range is deleted from tree when no items with smaller index is being processed.
-type steps_finished_ahead() :: gb_trees:tree({
    From :: workflow_execution_state:index(),
    To :: workflow_execution_state:index()
}, iteration_step()).

-export_type([state/0, iteration_step/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(iterator:iterator()) -> state().
init(InitialIterator) ->
    FirstStep = #iteration_step{iterator = InitialIterator},
    #iteration_state{
        pending_steps = #{0 => FirstStep},
        last_registered_step = FirstStep
    }.

-spec handle_iteration_finished(state()) -> state().
handle_iteration_finished(Progress) ->
    Progress#iteration_state{last_registered_step = undefined}.

-spec get_last_registered_step(state()) -> iteration_step() | undefined.
get_last_registered_step(#iteration_state{last_registered_step = Step}) ->
    Step.

-spec register_new_step(state(), iteration_step(), workflow_cached_item:id(), iterator:iterator()) ->
    state() | ?WF_ERROR_RACE_CONDITION.
register_new_step(
    Progress = #iteration_state{
        pending_steps = Steps,
        last_registered_step_index = LastItemIndex,
        last_registered_step = #iteration_step{cached_item_id = PrevItemId}
    },
    _PrevStep = #iteration_step{
        cached_item_id = PrevItemId
    },
    ItemId,
    NewIterator
) ->
    NewItemIndex = LastItemIndex + 1,
    NewStep = #iteration_step{cached_item_id = ItemId, iterator = NewIterator},
    {NewItemIndex, Progress#iteration_state{
        pending_steps = Steps#{NewItemIndex => NewStep},
        last_registered_step_index = NewItemIndex,
        last_registered_step = NewStep
    }};
register_new_step(_, _, _, _) ->
    ?WF_ERROR_RACE_CONDITION.

-spec handle_step_finish(state(), workflow_execution_state:index()) ->
    {state(), LastConsecutiveFinishedIterator :: iterator:iterator() | undefined}.
handle_step_finish(
    Progress = #iteration_state{
        pending_steps = Steps,
        last_finished_step_index = ItemIndex,
        steps_finished_ahead = FinishedAhead
    }, ItemIndex) ->
    #iteration_step{iterator = FinishedIterator} = maps:get(ItemIndex, Steps),
    {FinalIterator, LastConsecutiveFinishedIndex, FinalFinishedAhead} = case gb_trees:is_empty(FinishedAhead) of
        true ->
            {FinishedIterator, ItemIndex + 1, FinishedAhead};
        false ->
            case gb_trees:smallest(FinishedAhead) of
                {{From, To} = Key, IteratorToReturn} when From =:= ItemIndex + 1 ->
                    UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                    {IteratorToReturn, To + 1, UpdatedFinishedAhead};
                _ ->
                    {FinishedIterator, ItemIndex + 1, FinishedAhead}
            end
    end,
    
    {Progress#iteration_state{
        pending_steps = maps:remove(ItemIndex, Steps),
        last_finished_step_index = LastConsecutiveFinishedIndex,
        steps_finished_ahead = FinalFinishedAhead
    }, FinalIterator};
handle_step_finish(
    Progress = #iteration_state{
        pending_steps = Steps,
        last_finished_step_index = LastFinishedStepIndex,
        steps_finished_ahead = FinishedAhead
    },
    ItemIndex
) ->
    #iteration_step{iterator = FinishedIterator} = maps:get(ItemIndex, Steps),
    FinalFinishedAhead = case gb_trees:next(gb_trees:iterator_from(ItemIndex, FinishedAhead)) of
        {{From, To} = Key, Iterator, _} when From =:= ItemIndex + 1 ->
            UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
            gb_trees:insert({ItemIndex, To}, Iterator, UpdatedFinishedAhead);
        {{From, To} = Key, _, _} when To =:= ItemIndex - 1 ->
            UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
            gb_trees:insert({From, ItemIndex}, FinishedIterator, UpdatedFinishedAhead);
        _ ->
            gb_trees:insert({ItemIndex, ItemIndex}, FinishedIterator, FinishedAhead)
    end,
    {Progress#iteration_state{
        pending_steps = maps:remove(ItemIndex, Steps),
        steps_finished_ahead = FinalFinishedAhead,
        last_finished_step_index = min(LastFinishedStepIndex, ItemIndex)
    }, undefined}.

%%%===================================================================
%%% Helper API operating on #iteration_step record
%%%===================================================================

-spec get_iterator(iteration_step()) -> iterator:iterator().
get_iterator(#iteration_step{iterator = Iterator}) ->
    Iterator.

-spec get_item_id(state(), workflow_execution_state:index()) -> workflow_cached_item:id().
get_item_id(#iteration_state{pending_steps = Steps}, ItemIndex) ->
    #iteration_step{cached_item_id = ItemId} = maps:get(ItemIndex, Steps),
    ItemId.