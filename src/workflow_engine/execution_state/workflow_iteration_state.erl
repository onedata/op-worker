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
%%% being processed and iterator used to obtain next items.
%%% TODO VFS-7784 Add eunit tests
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_iteration_state).
-author("Michal Wrzeszcz").

-include("workflow_engine.hrl").

%% API
-export([init/0, handle_iteration_finished/1, get_last_registered_item_index/1, register_new_item/3,
    handle_item_processed/3, get_item_id/2]).
%% Test API
-export([is_finished_and_cleaned/1]).

% Internal record to store information about all items currently being used and last registered
% iterator that will be used to obtain next items.
% Item is considered as finished when all tasks for item are executed.
-record(iteration_state, {
    pending_items = #{} :: #{workflow_execution_state:index() => workflow_cached_item:id()},
    last_registered_item_index = 0 :: workflow_execution_state:index() | undefined, % undefined when iteration is
                                                                                    % finished and all items are
                                                                                    % registered
    first_not_finished_item_index = 1 :: workflow_execution_state:index(), % TODO VFS-7787 - maybe init as undefined?
    items_finished_ahead = gb_trees:empty() :: items_finished_ahead()
}).

-type state() :: #iteration_state{}.
% Tree storing ranges of items which processing already finished.
% Range is deleted from tree when no items with smaller index is being processed.
% Range is encoded as tupe {To, From} to allow easier finding of ranges to be merged (see handle_item_processed/3).
-type items_finished_ahead() :: gb_trees:tree({
    To :: workflow_execution_state:index(),
    From :: workflow_execution_state:index()
}, workflow_cached_item:id()).

-export_type([state/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> state().
init() ->
    #iteration_state{}.

-spec handle_iteration_finished(state()) -> state().
handle_iteration_finished(Progress) ->
    Progress#iteration_state{last_registered_item_index = undefined}.

-spec get_last_registered_item_index(state()) -> workflow_execution_state:index() | undefined.
get_last_registered_item_index(#iteration_state{last_registered_item_index = Index}) ->
    Index.

-spec register_new_item(state(), workflow_execution_state:index(), workflow_cached_item:id()) ->
    {workflow_execution_state:index(), state()} | ?WF_ERROR_RACE_CONDITION.
register_new_item(
    Progress = #iteration_state{
        pending_items = Pending,
        last_registered_item_index = LastItemIndex
    },
    LastItemIndex,
    ItemId
) ->
    NewItemIndex = LastItemIndex + 1,
    {NewItemIndex, Progress#iteration_state{
        pending_items = Pending#{NewItemIndex => ItemId},
        last_registered_item_index = NewItemIndex
    }};
register_new_item(_, _, _) ->
    ?WF_ERROR_RACE_CONDITION.

-spec handle_item_processed(state(), workflow_execution_state:index(), workflow_jobs:item_processing_result()) ->
    {
        state(),
        ItemIdToSnapshot :: workflow_cached_item:id() | undefined,
        ItemIdsToDelete :: [workflow_cached_item:id()]
    }.
handle_item_processed(
    Progress = #iteration_state{
        pending_items = Pending,
        first_not_finished_item_index = ItemIndex,
        items_finished_ahead = FinishedAhead
    },
    ItemIndex,
    SuccessOrFailure) ->
    FinishedItemId = maps:get(ItemIndex, Pending),
    {IdToSnapshot, IdsToDelete, FirstNotFinishedItemIndex, FinalFinishedAhead} =
        case gb_trees:is_empty(FinishedAhead) of
            true ->
                {FinishedItemId, [FinishedItemId], ItemIndex + 1, FinishedAhead};
            false ->
                case gb_trees:smallest(FinishedAhead) of
                    {{To, From} = Key, undefined} when From =:= ItemIndex + 1 ->
                        UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                        {FinishedItemId, [FinishedItemId], To + 1, UpdatedFinishedAhead};
                    {{To, From} = Key, ItemIdToReturn} when From =:= ItemIndex + 1 ->
                        UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                        {ItemIdToReturn, [FinishedItemId, ItemIdToReturn], To + 1, UpdatedFinishedAhead};
                    _ ->
                        {FinishedItemId, [FinishedItemId], ItemIndex + 1, FinishedAhead}
                end
        end,

    FinalIdToSnapshot = case SuccessOrFailure of
        ?SUCCESS -> IdToSnapshot;
        ?FAILURE -> undefined
    end,

    {Progress#iteration_state{
        pending_items = maps:remove(ItemIndex, Pending),
        first_not_finished_item_index = FirstNotFinishedItemIndex,
        items_finished_ahead = FinalFinishedAhead
    }, FinalIdToSnapshot, IdsToDelete};
handle_item_processed(
    Progress = #iteration_state{
        pending_items = Pending,
        items_finished_ahead = FinishedAhead
    },
    ItemIndex,
    ?SUCCESS
) ->
    FinishedItemId = maps:get(ItemIndex, Pending),
    {IdsToDelete, FinalFinishedAhead} =
        case gb_trees:next(gb_trees:iterator_from({ItemIndex - 1, 0}, FinishedAhead)) of
            {{To, From} = Key, undefined, _} when From =:= ItemIndex + 1 ->
                UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                {[], gb_trees:insert({To, ItemIndex}, FinishedItemId, UpdatedFinishedAhead)};
            {{To, From} = Key, ItemId, _} when From =:= ItemIndex + 1 ->
                UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                {[FinishedItemId], gb_trees:insert({To, ItemIndex}, ItemId, UpdatedFinishedAhead)};
            {{To, From} = Key, undefined, TreeIterator} when To =:= ItemIndex - 1 ->
                UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                case gb_trees:next(TreeIterator) of
                    {{To2, From2} = Key2, ItemId2, _} when From2 =:= ItemIndex + 1 ->
                        UpdatedFinishedAhead2 = gb_trees:delete(Key2, UpdatedFinishedAhead),
                        {[FinishedItemId, ItemId2], gb_trees:insert({To2, From}, undefined, UpdatedFinishedAhead2)};
                    _ ->
                        {[FinishedItemId], gb_trees:insert({ItemIndex, From}, undefined, UpdatedFinishedAhead)}
                end;
            {{To, From} = Key, ItemId, TreeIterator} when To =:= ItemIndex - 1 ->
                UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                case gb_trees:next(TreeIterator) of
                    {{To2, From2} = Key2, ItemId2, _} when From2 =:= ItemIndex + 1 ->
                        UpdatedFinishedAhead2 = gb_trees:delete(Key2, UpdatedFinishedAhead),
                        {[ItemId, FinishedItemId], gb_trees:insert({To2, From}, ItemId2, UpdatedFinishedAhead2)};
                    _ ->
                        {[ItemId], gb_trees:insert({ItemIndex, From}, FinishedItemId, UpdatedFinishedAhead)}
                end;
            _ ->
                {[], gb_trees:insert({ItemIndex, ItemIndex}, FinishedItemId, FinishedAhead)}
        end,
    {Progress#iteration_state{
        pending_items = maps:remove(ItemIndex, Pending),
        items_finished_ahead = FinalFinishedAhead
    }, undefined, IdsToDelete};
handle_item_processed(
    Progress = #iteration_state{
        pending_items = Pending,
        items_finished_ahead = FinishedAhead
    },
    ItemIndex,
    ?FAILURE
) ->
    FinishedItemId = maps:get(ItemIndex, Pending),
    {IdsToDelete, FinalFinishedAhead} =
        case gb_trees:next(gb_trees:iterator_from({ItemIndex - 1, 0}, FinishedAhead)) of
            {{To, From} = Key, undefined, _} when From =:= ItemIndex + 1 ->
                UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                {[FinishedItemId], gb_trees:insert({To, ItemIndex}, undefined, UpdatedFinishedAhead)};
            {{To, From} = Key, ItemId, _} when From =:= ItemIndex + 1 ->
                UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                {[FinishedItemId, ItemId], gb_trees:insert({To, ItemIndex}, undefined, UpdatedFinishedAhead)};
            {{To, From} = Key, ItemId, TreeIterator} when To =:= ItemIndex - 1 ->
                UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                case gb_trees:next(TreeIterator) of
                    {{To2, From2} = Key2, undefined, _} when From2 =:= ItemIndex + 1 ->
                        UpdatedFinishedAhead2 = gb_trees:delete(Key2, UpdatedFinishedAhead),
                        {[FinishedItemId], gb_trees:insert({To2, From}, ItemId, UpdatedFinishedAhead2)};
                    {{To2, From2} = Key2, ItemId2, _} when From2 =:= ItemIndex + 1 ->
                        UpdatedFinishedAhead2 = gb_trees:delete(Key2, UpdatedFinishedAhead),
                        {[FinishedItemId, ItemId2], gb_trees:insert({To2, From}, ItemId, UpdatedFinishedAhead2)};
                    _ ->
                        {[FinishedItemId], gb_trees:insert({ItemIndex, From}, ItemId, UpdatedFinishedAhead)}
                end;
            _ ->
                {[FinishedItemId], gb_trees:insert({ItemIndex, ItemIndex}, undefined, FinishedAhead)}
        end,
    {Progress#iteration_state{
        pending_items = maps:remove(ItemIndex, Pending),
        items_finished_ahead = FinalFinishedAhead
    }, undefined, IdsToDelete}.

-spec get_item_id(state(), workflow_execution_state:index()) -> workflow_cached_item:id().
get_item_id(#iteration_state{pending_items = Pending}, ItemIndex) ->
    maps:get(ItemIndex, Pending).

%%%===================================================================
%%% Test API
%%%===================================================================

-spec is_finished_and_cleaned(state()) -> boolean().
is_finished_and_cleaned(#iteration_state{
    pending_items = Pending,
    items_finished_ahead = FinishedAhead,
    last_registered_item_index = LastIteratorIndex
}) ->
    LastIteratorIndex =:= undefined orelse maps:size(Pending) =:= 0 andalso gb_trees:is_empty(FinishedAhead).