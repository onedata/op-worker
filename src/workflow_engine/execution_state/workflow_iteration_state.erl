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
-export([init/0, can_process_items/1, handle_iteration_finished/1, get_last_registered_item_index/1, register_item/3,
    handle_item_processed/3, finalize/1, is_resuming_last_item/1, get_item_id/2,
    dump/1, from_dump/1, get_dump_struct/0]).
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
    items_finished_ahead = gb_trees:empty() :: items_finished_ahead(),
    phase = executing :: phase()
}).

-type state() :: #iteration_state{}.
% Tree storing ranges of items which processing already finished.
% Range is deleted from tree when no items with smaller index is being processed.
% Range is encoded as tuple {To, From} to allow easier finding of ranges to be merged (see handle_item_processed/3).
-type items_finished_ahead() :: gb_trees:tree({
    To :: workflow_execution_state:index(),
    From :: workflow_execution_state:index()
}, SnapshotData :: {workflow_execution_state:index(), workflow_cached_item:id()} | undefined).
-type phase() :: executing | {resuming, workflow_execution_state:index() | last_item | last_items} | finalzing.

-type dump() :: {
    [workflow_execution_state:index()],
    workflow_execution_state:index(),
    workflow_execution_state:index(),
    [{{workflow_execution_state:index(), workflow_execution_state:index()}, workflow_execution_state:index()}]
}.

-export_type([state/0, dump/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> state().
init() ->
    #iteration_state{}.

-spec can_process_items(state()) -> boolean().
can_process_items(#iteration_state{phase = Phase}) ->
    Phase =:= executing.


-spec handle_iteration_finished(state()) -> state().
handle_iteration_finished(#iteration_state{phase = {resuming, last_item}} = Progress) ->
    Progress#iteration_state{phase = executing, last_registered_item_index = undefined};
handle_iteration_finished(#iteration_state{phase = {resuming, last_items}} = Progress) ->
    Progress#iteration_state{phase = executing, last_registered_item_index = undefined};
handle_iteration_finished(Progress) ->
    Progress#iteration_state{last_registered_item_index = undefined}.

-spec get_last_registered_item_index(state()) -> workflow_execution_state:index() | undefined.
get_last_registered_item_index(#iteration_state{last_registered_item_index = Index}) ->
    Index.

-spec register_item(state(), workflow_execution_state:index(), workflow_cached_item:id()) ->
    {new_item | restarted_item | already_processed_item, workflow_execution_state:index(), state()} |
    ?WF_ERROR_RACE_CONDITION.
register_item(
    Progress = #iteration_state{
        phase = executing,
        last_registered_item_index = LastItemIndex,
        pending_items = Pending
    },
    LastItemIndex,
    ItemId
) ->
    NewItemIndex = LastItemIndex + 1,
    {new_item, NewItemIndex, Progress#iteration_state{
        pending_items = Pending#{NewItemIndex => ItemId},
        last_registered_item_index = NewItemIndex
    }};
register_item(
    Progress = #iteration_state{
        last_registered_item_index = LastItemIndex,
        pending_items = Pending,
        items_finished_ahead = FinishedAhead,
        phase = {resuming, ResumeIndex} = Phase
    },
    LastItemIndex,
    ItemId
) ->
    NewItemIndex = LastItemIndex + 1,
    IsRestartedItem = case maps:get(NewItemIndex, Pending, not_found) of
        undefined -> true;
        _ -> false
    end,

    IsFinishedItem = case gb_trees:next(gb_trees:iterator_from({NewItemIndex, 0}, FinishedAhead)) of
        {Key, {ValueIndex, undefined}, _} when ValueIndex =:= NewItemIndex -> {true, Key};
        _ -> false
    end,

    Progress2 = Progress#iteration_state{
        last_registered_item_index = NewItemIndex,
        phase = case ResumeIndex of
            NewItemIndex -> executing;
            _ -> Phase
        end
    },

    case {IsRestartedItem, IsFinishedItem} of
        {false, false} ->
            {already_processed_item, NewItemIndex, Progress2};
        {true, false} ->
            {restarted_item, NewItemIndex, Progress2#iteration_state{
                pending_items = Pending#{NewItemIndex => ItemId}
            }};
        {false, {true, TreeKey}} ->
            {restarted_item, NewItemIndex, Progress2#iteration_state{
                items_finished_ahead = gb_trees:enter(TreeKey, {NewItemIndex, ItemId}, FinishedAhead)
            }}
    end;
register_item(_, _, _) ->
    ?WF_ERROR_RACE_CONDITION.

-spec handle_item_processed(state(), workflow_execution_state:index(), boolean()) ->
    {
        state(),
        ItemIdToReportError :: workflow_cached_item:id() | undefined,
        ItemIdToSnapshot :: workflow_cached_item:id() | undefined,
        ItemIdsToDelete :: [workflow_cached_item:id()]
    }.
handle_item_processed(
    Progress = #iteration_state{
        pending_items = Pending,
        first_not_finished_item_index = FinishedItemIndex,
        items_finished_ahead = FinishedAhead
    },
    FinishedItemIndex,
    ShouldSnapshotItem
) ->
    FinishedItemId = maps:get(FinishedItemIndex, Pending),
    {IdToSnapshot, IdsToDelete, FirstNotFinishedItemIndex, FinalFinishedAhead} =
        case gb_trees:is_empty(FinishedAhead) of
            true ->
                {FinishedItemId, [FinishedItemId], FinishedItemIndex + 1, FinishedAhead};
            false ->
                case gb_trees:smallest(FinishedAhead) of
                    {{To, From} = Key, undefined} when From =:= FinishedItemIndex + 1 ->
                        UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                        {FinishedItemId, [FinishedItemId], To + 1, UpdatedFinishedAhead};
                    {{To, From} = Key, {_, ItemIdToReturn}} when From =:= FinishedItemIndex + 1 ->
                        UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                        {ItemIdToReturn, [FinishedItemId, ItemIdToReturn], To + 1, UpdatedFinishedAhead};
                    _ ->
                        {FinishedItemId, [FinishedItemId], FinishedItemIndex + 1, FinishedAhead}
                end
        end,

    FinalIdToSnapshot = case ShouldSnapshotItem of
        true -> IdToSnapshot;
        false -> undefined
    end,

    {Progress#iteration_state{
        pending_items = maps:remove(FinishedItemIndex, Pending),
        first_not_finished_item_index = FirstNotFinishedItemIndex,
        items_finished_ahead = FinalFinishedAhead
    }, FinishedItemId, FinalIdToSnapshot, IdsToDelete};
handle_item_processed(
    Progress = #iteration_state{
        pending_items = Pending,
        items_finished_ahead = FinishedAhead
    },
    FinishedItemIndex,
    true
) ->
    FinishedItemId = maps:get(FinishedItemIndex, Pending),
    {IdsToDelete, FinalFinishedAhead} =
        case gb_trees:next(gb_trees:iterator_from({FinishedItemIndex - 1, 0}, FinishedAhead)) of
            {{To, From} = Key, undefined, _} when From =:= FinishedItemIndex + 1 ->
                UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                {[], gb_trees:insert({To, FinishedItemIndex}, {FinishedItemIndex, FinishedItemId}, UpdatedFinishedAhead)};
            {{To, From} = Key, Value, _} when From =:= FinishedItemIndex + 1 ->
                UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                {[FinishedItemId], gb_trees:insert({To, FinishedItemIndex}, Value, UpdatedFinishedAhead)};
            {{To, From} = Key, undefined, TreeIterator} when To =:= FinishedItemIndex - 1 ->
                UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                case gb_trees:next(TreeIterator) of
                    {{To2, From2} = Key2, undefined, _} when From2 =:= FinishedItemIndex + 1 ->
                        UpdatedFinishedAhead2 = gb_trees:delete(Key2, UpdatedFinishedAhead),
                        {[FinishedItemId], gb_trees:insert({To2, From}, undefined, UpdatedFinishedAhead2)};
                    {{To2, From2} = Key2, {_, ItemId2}, _} when From2 =:= FinishedItemIndex + 1 ->
                        UpdatedFinishedAhead2 = gb_trees:delete(Key2, UpdatedFinishedAhead),
                        {[FinishedItemId, ItemId2], gb_trees:insert({To2, From}, undefined, UpdatedFinishedAhead2)};
                    _ ->
                        {[FinishedItemId], gb_trees:insert({FinishedItemIndex, From}, undefined, UpdatedFinishedAhead)}
                end;
            {{To, From} = Key, {_, ItemId}, TreeIterator} when To =:= FinishedItemIndex - 1 ->
                UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                case gb_trees:next(TreeIterator) of
                    {{To2, From2} = Key2, undefined, _} when From2 =:= FinishedItemIndex + 1 ->
                        UpdatedFinishedAhead2 = gb_trees:delete(Key2, UpdatedFinishedAhead),
                        {[ItemId], gb_trees:insert({To2, From}, {FinishedItemIndex, FinishedItemId}, UpdatedFinishedAhead2)};
                    {{To2, From2} = Key2, Value, _} when From2 =:= FinishedItemIndex + 1 ->
                        UpdatedFinishedAhead2 = gb_trees:delete(Key2, UpdatedFinishedAhead),
                        {[ItemId, FinishedItemId], gb_trees:insert({To2, From}, Value, UpdatedFinishedAhead2)};
                    _ ->
                        {[ItemId], gb_trees:insert({FinishedItemIndex, From}, {FinishedItemIndex, FinishedItemId}, UpdatedFinishedAhead)}
                end;
            _ ->
                {[], gb_trees:insert({FinishedItemIndex, FinishedItemIndex}, {FinishedItemIndex, FinishedItemId}, FinishedAhead)}
        end,
    {Progress#iteration_state{
        pending_items = maps:remove(FinishedItemIndex, Pending),
        items_finished_ahead = FinalFinishedAhead
    }, FinishedItemId, undefined, IdsToDelete};
handle_item_processed(
    Progress = #iteration_state{
        pending_items = Pending,
        items_finished_ahead = FinishedAhead
    },
    FinishedItemIndex,
    false
) ->
    FinishedItemId = maps:get(FinishedItemIndex, Pending),
    {IdsToDelete, FinalFinishedAhead} =
        case gb_trees:next(gb_trees:iterator_from({FinishedItemIndex - 1, 0}, FinishedAhead)) of
            {{To, From} = Key, undefined, _} when From =:= FinishedItemIndex + 1 ->
                UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                {[FinishedItemId], gb_trees:insert({To, FinishedItemIndex}, undefined, UpdatedFinishedAhead)};
            {{To, From} = Key, {_, ItemId}, _} when From =:= FinishedItemIndex + 1 ->
                UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                {[FinishedItemId, ItemId], gb_trees:insert({To, FinishedItemIndex}, undefined, UpdatedFinishedAhead)};
            {{To, From} = Key, Value, TreeIterator} when To =:= FinishedItemIndex - 1 ->
                UpdatedFinishedAhead = gb_trees:delete(Key, FinishedAhead),
                case gb_trees:next(TreeIterator) of
                    {{To2, From2} = Key2, undefined, _} when From2 =:= FinishedItemIndex + 1 ->
                        UpdatedFinishedAhead2 = gb_trees:delete(Key2, UpdatedFinishedAhead),
                        {[FinishedItemId], gb_trees:insert({To2, From}, Value, UpdatedFinishedAhead2)};
                    {{To2, From2} = Key2, {_, ItemId2}, _} when From2 =:= FinishedItemIndex + 1 ->
                        UpdatedFinishedAhead2 = gb_trees:delete(Key2, UpdatedFinishedAhead),
                        {[FinishedItemId, ItemId2], gb_trees:insert({To2, From}, Value, UpdatedFinishedAhead2)};
                    _ ->
                        {[FinishedItemId], gb_trees:insert({FinishedItemIndex, From}, Value, UpdatedFinishedAhead)}
                end;
            _ ->
                {[FinishedItemId], gb_trees:insert({FinishedItemIndex, FinishedItemIndex}, undefined, FinishedAhead)}
        end,
    {Progress#iteration_state{
        pending_items = maps:remove(FinishedItemIndex, Pending),
        items_finished_ahead = FinalFinishedAhead
    }, FinishedItemId, undefined, IdsToDelete}.

-spec finalize(state()) -> {[workflow_cached_item:id()], state()}.
finalize(#iteration_state{phase = finalzing} = State) ->
    {[], State};
finalize(#iteration_state{pending_items = Pending, items_finished_ahead = FinishedAhead} = State) ->
    FinishedAheadItemIds = lists:filtermap(fun
        (undefined) -> false;
        ({_, Id}) -> {true, Id}
    end, gb_trees:values(FinishedAhead)),

    {
        maps:values(Pending) ++ FinishedAheadItemIds,
        State#iteration_state{phase = finalzing} % TODO - a co jak mamy resuming obecnie?
    }.


-spec is_resuming_last_item(state()) -> boolean().
is_resuming_last_item(#iteration_state{phase = Phase}) ->
    Phase =:= {resuming, last_item}.

-spec get_item_id(state(), workflow_execution_state:index()) -> workflow_cached_item:id().
get_item_id(#iteration_state{pending_items = Pending}, ItemIndex) ->
    maps:get(ItemIndex, Pending).


-spec dump(state()) -> dump().
dump(#iteration_state{
    pending_items = PendingItems,
    last_registered_item_index = LastRegistered,
    first_not_finished_item_index = FirstNotFinished,
    items_finished_ahead = FinishedAhead
}) ->
    MappedFinishedAhead = lists:map(fun
        ({Key, undefined}) -> {Key, undefined};
        ({Key, {ItemIndex, _}}) -> {Key, ItemIndex}
    end, gb_trees:to_list(FinishedAhead)),
    {maps:keys(PendingItems), LastRegistered, FirstNotFinished, MappedFinishedAhead}.


-spec from_dump(dump()) -> state().
% TODO - co jesli dumpowalismy gdy LastRegistered bylo juz undefined?
% Z undefined wywali sie po resumie - poprawic
from_dump({PendingItemsIndexes, LastRegistered, FirstNotFinished, FinishedAheadList}) ->
    PendingItems = maps:from_list(lists:map(fun(Index) -> {Index, undefined} end, PendingItemsIndexes)),
    FinishedAhead = gb_trees:from_orddict(lists:map(fun
        ({Key, undefined}) -> {Key, undefined};
        ({Key, ItemIndex}) -> {Key, {ItemIndex, undefined}}
    end, FinishedAheadList)),

    % TODO - a co jesli jestesmy ostatnim itemem?
    Phase = case LastRegistered =:= 0 orelse LastRegistered =:= FirstNotFinished - 1 of
        true -> 
            executing;
        false -> 
            case {LastRegistered, length(PendingItemsIndexes) =< 1} of
                {undefined, true} -> {resuming, last_item};
                {undefined, false} -> {resuming, last_items};
                _ -> {resuming, LastRegistered}
            end
    end,

    #iteration_state{
        pending_items = PendingItems,
        last_registered_item_index = FirstNotFinished - 1,
        first_not_finished_item_index = FirstNotFinished,
        items_finished_ahead = FinishedAhead,
        phase = Phase
    }.


-spec get_dump_struct() -> tuple().
get_dump_struct() ->
    {[integer], integer, integer, [{{integer, integer}, integer}]}.


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