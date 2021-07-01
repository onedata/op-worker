%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Stores iterators used to restart workflow executions (one iterator per execution).
%%% Each iterator is stored together with lane and item index to prevent races.
%%% TODO VFS-7787 - save first iterator of lane
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_iterator_snapshot).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([save/4, get/1, cleanup/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-define(CTX, #{
    model => ?MODULE
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec save(
    workflow_engine:execution_id(),
    workflow_execution_state:index(),
    workflow_execution_state:index(),
    iterator:iterator()
) -> ok.
save(ExecutionId, LaneIndex, ItemIndex, Iterator) ->
    {PrevLaneIndex, PrevIterator} = case ?MODULE:get(ExecutionId) of
        {ok, ReturnedLineIndex, ReturnedIterator} -> {ReturnedLineIndex, ReturnedIterator};
        ?ERROR_NOT_FOUND -> {undefined, undefined}
    end,
    Record = #workflow_iterator_snapshot{lane_index = LaneIndex, item_index = ItemIndex, iterator = Iterator},
    Diff = fun
        (ExistingRecord = #workflow_iterator_snapshot{lane_index = SavedLaneIndex, item_index = SavedItemIndex}) when
            SavedLaneIndex < LaneIndex orelse (SavedLaneIndex == LaneIndex andalso SavedItemIndex < ItemIndex) ->
            {ok, ExistingRecord#workflow_iterator_snapshot{
                lane_index = LaneIndex, item_index = ItemIndex, iterator = Iterator}};
        (_) ->
            % Multiple processes have been saving iterators in parallel
            {error, already_saved}
    end,
    case datastore_model:update(?CTX, ExecutionId, Diff, Record) of
        {ok, _} ->
            % Mark iterator exhausted after change of line
            % (each line has new iterator and iterator for previous line can be destroyed)
            case PrevLaneIndex =/= 0 andalso PrevLaneIndex < LaneIndex of
                true -> mark_exhausted(PrevIterator, ExecutionId); % TODO VFS-7787 - handle without additional get
                false -> ok
            end,
            case ItemIndex of
                0 ->
                    ok;
                _ ->
                    try
                        iterator:forget_before(Iterator)
                    catch
                        Error:Reason ->
                            ?error_stacktrace("Unexpected error forgeting iterator for execution: ~p ~p:~p",
                                [ExecutionId, Error, Reason]),
                            ok
                    end
            end;
        {error, already_saved} ->
            ok
    end.

-spec get(workflow_engine:execution_id()) ->
    {ok, workflow_execution_state:index(), iterator:iterator()} | ?ERROR_NOT_FOUND.
get(ExecutionId) ->
    case datastore_model:get(?CTX, ExecutionId) of
        {ok, #document{value = #workflow_iterator_snapshot{lane_index = LaneIndex, iterator = Iterator}}} ->
            {ok, LaneIndex, Iterator};
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND
    end.

-spec cleanup(workflow_engine:execution_id()) -> ok.
cleanup(ExecutionId) ->
    case ?MODULE:get(ExecutionId) of
        {ok, _LaneIndex, Iterator} ->
            mark_exhausted(Iterator, ExecutionId),
            ok = datastore_model:delete(?CTX, ExecutionId);
        ?ERROR_NOT_FOUND ->
            ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec mark_exhausted(iterator:iterator(), workflow_engine:execution_id()) -> ok.
mark_exhausted(Iterator, ExecutionId) ->
    try
        iterator:mark_exhausted(Iterator)
    catch
        Error:Reason ->
            ?error_stacktrace("Unexpected error marking exhausted iterator for execution: ~p ~p:~p",
                [ExecutionId, Error, Reason]),
            ok
    end.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {iterator, {custom, json, {iterator, encode, decode}}},
        {lane_index, integer},
        {item_index, integer}
    ]}.