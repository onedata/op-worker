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
%%% TODO VFS-7551 - delete not used iterators from cache (when lane is finished)
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_iterator_snapshot).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([save/4, get/1]).

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
    workflow_store:iterator()
) -> ok.
save(ExecutionId, LaneIndex, ItemIndex, Iterator) ->
    Record = #workflow_iterator_snapshot{lane_index = LaneIndex, item_index = ItemIndex, iterator = Iterator},
    Diff = fun
        (ExistingRecord = #workflow_iterator_snapshot{lane_index = SavedLaneIndex, item_index = SavedItemIndex}) when
            SavedLaneIndex < LaneIndex orelse (SavedLaneIndex == LaneIndex andalso SavedItemIndex < ItemIndex) ->
            {ok, ExistingRecord#workflow_iterator_snapshot{
                lane_index = LaneIndex, item_index = ItemIndex, iterator = Iterator}};
        (_) ->
            % Multiple processed have been saving iterators in parallel
            {error, already_saved}
    end,
    case datastore_model:update(?CTX, ExecutionId, Diff, Record) of
        {ok, _} -> ok;
        {error, already_saved} -> ok
    end.

-spec get(workflow_engine:execution_id()) -> workflow_store:iterator().
get(ExecutionId) ->
    {ok, #document{value = #workflow_iterator_snapshot{iterator = Iterator}}} =
        datastore_model:get(?CTX, ExecutionId),
    Iterator.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {iterator, {custom, json, {workflow_store, encode_iterator, decode_iterator}}},
        {lane_index, integer},
        {item_index, integer}
    ]}.