%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `atm_container_iterator` functionality for
%%% `atm_tree_forest_container`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_tree_forest_container_iterator).
-author("Michal Stanisz").

-behaviour(atm_container_iterator).
-behaviour(persistent_record).

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl"). 
-include_lib("ctool/include/logging.hrl").

%% API
-export([build/2]).

% atm_container_iterator callbacks
-export([get_next_batch/3, forget_before/1, mark_exhausted/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type item_id() :: binary().
-type list_opts() :: term().

-record(queue_ref, {
    id :: atm_tree_forest_iterator_queue:id(),
    current_queue_index = -1 :: integer()
}).

-type queue_ref() :: #queue_ref{}.

-record(atm_tree_forest_container_iterator, {
    callback_module :: module(),
    current_item = undefined :: undefined | item_id(),
    tree_list_opts :: list_opts(),
    roots_iterator :: atm_list_container_iterator:record(),
    queue_ref :: queue_ref()
}).
-type record() :: #atm_tree_forest_container_iterator{}.

-export_type([list_opts/0, record/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================

-callback list_children(
    atm_workflow_execution_ctx:record(), 
    item_id(), 
    list_opts(),
    atm_container_iterator:batch_size()
) -> 
    {[{item_id(), binary()}], [item_id()], list_opts(), IsLast :: boolean()} | no_return().

-callback check_exists(atm_workflow_execution_ctx:record(), item_id()) -> boolean().

-callback initial_listing_options() -> list_opts().

-callback encode_listing_options(list_opts()) -> json_utils:json_term().

-callback decode_listing_options(json_utils:json_term()) -> list_opts().

%%%===================================================================
%%% API
%%%===================================================================

-spec build(atm_list_container_iterator:record(), atm_data_spec:record()) -> record().
build(RootsIterator, DataSpec) ->
    Module = get_callback_module(atm_data_spec:get_type(DataSpec)),
    #atm_tree_forest_container_iterator{
        callback_module = Module,
        roots_iterator = RootsIterator,
        tree_list_opts = Module:initial_listing_options(),
        queue_ref = queue_init()
    }.


%%%===================================================================
%%% atm_container_iterator callbacks
%%%===================================================================

-spec get_next_batch(atm_workflow_execution_ctx:record(), atm_container_iterator:batch_size(), record()) ->
    {ok, [atm_api:item()], record()} | stop.
get_next_batch(AtmWorkflowExecutionCtx, BatchSize, #atm_tree_forest_container_iterator{} = Record) ->
    get_next_batch(AtmWorkflowExecutionCtx, BatchSize, Record, []).


-spec forget_before(record()) -> ok.
forget_before(#atm_tree_forest_container_iterator{queue_ref = QueueRef}) ->
    prune_queue(QueueRef).


-spec mark_exhausted(record()) -> ok.
mark_exhausted(#atm_tree_forest_container_iterator{queue_ref = QueueRef}) -> 
    destroy_queue(QueueRef).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_next_batch(
    atm_workflow_execution_ctx:record(), 
    atm_container_iterator:batch_size(), 
    record(), 
    [atm_api:item()]
) ->
    {ok, [atm_api:item()], record()} | stop.
get_next_batch(_AtmWorkflowExecutionCtx, BatchSize, Record, ForestAcc) when BatchSize =< 0 ->
    {ok, ForestAcc, Record};
get_next_batch(
    AtmWorkflowExecutionCtx, 
    BatchSize, 
    #atm_tree_forest_container_iterator{current_item = undefined} = Record, 
    ForestAcc
) ->
    #atm_tree_forest_container_iterator{
        callback_module = Module, 
        roots_iterator = ListIterator, 
        queue_ref = QueueRef
    } = Record,
    case atm_list_container_iterator:get_next_batch(AtmWorkflowExecutionCtx, 1, ListIterator) of
        {ok, [ItemId], NextRootsIterator} ->
            UpdatedRecord = Record#atm_tree_forest_container_iterator{
                current_item = ItemId,
                roots_iterator = NextRootsIterator,
                tree_list_opts = Module:initial_listing_options(),
                queue_ref = queue_report_new_tree(QueueRef)
            },
            case Module:check_exists(AtmWorkflowExecutionCtx, ItemId) of
                true ->
                    get_next_batch(
                        AtmWorkflowExecutionCtx, BatchSize - 1, UpdatedRecord, [ItemId | ForestAcc]);
                false ->
                    get_next_batch(AtmWorkflowExecutionCtx, BatchSize, 
                        UpdatedRecord#atm_tree_forest_container_iterator{current_item = undefined}, 
                        ForestAcc
                    )
            end;
        stop ->
            case length(ForestAcc) of
                0 -> stop;
                _ -> {ok, ForestAcc, Record}
            end
    end;
get_next_batch(AtmWorkflowExecutionCtx, BatchSize, Record, ForestAcc) ->
    {TreeAcc, NewRecord} = get_next_batch_from_single_tree(
        AtmWorkflowExecutionCtx, BatchSize, Record, ForestAcc),
    get_next_batch(AtmWorkflowExecutionCtx, BatchSize - length(TreeAcc), NewRecord, TreeAcc).


%% @private
-spec get_next_batch_from_single_tree(
    atm_workflow_execution_ctx:record(), 
    atm_container_iterator:batch_size(), 
    record(), 
    [atm_api:item()]
) ->
    {[atm_api:item()], record()}.
get_next_batch_from_single_tree(
    _AtmWorkflowExecutionCtx, 
    _BatchSize, 
    #atm_tree_forest_container_iterator{current_item = undefined} = Record, 
    Acc
) ->
    {Acc, Record};
get_next_batch_from_single_tree(_AtmWorkflowExecutionCtx, BatchSize, Record, Acc) when BatchSize =< 0 ->
    {Acc, Record};
get_next_batch_from_single_tree(AtmWorkflowExecutionCtx, BatchSize, Record, Acc) ->
    #atm_tree_forest_container_iterator{
        callback_module = Module,
        queue_ref = QueueRef,
        current_item = CurrentItem,
        tree_list_opts = ListOpts
    } = Record,
    {TraversableItemsWithNames, NonTraversableItemsIds, NewListOptions, IsLast} =
        Module:list_children(AtmWorkflowExecutionCtx, CurrentItem, ListOpts, BatchSize),
    UpdatedQueueRef = add_to_queue(QueueRef, TraversableItemsWithNames),
    TraversableItemsIds = lists:map(fun({Id, _}) -> Id end, TraversableItemsWithNames),
    
    UpdatedRecord = case IsLast of
        true ->
            {NextItem, QueueRef2} = get_from_queue(UpdatedQueueRef),
            Record#atm_tree_forest_container_iterator{
                current_item = NextItem,
                tree_list_opts = Module:initial_listing_options(),
                queue_ref = QueueRef2
            };
        false ->
            Record#atm_tree_forest_container_iterator{
                tree_list_opts = NewListOptions,
                queue_ref = UpdatedQueueRef
            }
    end,
    Result = TraversableItemsIds ++ NonTraversableItemsIds,
    get_next_batch_from_single_tree(
        AtmWorkflowExecutionCtx, BatchSize - length(Result), UpdatedRecord, Result ++ Acc).


%% @private
-spec queue_init() -> {ok, queue_ref()} | no_return().
queue_init() ->
    case atm_tree_forest_iterator_queue:init() of
        {ok, Id} -> #queue_ref{id = Id};
        {error, _} = Error -> throw(Error)
    end.


%% @private
-spec add_to_queue(queue_ref(), [{item_id(), binary()}]) -> queue_ref() | no_return().
add_to_queue(#queue_ref{id = Id, current_queue_index = Index} = QueueRef, Batch) ->
    case atm_tree_forest_iterator_queue:push(Id, Batch, Index) of
        ok -> QueueRef;
        {error, _} = Error -> throw(Error)
    end.


%% @private
-spec get_from_queue(queue_ref()) -> {item_id() | undefined, queue_ref()} | no_return().
get_from_queue(#queue_ref{id = Id, current_queue_index = Index} = QueueRef) ->
    case atm_tree_forest_iterator_queue:peek(Id, Index + 1) of
        {ok, undefined} ->
            {undefined, QueueRef#queue_ref{current_queue_index = Index}};
        {ok, Value} ->
            ok = atm_tree_forest_iterator_queue:report_processing_index(Id, Index + 1),
            {Value, QueueRef#queue_ref{current_queue_index = Index + 1}};
        {error, _} = Error ->
            throw(Error)
    end.


%% @private
-spec queue_report_new_tree(queue_ref()) -> queue_ref() | no_return().
queue_report_new_tree(#queue_ref{id = Id, current_queue_index = Index} = QueueRef) ->
    case atm_tree_forest_iterator_queue:report_new_tree(Id, Index) of
        ok -> QueueRef#queue_ref{current_queue_index = Index + 1};
        {error, _} = Error -> throw(Error)
    end.


%% @private
-spec prune_queue(queue_ref()) -> ok | no_return().
prune_queue(#queue_ref{id = Id, current_queue_index = Index}) ->
    case atm_tree_forest_iterator_queue:prune(Id, Index - 1) of
        ok -> ok;
        {error, _} = Error -> throw(Error)
    end.


%% @private
-spec destroy_queue(queue_ref()) -> ok | no_return().
destroy_queue(#queue_ref{id = Id}) ->
    case atm_tree_forest_iterator_queue:destroy(Id) of
        ok -> ok;
        {error, _} = Error -> throw(Error)
    end.

%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================

-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_tree_forest_container_iterator{
    callback_module = Module,
    current_item = CurrentItem,
    tree_list_opts = TreeListOpts,
    roots_iterator = RootsIterator,
    queue_ref = QueueRef
}, NestedRecordEncoder) ->
    #{
        <<"typeSpecificModule">> => atom_to_binary(Module, utf8),
        <<"currentItem">> => utils:undefined_to_null(CurrentItem),
        <<"treeListOpts">> => Module:encode_listing_options(TreeListOpts),
        <<"rootsIterator">> => NestedRecordEncoder(RootsIterator, atm_container_iterator),
        <<"queueRef">> => encode_queue_ref(QueueRef)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"typeSpecificModule">> := EncodedModule,
    <<"currentItem">> := CurrentItem,
    <<"treeListOpts">> := TreeListOpts,
    <<"rootsIterator">> := RootsIterator,
    <<"queueRef">> := QueueRef
}, NestedRecordDecoder) ->
    Module = binary_to_atom(EncodedModule, utf8),
    #atm_tree_forest_container_iterator{
        callback_module = Module,
        current_item = utils:null_to_undefined(CurrentItem),
        tree_list_opts = Module:decode_listing_options(TreeListOpts),
        roots_iterator = NestedRecordDecoder(RootsIterator, atm_container_iterator),
        queue_ref = decode_queue_ref(QueueRef)
    }.


%% @private
-spec encode_queue_ref(queue_ref()) -> json_utils:json_term().
encode_queue_ref(#queue_ref{id = QueueId, current_queue_index = CurrentIndex}) ->
   #{
        <<"queueId">> => QueueId,
        <<"currentIndex">> => CurrentIndex
    }.


%% @private
-spec decode_queue_ref(json_utils:json_term()) -> queue_ref().
decode_queue_ref(#{<<"queueId">> := QueueId, <<"currentIndex">> := CurrentIndex}) ->
    #queue_ref{
        id = QueueId,
        current_queue_index = CurrentIndex
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_callback_module(atm_data_type:type()) -> module().
get_callback_module(atm_dataset_type) ->
    atm_dataset_value;
get_callback_module(atm_file_type) ->
    atm_file_value.
