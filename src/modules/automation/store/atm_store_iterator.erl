%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `iterator` functionality for `atm_store`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_iterator).
-author("Bartosz Walkowicz").

-behaviour(iterator).
-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").
-include("modules/automation/atm_tmp.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([build/2]).

%% iterator callbacks
-export([get_next/2, forget_before/1, mark_exhausted/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_store_iterator, {
    spec :: atm_store_iterator_spec:record(),
    data_spec :: atm_data_spec:record(),
    store_container_iterator :: atm_store_container_iterator:record()
}).
-type record() :: #atm_store_iterator{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(atm_store_iterator_spec:record(), atm_store_container:record()) -> record().
build(AtmStoreIteratorSpec, AtmStoreContainer) ->
    #atm_store_iterator{
        spec = AtmStoreIteratorSpec,
        data_spec = atm_store_container:get_data_spec(AtmStoreContainer),
        store_container_iterator = atm_store_container:acquire_iterator(AtmStoreContainer)
    }.


%%%===================================================================
%%% Iterator callbacks
%%%===================================================================


-spec get_next(atm_workflow_execution_env:record(), record()) -> 
    {ok, automation:item(), record()} | stop.
get_next(AtmWorkflowExecutionEnv, AtmStoreIterator = #atm_store_iterator{
    spec = #atm_store_iterator_spec{strategy = #atm_store_iterator_serial_strategy{}},
    data_spec = DataSpec,
    store_container_iterator = AtmStoreContainerIterator
}) ->
    AtmWorkflowExecutionCtx =
        atm_workflow_execution_env:acquire_workflow_execution_ctx(AtmWorkflowExecutionEnv),
    case get_next_internal(AtmWorkflowExecutionCtx, AtmStoreContainerIterator, 1, DataSpec) of
        stop ->
            stop;
        {ok, [Item], NewAtmStoreContainerIterator} ->
            {ok, Item, AtmStoreIterator#atm_store_iterator{
                store_container_iterator = NewAtmStoreContainerIterator
            }}
    end;

get_next(AtmWorkflowExecutionEnv, AtmStoreIterator = #atm_store_iterator{
    data_spec = DataSpec,
    spec = #atm_store_iterator_spec{strategy = #atm_store_iterator_batch_strategy{
        size = Size
    }},
    store_container_iterator = AtmStoreContainerIterator
}) ->
    AtmWorkflowExecutionCtx =
        atm_workflow_execution_env:acquire_workflow_execution_ctx(AtmWorkflowExecutionEnv),
    case get_next_internal(AtmWorkflowExecutionCtx, AtmStoreContainerIterator, Size, DataSpec) of
        stop ->
            stop;
        {ok, Items, NewAtmStoreContainerIterator} ->
            {ok, Items, AtmStoreIterator#atm_store_iterator{
                store_container_iterator = NewAtmStoreContainerIterator
            }}
    end.


-spec forget_before(record()) -> ok.
forget_before(#atm_store_iterator{store_container_iterator = ContainerIterator}) ->
    atm_store_container_iterator:forget_before(ContainerIterator).


-spec mark_exhausted(record()) -> ok.
mark_exhausted(#atm_store_iterator{store_container_iterator = ContainerIterator}) ->
    atm_store_container_iterator:mark_exhausted(ContainerIterator).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_store_iterator{
    spec = AtmStoreIteratorSpec,
    data_spec = AtmDataSpec,
    store_container_iterator = AtmStoreContainerIterator
}, NestedRecordEncoder) ->
    #{
        <<"spec">> => NestedRecordEncoder(AtmStoreIteratorSpec, atm_store_iterator_spec),
        <<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec),
        <<"containerIterator">> => NestedRecordEncoder(
            AtmStoreContainerIterator, atm_store_container_iterator
        )
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"spec">> := AtmStoreIteratorSpecJson,
    <<"dataSpec">> := AtmDataSpecJson,
    <<"containerIterator">> := AtmStoreContainerIteratorJson
}, NestedRecordDecoder) ->
    #atm_store_iterator{
        spec = NestedRecordDecoder(AtmStoreIteratorSpecJson, atm_store_iterator_spec),
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        store_container_iterator = NestedRecordDecoder(
            AtmStoreContainerIteratorJson, atm_store_container_iterator
        )
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_next_internal(
    atm_workflow_execution_ctx:record(), 
    atm_store_container_iterator:record(),
    pos_integer(), 
    atm_data_spec:record()
) ->
    {ok, [automation:item()], atm_store_container_iterator:record()} | stop.
get_next_internal(AtmWorkflowExecutionCtx, AtmStoreContainerIterator, Size, DataSpec) ->
    case atm_store_container_iterator:get_next_batch(AtmWorkflowExecutionCtx, Size, AtmStoreContainerIterator) of
        stop ->
            stop;
        {ok, CompressedItems, NewAtmStoreContainerIterator} ->
            ExpandedItems = lists:filtermap(fun(CompressedItem) ->
                case atm_value:expand(AtmWorkflowExecutionCtx, CompressedItem, DataSpec) of
                    {ok, ExpandedItem} -> {true, ExpandedItem};
                    {error, _} -> false
                end
            end, CompressedItems),
            case ExpandedItems of
                [] ->
                    get_next_internal(AtmWorkflowExecutionCtx, NewAtmStoreContainerIterator, Size, DataSpec);
                _ ->
                    {ok, ExpandedItems, NewAtmStoreContainerIterator}
            end
    end.
