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
    container_iterator :: atm_container_iterator:record()
}).
-type record() :: #atm_store_iterator{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(atm_store_iterator_spec:record(), atm_container:record()) -> record().
build(AtmStoreIteratorSpec, AtmContainer) ->
    #atm_store_iterator{
        spec = AtmStoreIteratorSpec,
        data_spec = atm_container:get_data_spec(AtmContainer),
        container_iterator = atm_container:acquire_iterator(AtmContainer)
    }.


%%%===================================================================
%%% Iterator callbacks
%%%===================================================================


-spec get_next(atm_workflow_execution_env:record(), record()) -> 
    {ok, atm_api:item(), record()} | stop.
get_next(AtmWorkflowExecutionEnv, #atm_store_iterator{
    spec = #atm_store_iterator_spec{strategy = #atm_store_iterator_serial_strategy{}},
    data_spec = DataSpec,
    container_iterator = AtmContainerIterator
} = AtmStoreIterator) ->
    AtmWorkflowExecutionCtx = 
        atm_workflow_execution_env:acquire_workflow_execution_ctx(AtmWorkflowExecutionEnv),
    case get_next_internal(AtmWorkflowExecutionCtx, AtmContainerIterator, 1, DataSpec) of
        stop ->
            stop;
        {ok, [Item], NewAtmContainerIterator} ->
            {ok, Item, AtmStoreIterator#atm_store_iterator{
                container_iterator = NewAtmContainerIterator
            }}
    end;
get_next(AtmWorkflowExecutionEnv, #atm_store_iterator{
    data_spec = DataSpec,
    spec = #atm_store_iterator_spec{strategy = #atm_store_iterator_batch_strategy{
        size = Size
    }},
    container_iterator = AtmContainerIterator
} = AtmStoreIterator) ->
    AtmWorkflowExecutionCtx =
        atm_workflow_execution_env:acquire_workflow_execution_ctx(AtmWorkflowExecutionEnv),
    case get_next_internal(AtmWorkflowExecutionCtx, AtmContainerIterator, Size, DataSpec) of
        stop ->
            stop;
        {ok, Items, NewAtmContainerIterator} ->
            {ok, Items, AtmStoreIterator#atm_store_iterator{
                container_iterator = NewAtmContainerIterator
            }}
    end.


-spec forget_before(record()) -> ok.
forget_before(#atm_store_iterator{container_iterator = ContainerIterator}) ->
    atm_container_iterator:forget_before(ContainerIterator).


-spec mark_exhausted(record()) -> ok.
mark_exhausted(#atm_store_iterator{container_iterator = ContainerIterator}) ->
    atm_container_iterator:mark_exhausted(ContainerIterator).

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
    container_iterator = AtmContainerIterator
}, NestedRecordEncoder) ->
    #{
        <<"spec">> => NestedRecordEncoder(AtmStoreIteratorSpec, atm_store_iterator_spec),
        <<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec),
        <<"containerIterator">> => NestedRecordEncoder(AtmContainerIterator, atm_container_iterator)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"spec">> := AtmStoreIteratorSpecJson,
    <<"dataSpec">> := AtmDataSpecJson,
    <<"containerIterator">> := AtmContainerIteratorJson
}, NestedRecordDecoder) ->
    #atm_store_iterator{
        spec = NestedRecordDecoder(AtmStoreIteratorSpecJson, atm_store_iterator_spec),
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        container_iterator = NestedRecordDecoder(AtmContainerIteratorJson, atm_container_iterator)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_next_internal(
    atm_workflow_execution_ctx:record(), 
    atm_container_iterator:record(), 
    pos_integer(), 
    atm_data_spec:record()
) ->
    {ok, [atm_api:item()], atm_container_iterator:record()} | stop.
get_next_internal(AtmWorkflowExecutionCtx, AtmContainerIterator, Size, DataSpec) ->
    case atm_container_iterator:get_next_batch(AtmWorkflowExecutionCtx, Size, AtmContainerIterator) of
        stop ->
            stop;
        {ok, Items, NewAtmContainerIterator} ->
            ExpandedItems = lists:filtermap(fun(Item) ->
                case atm_data_compressor:expand(AtmWorkflowExecutionCtx, Item, DataSpec) of
                    {ok, Value} -> {true, Value};
                    {error, _} -> false
                end
            end, Items),
            case ExpandedItems of
                [] ->
                    get_next_internal(AtmWorkflowExecutionCtx, NewAtmContainerIterator, Size, DataSpec);
                _ ->
                    {ok, ExpandedItems, NewAtmContainerIterator}
            end
    end.
