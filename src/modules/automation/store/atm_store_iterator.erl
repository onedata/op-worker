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
-export([get_next/1]).

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


-spec get_next(record()) -> {ok, atm_api:item(), record()} | stop.
get_next(#atm_store_iterator{
    spec = #atm_store_iterator_spec{strategy = #atm_store_iterator_serial_strategy{}},
    container_iterator = AtmContainerIterator
} = AtmStoreIterator) ->
    case atm_container_iterator:get_next_batch(1, AtmContainerIterator) of
        stop ->
            stop;
        {ok, [Item], NewAtmContainerIterator} ->
            {ok, Item, AtmStoreIterator#atm_store_iterator{
                container_iterator = NewAtmContainerIterator
            }}
    end;
get_next(#atm_store_iterator{
    spec = #atm_store_iterator_spec{strategy = #atm_store_iterator_batch_strategy{
        size = Size
    }},
    container_iterator = AtmContainerIterator
} = AtmStoreIterator) ->
    case atm_container_iterator:get_next_batch(Size, AtmContainerIterator) of
        stop ->
            stop;
        {ok, Items, NewAtmContainerIterator} ->
            {ok, Items, AtmStoreIterator#atm_store_iterator{
                container_iterator = NewAtmContainerIterator
            }}
    end.


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
