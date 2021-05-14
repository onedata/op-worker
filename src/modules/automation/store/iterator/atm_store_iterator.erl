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

-include("modules/automation/atm_tmp.hrl").
-include("modules/automation/atm_wokflow_execution.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([create/2]).

%% iterator callbacks
-export([get_next/1, jump_to/2]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type item() :: json_utils:json_term().

-record(atm_store_iterator, {
    config :: atm_store_iterator_config:record(),
    data_spec :: atm_data_spec:record(),
    container_iterator :: atm_container_iterator:record()
}).
-type record() :: #atm_store_iterator{}.

-export_type([record/0, item/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(atm_store_iterator_config:record(), atm_container:record()) -> record().
create(AtmStoreIteratorConfig, AtmContainer) ->
    #atm_store_iterator{
        config = AtmStoreIteratorConfig,
        data_spec = atm_container:get_data_spec(AtmContainer),
        container_iterator = atm_container:get_iterator(AtmContainer)
    }.


%%%===================================================================
%%% Iterator callbacks
%%%===================================================================


-spec get_next(record()) -> {ok, item(), iterator:cursor(), record()} | stop.
get_next(#atm_store_iterator{
    config = #atm_store_iterator_config{strategy = #atm_store_iterator_serial_strategy{}},
    container_iterator = AtmContainerIterator
} = AtmStoreIterator) ->
    case atm_container_iterator:get_next_batch(1, AtmContainerIterator) of
        stop ->
            stop;
        {ok, [Item], Marker, NewAtmContainerIterator} ->
            {ok, Item, Marker, AtmStoreIterator#atm_store_iterator{
                container_iterator = NewAtmContainerIterator
            }}
    end;
get_next(#atm_store_iterator{
    config = #atm_store_iterator_config{strategy = #atm_store_iterator_batch_strategy{
        size = Size
    }},
    container_iterator = AtmContainerIterator
} = AtmStoreIterator) ->
    case atm_container_iterator:get_next_batch(Size, AtmContainerIterator) of
        stop ->
            stop;
        {ok, Items, Marker, NewAtmContainerIterator} ->
            {ok, Items, Marker, AtmStoreIterator#atm_store_iterator{
                container_iterator = NewAtmContainerIterator
            }}
    end.


-spec jump_to(iterator:cursor(), record()) -> record().
jump_to(Cursor, #atm_store_iterator{
    container_iterator = AtmContainerIterator
} = AtmStoreIterator) ->
    AtmStoreIterator#atm_store_iterator{
        container_iterator = atm_container_iterator:jump_to(Cursor, AtmContainerIterator)
    }.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_store_iterator{
    config = AtmStoreIteratorConfig,
    data_spec = AtmDataSpec,
    container_iterator = AtmContainerIterator
}, NestedRecordEncoder) ->
    #{
        <<"config">> => NestedRecordEncoder(AtmStoreIteratorConfig, atm_store_iterator_config),
        <<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec),
        <<"containerIterator">> => NestedRecordEncoder(AtmContainerIterator, atm_container_iterator)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"config">> := AtmStoreIteratorConfigJson,
    <<"dataSpec">> := AtmDataSpecJson,
    <<"containerIterator">> := AtmContainerIteratorJson
}, NestedRecordDecoder) ->
    #atm_store_iterator{
        config = NestedRecordDecoder(AtmStoreIteratorConfigJson, atm_store_iterator_config),
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        container_iterator = NestedRecordDecoder(AtmContainerIteratorJson, atm_container_iterator)
    }.
