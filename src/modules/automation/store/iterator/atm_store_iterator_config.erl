%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for performing operations on automation store iterator config.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_iterator_config).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% API
-export([build/2]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type record() :: #atm_store_iterator_config{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(atm_store_api:registry(), atm_store_iterator_spec:record()) ->
    record() | no_return().
build(AtmStoreRegistry, #atm_store_iterator_spec{
    store_schema_id = AtmStoreSchemaId,
    strategy = AtmStoreIteratorStrategy
}) ->
    case maps:get(AtmStoreSchemaId, AtmStoreRegistry, undefined) of
        undefined ->
            throw(?ERROR_ATM_REFERENCED_NONEXISTENT_STORE(AtmStoreSchemaId));
        AtmStoreId ->
            #atm_store_iterator_config{
                store_id = AtmStoreId,
                strategy = AtmStoreIteratorStrategy
            }
    end.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_store_iterator_config{
    store_id = AtmStoreId,
    strategy = AtmStoreIteratorStrategy
}, NestedRecordEncoder) ->
    AtmStoreIteratorStrategyRecordType = utils:record_type(AtmStoreIteratorStrategy),

    #{
        <<"_type">> => atom_to_binary(AtmStoreIteratorStrategyRecordType, utf8),
        <<"storeId">> => AtmStoreId,
        <<"strategy">> => NestedRecordEncoder(
            AtmStoreIteratorStrategy, AtmStoreIteratorStrategyRecordType
        )
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"_type">> := AtmStoreIteratorStrategyRecordTypeBin,
    <<"storeId">> := AtmStoreId,
    <<"strategy">> := AtmStoreIteratorStrategyJson
}, NestedRecordDecoder) ->
    AtmStoreIteratorStrategyRecordType = binary_to_atom(
        AtmStoreIteratorStrategyRecordTypeBin, utf8
    ),
    #atm_store_iterator_config{
        store_id = AtmStoreId,
        strategy = NestedRecordDecoder(
            AtmStoreIteratorStrategyJson, AtmStoreIteratorStrategyRecordType
        )
    }.
