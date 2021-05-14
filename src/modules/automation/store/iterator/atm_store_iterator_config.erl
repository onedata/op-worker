%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_iterator_config).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_tmp.hrl").
-include("modules/automation/atm_wokflow_execution.hrl").

%% API
-export([build/2]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type record() :: #atm_store_iterator_config{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(atm_store_api:registry(), atm_store_iterator_spec()) ->
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
}, _NestedRecordEncoder) ->
    #{
        <<"storeId">> => AtmStoreId,
        % TODO replace with below after integration with ctool/oz
        <<"strategy">> => json_utils:encode(term_to_binary(AtmStoreIteratorStrategy))
%%        <<"strategy">> => NestedRecordEncoder(AtmStoreIteratorStrategy, atm_store_iterator_strategy)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"storeId">> := AtmStoreId,
    <<"strategy">> := AtmStoreIteratorStrategyJson
}, _NestedRecordDecoder) ->
    #atm_store_iterator_config{
        store_id = AtmStoreId,
        strategy = binary_to_term(json_utils:decode(AtmStoreIteratorStrategyJson))
        % TODO replace with below after integration with ctool/oz
%%        strategy = NestedRecordDecoder(AtmStoreIteratorStrategyJson, atm_store_iterator_strategy)
    }.
