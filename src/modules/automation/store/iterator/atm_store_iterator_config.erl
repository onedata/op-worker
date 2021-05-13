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
-module(atm_store_iterator_config).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_tmp.hrl").

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type record() :: #atm_store_iterator_config{}.

-export_type([record/0]).


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
}, NestedRecordDecoder) ->
    #atm_store_iterator_config{
        store_id = AtmStoreId,
        strategy = binary_to_term(json_utils:decode(AtmStoreIteratorStrategyJson))
        % TODO replace with below after integration with ctool/oz
%%        strategy = NestedRecordDecoder(AtmStoreIteratorStrategyJson, atm_store_iterator_strategy)
    }.
