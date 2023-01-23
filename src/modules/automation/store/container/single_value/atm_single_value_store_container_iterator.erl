%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `atm_store_container_iterator` functionality for
%%% `atm_single_value_store_container`.
%%%
%%%                             !!! Caution !!!
%%% This iterator snapshots store container's item at creation time so that
%%% even if item kept in container changes the iterator will still return
%%% the same old item.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_single_value_store_container_iterator).
-author("Bartosz Walkowicz").

-behaviour(atm_store_container_iterator).
-behaviour(persistent_record).

-include_lib("ctool/include/errors.hrl").

%% API
-export([build/2]).

% atm_store_container_iterator callbacks
-export([get_next_batch/3]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_single_value_store_container_iterator, {
    item_data_spec :: atm_data_spec:record(),
    compressed_item :: undefined | atm_value:compressed(),
    exhausted = false :: boolean()
}).
-type record() :: #atm_single_value_store_container_iterator{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(undefined | atm_value:compressed(), atm_data_spec:record()) ->
    record().
build(CompressedItem, ItemDataSpec) ->
    #atm_single_value_store_container_iterator{
        compressed_item = CompressedItem,
        item_data_spec = ItemDataSpec,
        exhausted = false
    }.


%%%===================================================================
%%% atm_store_container_iterator callbacks
%%%===================================================================


-spec get_next_batch(
    atm_workflow_execution_auth:record(),
    atm_store_container_iterator:batch_size(),
    record()
) ->
    {ok, [atm_value:expanded()], record()} | stop.
get_next_batch(_, _, #atm_single_value_store_container_iterator{compressed_item = undefined}) ->
    stop;

get_next_batch(_, _, #atm_single_value_store_container_iterator{exhausted = true}) ->
    stop;

get_next_batch(AtmWorkflowExecutionAuth, _, Record = #atm_single_value_store_container_iterator{
    item_data_spec = ItemDataSpec,
    compressed_item = CompressedItem
}) ->
    Batch = atm_value:filterexpand_list(AtmWorkflowExecutionAuth, [CompressedItem], ItemDataSpec),
    {ok, Batch, Record#atm_single_value_store_container_iterator{exhausted = true}}.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_single_value_store_container_iterator{
    compressed_item = Item,
    item_data_spec = ItemDataSpec,
    exhausted = Exhausted
}, NestedRecordEncoder) ->
    maps_utils:put_if_defined(#{
        <<"itemDataSpec">> => NestedRecordEncoder(ItemDataSpec, atm_data_spec),
        <<"exhausted">> => Exhausted
    }, <<"compressedItem">>, Item).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"itemDataSpec">> := ItemDataSpecJson,
    <<"exhausted">> := Exhausted
} = RecordJson, NestedRecordDecoder) ->
    #atm_single_value_store_container_iterator{
        compressed_item = maps:get(<<"compressedItem">>, RecordJson, undefined),
        item_data_spec = NestedRecordDecoder(ItemDataSpecJson, atm_data_spec),
        exhausted = Exhausted
    }.
