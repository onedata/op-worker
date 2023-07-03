%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `atm_store_container_iterator` functionality for
%%% `atm_exception_store_container`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_exception_store_container_iterator).
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


-record(atm_exception_store_container_iterator, {
    item_data_spec :: atm_data_spec:record(),
    backend_id :: atm_store_container_infinite_log_backend:id(),
    last_listed_index = atm_store_container_infinite_log_backend:iterator_start_index() ::
        atm_store_container_infinite_log_backend:index()
}).
-type record() :: #atm_exception_store_container_iterator{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(atm_data_spec:record(), atm_store_container_infinite_log_backend:id()) ->
    record().
build(ItemDataSpec, BackendId) ->
    #atm_exception_store_container_iterator{
        item_data_spec = ItemDataSpec,
        backend_id = BackendId
    }.


%%%===================================================================
%%% atm_store_container_iterator callbacks
%%%===================================================================


-spec get_next_batch(
    atm_workflow_execution_auth:record(),
    atm_store_container_iterator:batch_size(),
    record()
) ->
    {ok, [automation:item()], record()} | stop.
get_next_batch(AtmWorkflowExecutionAuth, BatchSize, Record = #atm_exception_store_container_iterator{
    item_data_spec = ItemDataSpec,
    backend_id = BackendId,
    last_listed_index = LastListedIndex
}) ->
    Result = atm_store_container_infinite_log_backend:iterator_get_next_batch(
        BatchSize, BackendId, LastListedIndex, fun({Index, {_Timestamp, CompressedItem}}) ->
            {Index, atm_value:from_store_item(AtmWorkflowExecutionAuth, CompressedItem, ItemDataSpec)}
        end
    ),
    case Result of
        stop ->
            stop;
        {ok, FilteredEntries, NewLastListedIndex} ->
            {ok, FilteredEntries, Record#atm_exception_store_container_iterator{
                last_listed_index = NewLastListedIndex
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
db_encode(#atm_exception_store_container_iterator{
    item_data_spec = ItemDataSpec,
    backend_id = BackendId,
    last_listed_index = LastListedIndex
}, NestedRecordEncoder) ->
    #{
        <<"itemDataSpec">> => NestedRecordEncoder(ItemDataSpec, atm_data_spec),
        <<"backendId">> => BackendId,
        <<"lastListedIndex">> => LastListedIndex
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(
    JsonRecord = #{<<"itemDataSpec">> := ItemDataSpecJson, <<"backendId">> := BackendId},
    NestedRecordDecoder
) ->
    #atm_exception_store_container_iterator{
        item_data_spec = NestedRecordDecoder(ItemDataSpecJson, atm_data_spec),
        backend_id = BackendId,
        last_listed_index = maps:get(<<"lastListedIndex">>, JsonRecord, <<"-1">>)
    }.
