%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines `atm_store_container_iterator` interface - an object which
%%% can be used for iteration over specific `atm_store_container` in batches.
%%%
%%%                             !!! Caution !!!
%%% 1) This behaviour must be implemented by modules with records of the same name.
%%% 2) Modules implementing this behaviour must also implement `persistent_record`
%%%    behaviour.
%%% 3) The container iterator behaviour in case of changes to values kept in container
%%%    is not defined and implementation dependent (it may e.g. return old values).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_container_iterator).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

%% API
-export([get_next_batch/3, forget_before/1, mark_exhausted/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type batch_size() :: pos_integer().

-type record() ::
    atm_list_store_container_iterator:record() |
    atm_range_store_container_iterator:record() |
    atm_single_value_store_container_iterator:record() |
    atm_tree_forest_store_container_iterator:record() |
    atm_audit_log_store_container_iterator:record().

-export_type([batch_size/0, record/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback get_next_batch(atm_workflow_execution_ctx:record(), batch_size(), record()) ->
    {ok, [atm_value:compressed()], record()} | stop.

-callback forget_before(record()) -> ok.

-callback mark_exhausted(record()) -> ok.


%%%===================================================================
%%% API
%%%===================================================================


-spec get_next_batch(atm_workflow_execution_ctx:record(), batch_size(), record()) ->
    {ok, [atm_value:compressed()], record()} | stop.
get_next_batch(AtmWorkflowExecutionCtx, BatchSize, AtmStoreContainerIterator) ->
    Module = utils:record_type(AtmStoreContainerIterator),
    Module:get_next_batch(AtmWorkflowExecutionCtx, BatchSize, AtmStoreContainerIterator).


-spec forget_before(record()) -> ok.
forget_before(AtmStoreContainerIterator) ->
    Module = utils:record_type(AtmStoreContainerIterator),
    Module:forget_before(AtmStoreContainerIterator).


-spec mark_exhausted(record()) -> ok.
mark_exhausted(AtmStoreContainerIterator) ->
    Module = utils:record_type(AtmStoreContainerIterator),
    Module:mark_exhausted(AtmStoreContainerIterator).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(AtmStoreContainerIterator, NestedRecordEncoder) ->
    RecordType = utils:record_type(AtmStoreContainerIterator),

    maps:merge(
        #{<<"_type">> => atom_to_binary(RecordType, utf8)},
        NestedRecordEncoder(AtmStoreContainerIterator, RecordType)
    ).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"_type">> := RecordTypeJson} = AtmStoreContainerIteratorJson, NestedRecordDecoder) ->
    RecordType = binary_to_atom(RecordTypeJson, utf8),
    NestedRecordDecoder(AtmStoreContainerIteratorJson, RecordType).
