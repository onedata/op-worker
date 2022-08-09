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
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([build/2]).

%% iterator callbacks
-export([get_next/2, forget_before/1, mark_exhausted/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_store_iterator, {
    spec :: atm_store_iterator_spec:record(),
    container_iterator :: atm_store_container_iterator:record()
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
        container_iterator = atm_store_container:acquire_iterator(AtmStoreContainer)
    }.


%%%===================================================================
%%% Iterator callbacks
%%%===================================================================


-spec get_next(atm_workflow_execution_env:record(), record()) -> 
    {ok, [atm_value:expanded()], record()} | stop.
get_next(AtmWorkflowExecutionEnv, AtmStoreIterator = #atm_store_iterator{
    spec = #atm_store_iterator_spec{max_batch_size = Size},
    container_iterator = ContainerIterator
}) ->
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(AtmWorkflowExecutionEnv),
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),

    case get_next_internal(AtmWorkflowExecutionAuth, ContainerIterator, Size) of
        stop ->
            stop;
        {ok, Items, NewAtmStoreContainerIterator} ->
            {ok, Items, AtmStoreIterator#atm_store_iterator{
                container_iterator = NewAtmStoreContainerIterator
            }}
    end.


-spec forget_before(record()) -> ok.
forget_before(#atm_store_iterator{container_iterator = ContainerIterator}) ->
    atm_store_container_iterator:forget_before(ContainerIterator).


-spec mark_exhausted(record()) -> ok.
mark_exhausted(#atm_store_iterator{container_iterator = ContainerIterator}) ->
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
    container_iterator = AtmStoreContainerIterator
}, NestedRecordEncoder) ->
    #{
        <<"spec">> => NestedRecordEncoder(AtmStoreIteratorSpec, atm_store_iterator_spec),
        <<"containerIterator">> => NestedRecordEncoder(
            AtmStoreContainerIterator, atm_store_container_iterator
        )
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"spec">> := AtmStoreIteratorSpecJson,
    <<"containerIterator">> := AtmStoreContainerIteratorJson
}, NestedRecordDecoder) ->
    #atm_store_iterator{
        spec = NestedRecordDecoder(AtmStoreIteratorSpecJson, atm_store_iterator_spec),
        container_iterator = NestedRecordDecoder(
            AtmStoreContainerIteratorJson, atm_store_container_iterator
        )
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_next_internal(
    atm_workflow_execution_auth:record(),
    atm_store_container_iterator:record(),
    pos_integer()
) ->
    {ok, [atm_value:expanded()], atm_store_container_iterator:record()} | stop.
get_next_internal(AtmWorkflowExecutionAuth, AtmStoreContainerIterator, Size) ->
    case atm_store_container_iterator:get_next_batch(
        AtmWorkflowExecutionAuth, Size, AtmStoreContainerIterator
    ) of
        stop ->
            stop;
        {ok, [], NewAtmStoreContainerIterator} ->
            get_next_internal(AtmWorkflowExecutionAuth, NewAtmStoreContainerIterator, Size);
        {ok, _Items, _NewAtmStoreContainerIterator} = Result ->
            Result
    end.
