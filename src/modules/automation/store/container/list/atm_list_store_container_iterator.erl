%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `atm_store_container_iterator` functionality for
%%% `atm_list_store_container`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_list_store_container_iterator).
-author("Michal Stanisz").

-behaviour(atm_store_container_iterator).
-behaviour(persistent_record).

-include_lib("ctool/include/errors.hrl").

%% API
-export([build/1]).

% atm_store_container_iterator callbacks
-export([get_next_batch/3, forget_before/1, mark_exhausted/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_list_store_container_iterator, {
    atm_infinite_log_container_iterator :: atm_infinite_log_container_iterator:record()
}).
-type record() :: #atm_list_store_container_iterator{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(atm_infinite_log_container_iterator:record()) -> record().
build(AtmInfiniteLogContainerIterator) ->
    #atm_list_store_container_iterator{
        atm_infinite_log_container_iterator = AtmInfiniteLogContainerIterator
    }.


%%%===================================================================
%%% atm_store_container_iterator callbacks
%%%===================================================================


-spec get_next_batch(atm_workflow_execution_ctx:record(), atm_store_container_iterator:batch_size(), record()) ->
    {ok, [atm_value:compressed()], record()} | stop.
get_next_batch(AtmWorkflowExecutionCtx, BatchSize, #atm_list_store_container_iterator{
    atm_infinite_log_container_iterator = AtmInfiniteLogContainerIterator
} = AtmListStoreContainerIterator) ->
    case atm_infinite_log_container_iterator:get_next_batch(
        AtmWorkflowExecutionCtx, BatchSize, AtmInfiniteLogContainerIterator
    ) of
        stop ->
            stop;
        {ok, Items, NewAtmInfiniteLogContainerIterator} ->
            {ok, Items, AtmListStoreContainerIterator#atm_list_store_container_iterator{
                atm_infinite_log_container_iterator = NewAtmInfiniteLogContainerIterator
            }}
    end.


-spec forget_before(record()) -> ok.
forget_before(#atm_list_store_container_iterator{
    atm_infinite_log_container_iterator = AtmInfiniteLogContainerIterator
}) ->
    atm_infinite_log_container_iterator:forget_before(AtmInfiniteLogContainerIterator).


-spec mark_exhausted(record()) -> ok.
mark_exhausted(#atm_list_store_container_iterator{
    atm_infinite_log_container_iterator = AtmInfiniteLogContainerIterator
}) ->
    atm_infinite_log_container_iterator:mark_exhausted(AtmInfiniteLogContainerIterator).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_list_store_container_iterator{
    atm_infinite_log_container_iterator = AtmInfiniteLogContainerIterator
}, NestedRecordEncoder) ->
    #{
        <<"atmInfiniteLogContainerIterator">> => NestedRecordEncoder(
            AtmInfiniteLogContainerIterator, atm_infinite_log_container_iterator
        )
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"atmInfiniteLogContainer">> := AtmInfiniteLogContainerIteratorJson}, NestedRecordDecoder) ->
    #atm_list_store_container_iterator{
        atm_infinite_log_container_iterator = NestedRecordDecoder(
            AtmInfiniteLogContainerIteratorJson, atm_infinite_log_container_iterator
        )
    }.
