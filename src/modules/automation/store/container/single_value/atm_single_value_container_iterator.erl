%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `atm_container_iterator` functionality for
%%% `atm_single_value_container`.
%%%
%%%                             !!! Caution !!!
%%% This iterator snapshots container's value at creation time so that even if
%%% value kept in container changes the iterator will still return the same
%%% old value.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_single_value_container_iterator).
-author("Bartosz Walkowicz").

-behaviour(atm_container_iterator).
-behaviour(persistent_record).

-include_lib("ctool/include/errors.hrl").

%% API
-export([build/1]).

% atm_container_iterator callbacks
-export([get_next_batch/3, mark_exhausted/2]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_single_value_container_iterator, {
    value :: undefined | atm_api:item(),
    exhausted = false :: boolean()
}).
-type record() :: #atm_single_value_container_iterator{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(undefined | atm_api:item()) -> record().
build(Value) ->
    #atm_single_value_container_iterator{value = Value, exhausted = false}.


%%%===================================================================
%%% atm_container_iterator callbacks
%%%===================================================================


-spec get_next_batch(atm_container_iterator:batch_size(), atm_workflow_execution_ctx:record(), record()) ->
    {ok, [atm_api:item()], record()} | stop.
get_next_batch(_AtmWorkflowExecutionCtx, _BatchSize, #atm_single_value_container_iterator{value = undefined}) ->
    stop;
get_next_batch(_AtmWorkflowExecutionCtx, _BatchSize, #atm_single_value_container_iterator{exhausted = true}) ->
    stop;
get_next_batch(_AtmWorkflowExecutionCtx, _BatchSize, #atm_single_value_container_iterator{value = Value} = AtmContainerIterator) ->
    {ok, [Value], AtmContainerIterator#atm_single_value_container_iterator{
        exhausted = true
    }}.


-spec mark_exhausted(atm_workflow_execution_ctx:record(), record()) -> ok.
mark_exhausted(_AtmWorkflowExecutionCtx, _AtmContainerIterator) ->
    ok.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_single_value_container_iterator{
    value = Value,
    exhausted = Exhausted
}, _NestedRecordEncoder) ->
    maps_utils:put_if_defined(#{<<"exhausted">> => Exhausted}, <<"value">>, Value).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"exhausted">> := Exhausted} = AtmContainerIteratorJson, _NestedRecordDecoder) ->
    #atm_single_value_container_iterator{
        value = maps:get(<<"value">>, AtmContainerIteratorJson, undefined),
        exhausted = Exhausted
    }.
