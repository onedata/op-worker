%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `atm_store_container_iterator` functionality for
%%% `atm_range_store_container`.
%%%
%%%                             !!! Caution !!!
%%% This iterator snapshots store container's range at creation time so that
%%% even if range kept in container changes the iterator will still return
%%% integers from previous range.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_range_store_container_iterator).
-author("Bartosz Walkowicz").

-behaviour(atm_store_container_iterator).
-behaviour(persistent_record).

-include_lib("ctool/include/automation/automation.hrl").

%% API
-export([build/3]).

% atm_store_container_iterator callbacks
-export([get_next_batch/3, forget_before/1, mark_exhausted/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_range_store_container_iterator, {
    current_num :: integer(),
    inclusive_start :: integer(),
    exclusive_end :: integer(),
    step :: integer()
}).
-type record() :: #atm_range_store_container_iterator{}.

-export_type([record/0]).


-define(ITEM_DATA_SPEC, #atm_data_spec{type = atm_integer_type}).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(integer(), integer(), integer()) -> record().
build(InclusiveStart, ExclusiveEnd, Step) ->
    #atm_range_store_container_iterator{
        current_num = InclusiveStart,
        inclusive_start = InclusiveStart,
        exclusive_end = ExclusiveEnd,
        step = Step
    }.


%%%===================================================================
%%% atm_store_container_iterator callbacks
%%%===================================================================


-spec get_next_batch(
    atm_workflow_execution_auth:record(),
    atm_store_container_iterator:batch_size(),
    record()
) ->
    {ok, [integer()], record()} | stop.
get_next_batch(AtmWorkflowExecutionAuth, BatchSize, Record = #atm_range_store_container_iterator{
    current_num = CurrentNum,
    exclusive_end = ExclusiveEnd,
    step = Step
}) ->
    RequestedEndNum = CurrentNum + (BatchSize - 1) * Step,
    Threshold = case Step > 0 of
        true -> min(RequestedEndNum, ExclusiveEnd - 1);
        false -> max(RequestedEndNum, ExclusiveEnd + 1)
    end,
    case lists:seq(CurrentNum, Threshold, Step) of
        [] ->
            stop;
        CompressedItems ->
            Batch = atm_value:filterexpand_list(
                AtmWorkflowExecutionAuth, CompressedItems, ?ITEM_DATA_SPEC
            ),
            {ok, Batch, Record#atm_range_store_container_iterator{current_num = Threshold + Step}}
    end.


-spec forget_before(record()) -> ok.
forget_before(_Record) ->
    ok.


-spec mark_exhausted(record()) -> ok.
mark_exhausted(_Record) ->
    ok.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_range_store_container_iterator{
    current_num = Current,
    inclusive_start = InclusiveStart,
    exclusive_end = ExclusiveEnd,
    step = Step
}, _NestedRecordEncoder) ->
    #{
        <<"current">> => Current,
        <<"inclusive_start">> => InclusiveStart,
        <<"exclusive_end">> => ExclusiveEnd,
        <<"step">> => Step
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"current">> := Current,
    <<"inclusive_start">> := InclusiveStart,
    <<"exclusive_end">> := ExclusiveEnd,
    <<"step">> := Step
}, _NestedRecordDecoder) ->
    #atm_range_store_container_iterator{
        current_num = Current,
        inclusive_start = InclusiveStart,
        exclusive_end = ExclusiveEnd,
        step = Step
    }.
