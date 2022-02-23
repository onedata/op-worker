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
    start_num :: integer(),
    end_num :: integer(),
    step :: integer()
}).
-type record() :: #atm_range_store_container_iterator{}.

-export_type([record/0]).


-define(ITEM_DATA_SPEC, #atm_data_spec{type = atm_integer_type}).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(integer(), integer(), integer()) -> record().
build(Start, End, Step) ->
    #atm_range_store_container_iterator{
        current_num = Start,
        start_num = Start, end_num = End, step = Step
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
    end_num = End,
    step = Step
}) ->
    RequestedEndNum = CurrentNum + (BatchSize - 1) * Step,
    Threshold = case Step > 0 of
        true -> min(RequestedEndNum, End);
        false -> max(RequestedEndNum, End)
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
    start_num = Start,
    end_num = End,
    step = Step
}, _NestedRecordEncoder) ->
    #{<<"current">> => Current, <<"start">> => Start, <<"end">> => End, <<"step">> => Step}.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"current">> := Current,
    <<"start">> := Start,
    <<"end">> := End,
    <<"step">> := Step
}, _NestedRecordDecoder) ->
    #atm_range_store_container_iterator{
        current_num = Current,
        start_num = Start,
        end_num = End,
        step = Step
    }.
