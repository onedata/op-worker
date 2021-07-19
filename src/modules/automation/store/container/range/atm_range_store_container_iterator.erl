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

-include_lib("ctool/include/errors.hrl").

%% API
-export([build/3]).

% atm_store_container_iterator callbacks
-export([get_next_batch/4, forget_before/1, mark_exhausted/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_range_store_container_iterator, {
    curr_num :: integer(),
    start_num :: integer(),
    end_num :: integer(),
    step :: integer()
}).
-type record() :: #atm_range_store_container_iterator{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(integer(), integer(), integer()) -> record().
build(Start, End, Step) ->
    #atm_range_store_container_iterator{
        curr_num = Start,
        start_num = Start, end_num = End, step = Step
    }.


%%%===================================================================
%%% atm_store_container_iterator callbacks
%%%===================================================================


-spec get_next_batch(atm_workflow_execution_ctx:record(), atm_store_container_iterator:batch_size(), 
    record(), atm_data_spec:record()
) ->
    {ok, [integer()], record()} | stop.
get_next_batch(AtmWorkflowExecutionCtx, BatchSize, #atm_range_store_container_iterator{
    curr_num = CurrNum,
    end_num = End,
    step = Step
} = Record, DataSpec) ->
    RequestedEndNum = CurrNum + (BatchSize - 1) * Step,
    Threshold = case Step > 0 of
        true -> min(RequestedEndNum, End);
        false -> max(RequestedEndNum, End)
    end,
    case lists:seq(CurrNum, Threshold, Step) of
        [] ->
            stop;
        CompressedItems ->
            NewCurrNum = Threshold + Step,
            NewRecord = Record#atm_range_store_container_iterator{curr_num = NewCurrNum},
            {ok, atm_value:filterexpand_list(AtmWorkflowExecutionCtx, CompressedItems, DataSpec), NewRecord}
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
    curr_num = Current,
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
    #atm_range_store_container_iterator{curr_num = Current, start_num = Start, end_num = End, step = Step}.
