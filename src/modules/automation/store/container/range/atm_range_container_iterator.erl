%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `atm_container_iterator` functionality for
%%% `atm_range_container`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_range_container_iterator).
-author("Bartosz Walkowicz").

-behaviour(atm_container_iterator).
-behaviour(persistent_record).

-include_lib("ctool/include/errors.hrl").

%% API
-export([build/3]).

% atm_container_iterator callbacks
-export([get_next_batch/2, jump_to/2]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_range_container_iterator, {
    curr_num :: integer(),
    start_num :: integer(),
    end_num :: integer(),
    step :: integer()
}).
-type record() :: #atm_range_container_iterator{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(integer(), integer(), integer()) -> record().
build(Start, End, Step) ->
    #atm_range_container_iterator{
        curr_num = Start,
        start_num = Start, end_num = End, step = Step
    }.


%%%===================================================================
%%% atm_container_iterator callbacks
%%%===================================================================


-spec get_next_batch(atm_container_iterator:batch_size(), record()) ->
    {ok, [atm_api:item()], iterator:cursor(), record()} | stop.
get_next_batch(BatchSize, #atm_range_container_iterator{
    curr_num = CurrNum,
    end_num = End,
    step = Step
} = AtmContainerIterator) ->
    RequestedEndNum = CurrNum + (BatchSize - 1) * Step,
    Threshold = case Step > 0 of
        true -> min(RequestedEndNum, End);
        false -> max(RequestedEndNum, End)
    end,
    case lists:seq(CurrNum, Threshold, Step) of
        [] ->
            stop;
        Items ->
            NewCurrNum = Threshold + Step,
            NewAtmContainerIterator = AtmContainerIterator#atm_range_container_iterator{
                curr_num = NewCurrNum
            },
            {ok, Items, integer_to_binary(NewCurrNum), NewAtmContainerIterator}
    end.


-spec jump_to(iterator:cursor(), record()) -> record().
jump_to(<<>>, #atm_range_container_iterator{start_num = Start} = AtmContainerIterator) ->
    AtmContainerIterator#atm_range_container_iterator{curr_num = Start};
jump_to(Cursor, AtmContainerIterator) ->
    AtmContainerIterator#atm_range_container_iterator{
        curr_num = sanitize_cursor(Cursor, AtmContainerIterator)
    }.


%% @private
-spec sanitize_cursor(iterator:cursor(), record()) -> integer() | no_return().
sanitize_cursor(Cursor, AtmContainerIterator) ->
    try
        CursorInt = binary_to_integer(Cursor),
        true = is_in_proper_range(CursorInt, AtmContainerIterator),
        CursorInt
    catch _:_ ->
        throw(?EINVAL)
    end.


%% @private
-spec is_in_proper_range(integer(), record()) -> boolean().
is_in_proper_range(Num, #atm_range_container_iterator{
    start_num = Start,
    end_num = End,
    step = Step
}) ->
    (Num - Start) rem Step == 0 andalso case Step > 0 of
        true -> Num >= Start andalso Num =< End + Step;
        false -> Num =< Start andalso Num >= End + Step
    end.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_range_container_iterator{
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
    #atm_range_container_iterator{curr_num = Current, start_num = Start, end_num = End, step = Step}.
