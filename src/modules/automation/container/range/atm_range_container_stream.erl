%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `atm_container_stream` functionality for
%%% `atm_range_container`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_range_container_stream).
-author("Bartosz Walkowicz").

-behaviour(atm_container_stream).

-include_lib("ctool/include/errors.hrl").

%% API
-export([init/3]).

% atm_container_stream callbacks
-export([
    get_next_batch/2,
    jump_to/2,
    to_json/1,
    from_json/1
]).

-type item() :: integer().

-record(atm_range_container_stream, {
    curr_num :: integer(),
    start_num :: integer(),
    end_num :: integer(),
    step :: integer()
}).
-type stream() :: #atm_range_container_stream{}.

-export_type([item/0, stream/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(integer(), integer(), integer()) -> stream().
init(Start, End, Step) ->
    #atm_range_container_stream{
        curr_num = Start,
        start_num = Start, end_num = End, step = Step
    }.


%%%===================================================================
%%% atm_data_stream callbacks
%%%===================================================================


-spec get_next_batch(atm_container_stream:batch_size(), stream()) ->
    {ok, [item()], iterator:cursor(), stream()} | stop.
get_next_batch(BatchSize, #atm_range_container_stream{
    curr_num = CurrNum,
    end_num = End,
    step = Step
} = AtmContainerStream) ->
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
            {ok, Items, integer_to_binary(NewCurrNum), AtmContainerStream#atm_range_container_stream{
                curr_num = NewCurrNum
            }}
    end.


-spec jump_to(iterator:cursor(), stream()) -> stream().
jump_to(<<>>, #atm_range_container_stream{start_num = Start} = AtmContainerStream) ->
    AtmContainerStream#atm_range_container_stream{curr_num = Start};
jump_to(Cursor, AtmContainerStream) ->
    AtmContainerStream#atm_range_container_stream{
        curr_num = sanitize_cursor(Cursor, AtmContainerStream)
    }.


%% @private
-spec sanitize_cursor(iterator:cursor(), stream()) -> boolean().
sanitize_cursor(Cursor, AtmContainerStream) ->
    try
        CursorInt = binary_to_integer(Cursor),
        true = is_in_proper_range(CursorInt, AtmContainerStream),
        CursorInt
    catch _:_ ->
        throw(?EINVAL)
    end.


%% @private
-spec is_in_proper_range(integer(), stream()) -> boolean().
is_in_proper_range(Num, #atm_range_container_stream{
    start_num = Start,
    end_num = End,
    step = Step
}) ->
    (Num - Start) rem Step == 0 andalso case Step > 0 of
        true -> Num >= Start andalso Num =< End + Step;
        false -> Num =< Start andalso Num >= End + Step
    end.


-spec to_json(stream()) -> json_utils:json_map().
to_json(#atm_range_container_stream{
    curr_num = Current,
    start_num = Start,
    end_num = End,
    step = Step
}) ->
    #{<<"current">> => Current, <<"start">> => Start, <<"end">> => End, <<"step">> => Step}.


-spec from_json(json_utils:json_map()) -> stream().
from_json(#{
    <<"current">> := Current,
    <<"start">> := Start,
    <<"end">> := End,
    <<"step">> := Step
}) ->
    #atm_range_container_stream{curr_num = Current, start_num = Start, end_num = End, step = Step}.
