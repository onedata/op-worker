%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `atm_data_stream` functionality for
%%% `atm_range_data_container`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_range_data_stream).
-author("Bartosz Walkowicz").

-behaviour(atm_data_stream).

-include_lib("ctool/include/errors.hrl").

%% API
-export([init/3]).

% atm_data_stream callbacks
-export([
    get_next_batch/2,
    jump_to/2,
    to_json/1,
    from_json/1
]).

-type item() :: integer().
-type marker() :: binary().

-record(atm_range_data_stream, {
    curr_num :: integer(),
    start_num :: integer(),
    end_num :: integer(),
    step :: integer()
}).
-type stream() :: #atm_range_data_stream{}.

-export_type([item/0, marker/0, stream/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(integer(), integer(), integer()) -> stream().
init(Start, End, Step) ->
    #atm_range_data_stream{
        curr_num = Start,
        start_num = Start, end_num = End, step = Step
    }.


%%%===================================================================
%%% atm_data_stream callbacks
%%%===================================================================


-spec get_next_batch(pos_integer(), stream()) ->
    {ok, [item()], marker(), stream()} | stop.
get_next_batch(Size, #atm_range_data_stream{
    curr_num = CurrNum,
    end_num = End,
    step = Step
} = AtmDataStream) ->
    RequestedEndNum = CurrNum + (Size - 1) * Step,
    Threshold = case Step > 0 of
        true -> min(RequestedEndNum, End);
        false -> max(RequestedEndNum, End)
    end,
    case lists:seq(CurrNum, Threshold, Step) of
        [] ->
            stop;
        Items ->
            NewCurrNum = Threshold + Step,
            {ok, Items, integer_to_binary(NewCurrNum), AtmDataStream#atm_range_data_stream{
                curr_num = NewCurrNum
            }}
    end.


-spec jump_to(marker(), stream()) -> stream().
jump_to(<<>>, #atm_range_data_stream{start_num = Start} = AtmDataStream) ->
    AtmDataStream#atm_range_data_stream{curr_num = Start};
jump_to(Marker, #atm_range_data_stream{
    start_num = Start,
    end_num = End,
    step = Step
} = AtmDataStream) ->
    NewCurrNum = binary_to_integer(Marker),

    IsValidMarker = (NewCurrNum - Start) rem Step == 0 andalso case Step > 0 of
        true -> NewCurrNum >= Start andalso NewCurrNum =< End + Step;
        false -> NewCurrNum =< Start andalso NewCurrNum >= End + Step
    end,
    case IsValidMarker of
        true ->
            AtmDataStream#atm_range_data_stream{curr_num = NewCurrNum};
        false ->
            throw(?EINVAL)
    end.


-spec to_json(stream()) -> json_utils:json_map().
to_json(#atm_range_data_stream{
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
    #atm_range_data_stream{curr_num = Current, start_num = Start, end_num = End, step = Step}.
