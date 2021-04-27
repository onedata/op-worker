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

%% API
-export([init/3]).

% atm_data_stream callbacks
-export([
    get_next_batch/2,
    jump_to/2,
    to_json/1,
    from_json/1
]).


-record(atm_range_data_stream, {
    start_num :: integer(),
    end_num :: integer(),
    step :: integer()
}).
-type stream() :: #atm_range_data_stream{}.

-type marker() :: binary().
-type item() :: integer().


-export_type([stream/0, marker/0, item/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(integer(), integer(), integer()) -> stream().
init(StartNum, EndNum, Step) ->
    #atm_range_data_stream{start_num = StartNum, end_num = EndNum, step = Step}.


%%%===================================================================
%%% atm_data_stream callbacks
%%%===================================================================


-spec get_next_batch(pos_integer(), stream()) ->
    {ok, [item()], marker(), stream()} | stop.
get_next_batch(Size, #atm_range_data_stream{
    start_num = StartNum,
    end_num = EndNum,
    step = Step
} = AtmDataStream) ->
    case lists:seq(StartNum, EndNum, Step) of
        [] ->
            stop;
        Items ->
            NewStartNum = StartNum + Size * Step,
            {ok, Items, integer_to_binary(NewStartNum), AtmDataStream#atm_range_data_stream{
                start_num = NewStartNum
            }}
    end.


-spec jump_to(marker(), stream()) -> stream().
jump_to(StartNumBin, AtmDataStream) ->
    AtmDataStream#atm_range_data_stream{start_num = binary_to_integer(StartNumBin)}.


-spec to_json(stream()) -> json_utils:json_map().
to_json(#atm_range_data_stream{start_num = StartNum, end_num = EndNum, step = Step}) ->
    #{<<"start">> => StartNum, <<"end">> => EndNum, <<"step">> => Step}.


-spec from_json(json_utils:json_map()) -> stream().
from_json(#{<<"start">> := StartNum, <<"end">> := EndNum, <<"step">> := Step}) ->
    #atm_range_data_stream{start_num = StartNum, end_num = EndNum, step = Step}.
