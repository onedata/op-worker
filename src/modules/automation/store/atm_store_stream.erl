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
-module(atm_store_stream).
-author("Bartosz Walkowicz").

-behaviour(iterator).

-include("modules/automation/atm_tmp.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([init/2]).

%% iterator callbacks
-export([
    get_next/1,
    jump_to/2,
    to_json/1,
    from_json/1
]).


-record(atm_store_stream, {
    mode :: atm_store_stream_mode(),
    data_spec :: atm_data_spec:spec(),
    data_stream :: atm_data_stream:stream()
}).
-type stream() :: #atm_store_stream{}.
-type marker() :: atm_data_stream:marker().
-type item() :: atm_data_stream:item().

-export_type([stream/0, marker/0, item/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(atm_store_stream_schema(), atm_store:record()) -> stream().
init(AtmStoreStreamSchema, #atm_store{container = AtmDataContainer}) ->
    #atm_store_stream{
        mode = AtmStoreStreamSchema#atm_store_stream_schema.mode,
        data_spec = atm_data_container:get_data_spec(AtmDataContainer),
        data_stream = atm_data_container:get_data_stream(AtmDataContainer)
    }.


%%%===================================================================
%%% Iterator callbacks
%%%===================================================================


-spec get_next(stream()) -> {ok, item(), marker(), stream()} | stop.
get_next(#atm_store_stream{
    mode = #serial_mode{},
    data_stream = AtmDataStream
} = AtmStoreStream) ->
    case atm_data_stream:get_next_batch(1, AtmDataStream) of
        stop ->
            stop;
        {ok, [Item], Marker, NewAtmDataStream} ->
            {ok, Item, Marker, AtmStoreStream#atm_store_stream{
                data_stream = NewAtmDataStream
            }}
    end;
get_next(#atm_store_stream{
    mode = #bulk_mode{size = Size},
    data_stream = AtmDataStream
} = AtmStoreStream) ->
    case atm_data_stream:get_next_batch(Size, AtmDataStream) of
        stop ->
            stop;
        {ok, Items, Marker, NewAtmDataStream} ->
            {ok, Items, Marker, AtmStoreStream#atm_store_stream{
                data_stream = NewAtmDataStream
            }}
    end.


-spec jump_to(marker(), stream()) -> stream().
jump_to(Marker, #atm_store_stream{data_stream = AtmDataStream} = AtmStoreStream) ->
    AtmStoreStream#atm_store_stream{
        data_stream = atm_data_stream:jump_to(Marker, AtmDataStream)
    }.


-spec to_json(stream()) -> json_utils:json_map().
to_json(#atm_store_stream{
    mode = Mode,
    data_spec = AtmDataSpec,
    data_stream = AtmDataStream
}) ->
    #{
        <<"mode">> => mode_to_json(Mode),
        <<"dataSpec">> => atm_data_spec:to_json(AtmDataSpec),
        <<"dataStream">> => atm_data_stream:to_json(AtmDataStream)
    }.


-spec from_json(json_utils:json_map()) -> stream().
from_json(#{
    <<"mode">> := ModeJson,
    <<"dataSpec">> := AtmDataSpecJson,
    <<"dataStream">> := AtmDataStreamJson
}) ->
    #atm_store_stream{
        mode = mode_from_json(ModeJson),
        data_spec = atm_data_spec:from_json(AtmDataSpecJson),
        data_stream = atm_data_stream:from_json(AtmDataStreamJson)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec mode_to_json(atm_store_stream_mode()) -> json_utils:json_map().
mode_to_json(#serial_mode{}) ->
    #{<<"type">> => <<"serial">>};
mode_to_json(#bulk_mode{size = Size}) ->
    #{<<"type">> => <<"bulk">>, <<"size">> => Size}.


%% @private
-spec mode_from_json(json_utils:json_map()) -> atm_store_stream_mode().
mode_from_json(#{<<"type">> := <<"serial">>}) ->
    #serial_mode{};
mode_from_json(#{<<"type">> := <<"bulk">>, <<"size">> := Size}) ->
    #bulk_mode{size = Size}.
