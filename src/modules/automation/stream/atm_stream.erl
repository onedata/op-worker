%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `iterator` functionality for `atm_container`
%%% extending it with additional features like bulk mode or filtering.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_stream).
-author("Bartosz Walkowicz").

-behaviour(iterator).
-behaviour(persistent_record).

-include("modules/automation/atm_tmp.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([init/2]).

%% iterator callbacks
-export([get_next/1, jump_to/2]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_stream, {
    mode :: atm_stream_mode(),
    data_spec :: atm_data_spec:record(),
    container_stream :: atm_container_stream:stream()
}).
-type stream() :: #atm_stream{}.
-type item() :: json_utils:json_term().

-export_type([stream/0, item/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(atm_stream_schema(), atm_container:container()) -> stream().
init(AtmStreamSchema, AtmContainer) ->
    #atm_stream{
        mode = AtmStreamSchema#atm_stream_schema.mode,
        data_spec = atm_container:get_data_spec(AtmContainer),
        container_stream = atm_container:get_container_stream(AtmContainer)
    }.


%%%===================================================================
%%% Iterator callbacks
%%%===================================================================


-spec get_next(stream()) -> {ok, item(), iterator:cursor(), stream()} | stop.
get_next(#atm_stream{
    mode = #serial_mode{},
    container_stream = AtmContainerStream
} = AtmStream) ->
    case atm_container_stream:get_next_batch(1, AtmContainerStream) of
        stop ->
            stop;
        {ok, [Item], Marker, NewAtmContainerStream} ->
            {ok, Item, Marker, AtmStream#atm_stream{
                container_stream = NewAtmContainerStream
            }}
    end;
get_next(#atm_stream{
    mode = #bulk_mode{size = Size},
    container_stream = AtmContainerStream
} = AtmStream) ->
    case atm_container_stream:get_next_batch(Size, AtmContainerStream) of
        stop ->
            stop;
        {ok, Items, Marker, NewAtmContainerStream} ->
            {ok, Items, Marker, AtmStream#atm_stream{
                container_stream = NewAtmContainerStream
            }}
    end.


-spec jump_to(iterator:cursor(), stream()) -> stream().
jump_to(Marker, #atm_stream{container_stream = AtmContainerStream} = AtmStream) ->
    AtmStream#atm_stream{
        container_stream = atm_container_stream:jump_to(Marker, AtmContainerStream)
    }.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(stream(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_stream{
    mode = Mode,
    data_spec = AtmDataSpec,
    container_stream = AtmContainerStream
}, NestedRecordEncoder) ->
    #{
        <<"mode">> => mode_to_json(Mode),
        <<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec),
        <<"containerStream">> => NestedRecordEncoder(AtmContainerStream, atm_container_stream)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    stream().
db_decode(#{
    <<"mode">> := ModeJson,
    <<"dataSpec">> := AtmDataSpecJson,
    <<"containerStream">> := AtmContainerStreamJson
}, NestedRecordDecoder) ->
    #atm_stream{
        mode = mode_from_json(ModeJson),
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        container_stream = NestedRecordDecoder(AtmContainerStreamJson, atm_container_stream)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec mode_to_json(atm_stream_mode()) -> json_utils:json_map().
mode_to_json(#serial_mode{}) ->
    #{<<"type">> => <<"serial">>};
mode_to_json(#bulk_mode{size = Size}) ->
    #{<<"type">> => <<"bulk">>, <<"size">> => Size}.


%% @private
-spec mode_from_json(json_utils:json_map()) -> atm_stream_mode().
mode_from_json(#{<<"type">> := <<"serial">>}) ->
    #serial_mode{};
mode_from_json(#{<<"type">> := <<"bulk">>, <<"size">> := Size}) ->
    #bulk_mode{size = Size}.
