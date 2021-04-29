%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines `atm_data_stream` interface - an object which can be
%%% used to iterate over specific `atm_data_container` in batches.
%%%
%%%                             !!! Caution !!!
%%% 1) This behaviour must be implemented by proper models, that is modules with
%%%    records of the same name.
%%% 2) The data stream behaviour in case of changes to values kept in container
%%%    is not defined and implementation dependent (it may e.g. return old values).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_data_stream).
-author("Bartosz Walkowicz").

%% API
-export([
    get_next_batch/2,
    jump_to/2,
    to_json/1,
    from_json/1
]).


-type stream() ::
    atm_single_value_data_stream:stream() |
    atm_range_data_stream:stream().

% Pointer to specific location in container so that it is possible to shift
% stream and begin streaming from this position.
-type marker() ::
    atm_single_value_data_stream:marker() |
    atm_range_data_stream:marker().

-type item() ::
    atm_single_value_data_stream:item() |
    atm_range_data_stream:item().


-export_type([stream/0, marker/0, item/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback get_next_batch(Size :: pos_integer(), stream()) ->
    {ok, [item()], marker(), stream()} | stop.

-callback jump_to(marker(), stream()) -> stream().

-callback to_json(stream()) -> json_utils:json_map().

-callback from_json(json_utils:json_map()) -> stream().


%%%===================================================================
%%% API
%%%===================================================================


-spec get_next_batch(pos_integer(), stream()) ->
    {ok, [item()], marker(), stream()} | stop.
get_next_batch(Size, AtmDataStream) ->
    Module = utils:record_type(AtmDataStream),
    Module:get_next_batch(Size, AtmDataStream).


-spec jump_to(marker(), stream()) -> stream().
jump_to(Marker, AtmDataStream) ->
    Module = utils:record_type(AtmDataStream),
    Module:jump_to(Marker, AtmDataStream).


-spec to_json(stream()) -> json_utils:json_map().
to_json(AtmDataStream) ->
    Module = utils:record_type(AtmDataStream),
    AtmDataContainerJson = Module:to_json(AtmDataStream),
    AtmDataContainerJson#{<<"_type">> => atom_to_binary(Module, utf8)}.


-spec from_json(json_utils:json_map()) -> stream().
from_json(AtmDataStreamJson) ->
    {ModuleBin, AtmDataStreamJson2} = maps:take(<<"_type">>, AtmDataStreamJson),
    Module = binary_to_atom(ModuleBin, utf8),
    Module:from_json(AtmDataStreamJson2).
