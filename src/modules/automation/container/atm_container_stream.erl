%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines `atm_container_stream` interface - an object which can be
%%% used to iterate over specific `atm_container` in batches.
%%%
%%%                             !!! Caution !!!
%%% 1) This behaviour must be implemented by proper models, that is modules with
%%%    records of the same name.
%%% 2) The container stream behaviour in case of changes to values kept in container
%%%    is not defined and implementation dependent (it may e.g. return old values).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_container_stream).
-author("Bartosz Walkowicz").

%% API
-export([
    get_next_batch/2,
    jump_to/2,
    to_json/1,
    from_json/1
]).


-type stream() ::
    atm_single_value_container_stream:stream() |
    atm_range_container_stream:stream().

-type batch_size() :: pos_integer().

-type item() ::
    atm_single_value_container_stream:item() |
    atm_range_container_stream:item().


-export_type([stream/0, batch_size/0, item/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback get_next_batch(batch_size(), stream()) ->
    {ok, [item()], iterator:cursor(), stream()} | stop.

-callback jump_to(iterator:cursor(), stream()) -> stream().

-callback to_json(stream()) -> json_utils:json_map().

-callback from_json(json_utils:json_map()) -> stream().


%%%===================================================================
%%% API
%%%===================================================================


-spec get_next_batch(batch_size(), stream()) ->
    {ok, [item()], iterator:cursor(), stream()} | stop.
get_next_batch(BatchSize, AtmContainerStream) ->
    Module = utils:record_type(AtmContainerStream),
    Module:get_next_batch(BatchSize, AtmContainerStream).


-spec jump_to(iterator:cursor(), stream()) -> stream().
jump_to(Cursor, AtmContainerStream) ->
    Module = utils:record_type(AtmContainerStream),
    Module:jump_to(Cursor, AtmContainerStream).


-spec to_json(stream()) -> json_utils:json_map().
to_json(AtmContainerStream) ->
    Module = utils:record_type(AtmContainerStream),
    AtmContainerStreamJson = Module:to_json(AtmContainerStream),
    AtmContainerStreamJson#{<<"_type">> => atom_to_binary(Module, utf8)}.


-spec from_json(json_utils:json_map()) -> stream().
from_json(AtmContainerStreamJson) ->
    {ModuleBin, AtmContainerStreamJson2} = maps:take(<<"_type">>, AtmContainerStreamJson),
    Module = binary_to_atom(ModuleBin, utf8),
    Module:from_json(AtmContainerStreamJson2).
