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
%%% 3) Models implementing this behaviour must also implement `persistent_record`
%%%    behaviour.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_container_stream).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

%% API
-export([get_next_batch/2, jump_to/2]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


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


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(stream(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(AtmContainerStream, NestedRecordEncoder) ->
    Model = utils:record_type(AtmContainerStream),

    maps:merge(
        #{<<"_type">> => atom_to_binary(Model, utf8)},
        NestedRecordEncoder(AtmContainerStream, Model)
    ).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    stream().
db_decode(#{<<"_type">> := TypeJson} = AtmContainerStreamJson, NestedRecordDecoder) ->
    Model = binary_to_atom(TypeJson, utf8),

    NestedRecordDecoder(AtmContainerStreamJson, Model).
