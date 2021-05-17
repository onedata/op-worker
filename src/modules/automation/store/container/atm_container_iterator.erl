%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines `atm_container_iterator` interface - an object which can be
%%% used to iterate over specific `atm_container` in batches.
%%%
%%%                             !!! Caution !!!
%%% 1) This behaviour must be implemented by modules with records of the same name.
%%% 2) Modules implementing this behaviour must also implement `persistent_record`
%%%    behaviour.
%%% 3) The container iterator behaviour in case of changes to values kept in container
%%%    is not defined and implementation dependent (it may e.g. return old values).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_container_iterator).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

%% API
-export([get_next_batch/2, jump_to/2]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type batch_size() :: pos_integer().

-type record() ::
    atm_single_value_container_iterator:record() |
    atm_list_container_iterator:record() |
    atm_range_container_iterator:record().

-export_type([batch_size/0, record/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback get_next_batch(batch_size(), record()) ->
    {ok, [atm_api:item()], iterator:cursor(), record()} | stop.

-callback jump_to(iterator:cursor(), record()) -> record().


%%%===================================================================
%%% API
%%%===================================================================


-spec get_next_batch(batch_size(), record()) ->
    {ok, [atm_api:item()], iterator:cursor(), record()} | stop.
get_next_batch(BatchSize, AtmContainerIterator) ->
    Module = utils:record_type(AtmContainerIterator),
    Module:get_next_batch(BatchSize, AtmContainerIterator).


-spec jump_to(iterator:cursor(), record()) -> record().
jump_to(Cursor, AtmContainerIterator) ->
    Module = utils:record_type(AtmContainerIterator),
    Module:jump_to(Cursor, AtmContainerIterator).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(AtmContainerIterator, NestedRecordEncoder) ->
    RecordType = utils:record_type(AtmContainerIterator),

    maps:merge(
        #{<<"_type">> => atom_to_binary(RecordType, utf8)},
        NestedRecordEncoder(AtmContainerIterator, RecordType)
    ).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"_type">> := RecordTypeJson} = AtmContainerIteratorJson, NestedRecordDecoder) ->
    RecordType = binary_to_atom(RecordTypeJson, utf8),
    NestedRecordDecoder(AtmContainerIteratorJson, RecordType).
