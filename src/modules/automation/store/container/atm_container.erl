%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines `atm_container` interface - an object which can be
%%% used to store and retrieve data of specific type.
%%%
%%%                             !!! Caution !!!
%%% 1) This behaviour must be implemented by modules with records of the same name.
%%% 2) Modules implementing this behaviour must also implement `persistent_record`
%%%    behaviour.
%%% 3) Modules implementing this behaviour must be registered in
%%%    `atm_store_api:store_type_to_container_type` function.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_container).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

%% API
-export([create/3, get_data_spec/1, acquire_iterator/1, update/4, delete/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type type() ::
    atm_single_value_container |
    atm_range_container |
    atm_list_container.

-type initial_value() ::
    atm_single_value_container:initial_value() | 
    atm_range_container:initial_value() |
    atm_list_container:initial_value().

-type record() ::
    atm_single_value_container:record() | 
    atm_range_container:record() |
    atm_list_container:record().

-type update_operation() :: append | set.

-type update_options() ::
    atm_single_value_container:update_options() |
    atm_range_container:update_options() |
    atm_list_container:update_options().

-export_type([type/0, initial_value/0, record/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback create(atm_data_spec:record(), initial_value()) -> record().

-callback get_data_spec(record()) -> atm_data_spec:record().

-callback acquire_iterator(record()) -> atm_container_iterator:record().

-callback update(record(), update_operation(), update_options(), atm_api:item()) -> 
    record() | no_return().

-callback delete(record()) -> ok | no_return().


%%%===================================================================
%%% API
%%%===================================================================


-spec create(type(), atm_data_spec:record(), initial_value()) -> record().
create(RecordType, AtmDataSpec, InitArgs) ->
    RecordType:create(AtmDataSpec, InitArgs).


-spec get_data_spec(record()) -> atm_data_spec:record().
get_data_spec(AtmContainer) ->
    RecordType = utils:record_type(AtmContainer),
    RecordType:get_data_spec(AtmContainer).


-spec acquire_iterator(record()) -> atm_container_iterator:record().
acquire_iterator(AtmContainer) ->
    RecordType = utils:record_type(AtmContainer),
    RecordType:acquire_iterator(AtmContainer).


-spec update(record(), update_operation(), update_options(), json_utils:json_term()) ->
    record() | no_return().
update(AtmContainer, Operation, Options, Item) ->
    RecordType = utils:record_type(AtmContainer),
    RecordType:update(AtmContainer, Operation, Options, Item).


-spec delete(record()) -> ok | no_return().
delete(AtmContainer) ->
    RecordType = utils:record_type(AtmContainer),
    RecordType:delete(AtmContainer).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(AtmContainer, NestedRecordEncoder) ->
    RecordType = utils:record_type(AtmContainer),

    maps:merge(
        #{<<"_type">> => atom_to_binary(RecordType, utf8)},
        NestedRecordEncoder(AtmContainer, RecordType)
    ).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"_type">> := RecordTypeJson} = AtmContainerJson, NestedRecordDecoder) ->
    RecordType = binary_to_atom(RecordTypeJson, utf8),
    NestedRecordDecoder(AtmContainerJson, RecordType).
