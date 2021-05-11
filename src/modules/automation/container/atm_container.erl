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
%%% 1) This behaviour must be implemented by proper models, that is modules with
%%%    records of the same name.
%%% 2) Models implementing this behaviour must also implement `persistent_record`
%%%    behaviour.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_container).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

%% API
-export([create/3, get_data_spec/1, get_container_stream/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type model() ::
    atm_single_value_container |
    atm_range_container.

-type init_args() ::
    atm_single_value_container:init_args() |
    atm_range_container:init_args().

-type container() ::
    atm_single_value_container:container() |
    atm_range_container:container().


-export_type([model/0, init_args/0, container/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback create(atm_data_spec:record(), init_args()) -> container().

-callback get_data_spec(container()) -> atm_data_spec:record().

-callback get_container_stream(container()) -> atm_container_stream:stream().


%%%===================================================================
%%% API
%%%===================================================================


-spec create(model(), atm_data_spec:record(), init_args()) -> container().
create(Model, AtmDataSpec, InitArgs) ->
    Model:create(AtmDataSpec, InitArgs).


-spec get_data_spec(container()) -> atm_data_spec:record().
get_data_spec(AtmContainer) ->
    Model = utils:record_type(AtmContainer),
    Model:get_data_spec(AtmContainer).


-spec get_container_stream(container()) -> atm_container_stream:stream().
get_container_stream(AtmContainer) ->
    Model = utils:record_type(AtmContainer),
    Model:get_container_stream(AtmContainer).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(container(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(AtmContainer, NestedRecordEncoder) ->
    Model = utils:record_type(AtmContainer),

    maps:merge(
        #{<<"_type">> => atom_to_binary(Model, utf8)},
        NestedRecordEncoder(AtmContainer, Model)
    ).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    container().
db_decode(#{<<"_type">> := TypeJson} = AtmContainerJson, NestedRecordDecoder) ->
    Model = binary_to_atom(TypeJson, utf8),

    NestedRecordDecoder(AtmContainerJson, Model).
