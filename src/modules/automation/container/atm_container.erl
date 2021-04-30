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
%%% This behaviour must be implemented by proper models, that is modules with
%%% records of the same name.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_container).
-author("Bartosz Walkowicz").

%% API
-export([
    init/3,
    get_data_spec/1,
    get_container_stream/1,
    encode/1,
    decode/1
]).


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


-callback init(atm_data_spec:spec(), init_args()) -> container().

-callback get_data_spec(container()) -> atm_data_spec:spec().

-callback get_container_stream(container()) -> atm_container_stream:stream().

-callback to_json(container()) -> json_utils:json_map().

-callback from_json(json_utils:json_map()) -> container().


%%%===================================================================
%%% API
%%%===================================================================


-spec init(model(), atm_data_spec:spec(), init_args()) -> container().
init(Model, AtmDataSpec, InitArgs) ->
    Model:init(AtmDataSpec, InitArgs).


-spec get_data_spec(container()) -> atm_data_spec:spec().
get_data_spec(AtmContainer) ->
    Model = utils:record_type(AtmContainer),
    Model:get_data_spec(AtmContainer).


-spec get_container_stream(container()) -> atm_container_stream:stream().
get_container_stream(AtmContainer) ->
    Model = utils:record_type(AtmContainer),
    Model:get_container_stream(AtmContainer).


-spec encode(container()) -> binary().
encode(AtmContainer) ->
    Model = utils:record_type(AtmContainer),
    AtmContainerJson = Model:to_json(AtmContainer),
    json_utils:encode(AtmContainerJson#{<<"_type">> => atom_to_binary(Model, utf8)}).


-spec decode(binary()) -> container().
decode(AtmContainerBin) ->
    AtmContainerJson = json_utils:decode(AtmContainerBin),
    {ModelBin, AtmContainerJson2} = maps:take(<<"_type">>, AtmContainerJson),
    Model = binary_to_atom(ModelBin, utf8),
    Model:from_json(AtmContainerJson2).
