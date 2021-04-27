%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines atm_data_container interface - an object which can be
%%% used to store and retrieve data of specific type.
%%%
%%%                             !!! Caution !!!
%%% 1) Any model implementing this interface must be registered in
%%%    `atm_store_api:store_type_to_container_model` function.
%%% 2) This behaviour must be implemented by proper models, that is modules with
%%%    records of the same name.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_data_container).
-author("Bartosz Walkowicz").

%% API
-export([
    init/3,
    get_data_spec/1,
    get_data_stream/1,
    encode/1,
    decode/1
]).

-type model() :: atm_range_data_container.
-type init_args() :: atm_range_data_container:init_args().
-type container() :: atm_range_data_container:container().

-export_type([model/0, init_args/0, container/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback init(atm_data_spec:spec(), init_args()) -> container().

-callback get_data_spec(container()) -> atm_data_spec:spec().

-callback get_data_stream(container()) -> atm_data_stream:stream().

-callback to_json(container()) -> json_utils:json_map().

-callback from_json(json_utils:json_map()) -> container().


%%%===================================================================
%%% API
%%%===================================================================


-spec init(model(), atm_data_spec:spec(), init_args()) -> container().
init(Model, DataSpec, InitArgs) ->
    Model:init(DataSpec, InitArgs).


-spec get_data_spec(container()) -> atm_data_spec:spec().
get_data_spec(AtmDataContainer) ->
    Model = utils:record_type(AtmDataContainer),
    Model:get_data_spec(AtmDataContainer).


-spec get_data_stream(container()) -> atm_data_stream:stream().
get_data_stream(AtmDataContainer) ->
    Model = utils:record_type(AtmDataContainer),
    Model:get_data_stream(AtmDataContainer).


-spec encode(container()) -> binary().
encode(AtmDataContainer) ->
    Model = utils:record_type(AtmDataContainer),
    AtmDataContainerJson = Model:to_json(AtmDataContainer),
    json_utils:encode(AtmDataContainerJson#{<<"_type">> => atom_to_binary(Model, utf8)}).


-spec decode(binary()) -> container().
decode(AtmDataContainerBin) ->
    AtmDataContainerJson = json_utils:decode(AtmDataContainerBin),
    {ModelBin, AtmDataContainerJson2} = maps:take(<<"_type">>, AtmDataContainerJson),
    Model = binary_to_atom(ModelBin, utf8),
    Model:from_json(AtmDataContainerJson2).
