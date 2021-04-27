%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for performing operations on automation stores.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_api).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_tmp.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    create/2,
    init_stream/2
]).

-type error() :: {error, term()}.


%%%===================================================================
%%% API
%%%===================================================================


-spec create(atm_store_schema(), term()) -> {ok, atm_store:id()} | error().
create(#atm_store_schema{
    name = Name,
    summary = Summary,
    description = Description,
    is_input_store = IsInputStore,
    store_type = StoreType,
    data_spec = AtmDataSpec
}, InitialArgs) ->
    ContainerModel = store_type_to_container_model(StoreType),

    atm_store:create(#atm_store{
        name = Name,
        summary = Summary,
        description = Description,
        frozen = false,
        is_input_store = IsInputStore,
        type = StoreType,
        container = atm_data_container:init(ContainerModel, AtmDataSpec, InitialArgs)
    }).


-spec init_stream(atm_store_stream_schema(), atm_store:record()) ->
    atm_store_stream:stream().
init_stream(AtmStoreStreamSchema, AtmStore) ->
    atm_store_stream:init(AtmStoreStreamSchema, AtmStore).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec store_type_to_container_model(atm_store:type()) ->
    atm_data_container:model().
store_type_to_container_model(range) -> atm_range_data_container.
