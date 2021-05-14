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
-export([create/2, get_iterator/1]).

-type initial_value() :: atm_container:initial_value().

-export_type([initial_value/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(atm_store_schema(), initial_value()) ->
    {ok, atm_store:id()} | {error, term()}.
create(#atm_store_schema{
    id = AtmStoreSchemaId,
    name = AtmStoreName,
    description = AtmStoreDescription,
    requires_initial_value = RequiresInitialValues,  % TODO check
    type = StoreType,
    data_spec = AtmDataSpec
}, InitialValue) ->
    ContainerModel = store_type_to_container_type(StoreType),

    {ok, _} = atm_store:create(#atm_store{
        schema_id = AtmStoreSchemaId,
        name = AtmStoreName,
        description = AtmStoreDescription,
        requires_initial_value = RequiresInitialValues,
        frozen = false,
        type = StoreType,
        container = atm_container:create(ContainerModel, AtmDataSpec, InitialValue)
    }).


-spec get_iterator(atm_store_iterator_config:record()) ->
    atm_store_iterator:record().
get_iterator(#atm_store_iterator_config{store_id = AtmStoreId} = AtmStoreIteratorConfig) ->
    {ok, #atm_store{container = AtmContainer}} = atm_store:get(AtmStoreId),
    atm_store_iterator:create(AtmStoreIteratorConfig, AtmContainer).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec store_type_to_container_type(automation:store_type()) ->
    atm_container:type().
store_type_to_container_type(single_value) -> atm_single_value_container;
store_type_to_container_type(range) -> atm_range_container.
