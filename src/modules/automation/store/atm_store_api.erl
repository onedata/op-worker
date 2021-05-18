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
-include("modules/automation/atm_wokflow_execution.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    create_all/2, create/2,
    build_iterator_config/2, get_iterator/1,
    delete_all/1, delete/1
]).

-type initial_value() :: atm_container:initial_value().
-type initial_values() :: #{AtmStoreSchemaId :: automation:id() => initial_value()}.

-type registry() :: #{AtmStoreSchemaId :: automation:id() => atm_store:id()}.

-export_type([initial_value/0, initial_values/0, registry/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_all([atm_store_schema:record()], initial_values()) ->
    registry() | no_return().
create_all(AtmStoreSchemas, InitialValues) ->
    lists:foldl(fun(#atm_store_schema{
        id = AtmStoreSchemaId
    } = AtmStoreSchema, Acc) ->
        InitialValue = maps:get(AtmStoreSchemaId, InitialValues, undefined),
        try
            {ok, AtmStoreId} = create(AtmStoreSchema, InitialValue),
            Acc#{AtmStoreSchemaId => AtmStoreId}
        catch _:Reason ->
            delete_all(maps:values(Acc)),
            throw(?ERROR_ATM_STORE_CREATION_FAILED(AtmStoreSchemaId, Reason))
        end
    end, #{}, AtmStoreSchemas).


-spec create(atm_store_schema:record(), undefined | initial_value()) ->
    {ok, atm_store:id()} | no_return().
create(#atm_store_schema{requires_initial_value = true}, undefined) ->
    throw(?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_VALUE);

create(#atm_store_schema{
    id = AtmStoreSchemaId,
    name = AtmStoreName,
    description = AtmStoreDescription,
    requires_initial_value = RequiresInitialValues,
    default_initial_value = DefaultInitialValue,
    type = StoreType,
    data_spec = AtmDataSpec
}, InitialValue) ->
    ContainerModel = store_type_to_container_type(StoreType),
    ActualInitialValue = utils:ensure_defined(InitialValue, DefaultInitialValue),

    {ok, _} = atm_store:create(#atm_store{
        schema_id = AtmStoreSchemaId,
        name = AtmStoreName,
        description = AtmStoreDescription,
        requires_initial_value = RequiresInitialValues,
        initial_value = ActualInitialValue,
        frozen = false,
        type = StoreType,
        container = atm_container:create(ContainerModel, AtmDataSpec, ActualInitialValue)
    }).


-spec build_iterator_config(registry(), atm_store_iterator_spec:record()) ->
    atm_store_iterator_config:record() | no_return().
build_iterator_config(AtmStoreRegistry, AtmStoreIteratorConfig) ->
    atm_store_iterator_config:build(AtmStoreRegistry, AtmStoreIteratorConfig).


-spec get_iterator(atm_store_iterator_config:record()) ->
    atm_store_iterator:record().
get_iterator(#atm_store_iterator_config{store_id = AtmStoreId} = AtmStoreIteratorConfig) ->
    {ok, #atm_store{container = AtmContainer}} = atm_store:get(AtmStoreId),
    atm_store_iterator:create(AtmStoreIteratorConfig, AtmContainer).


-spec delete_all([atm_store:id()]) -> ok.
delete_all(AtmStoreIds) ->
    lists:foreach(fun delete/1, AtmStoreIds).


-spec delete(atm_store:id()) -> ok | {error, term()}.
delete(AtmStoreId) ->
    atm_store:delete(AtmStoreId).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec store_type_to_container_type(automation:store_type()) ->
    atm_container:type().
store_type_to_container_type(single_value) -> atm_single_value_container;
store_type_to_container_type(range) -> atm_range_container.
