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

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    create_all/1, create/3,
    update/4,
    delete_all/1, delete/1,
    acquire_iterator/2
]).

-type initial_value() :: atm_container:initial_value().

-export_type([initial_value/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_all(atm_api:creation_ctx()) -> [atm_store:doc()] | no_return().
create_all(#atm_execution_creation_ctx{
    workflow_execution_id = AtmWorkflowExecutionId,
    workflow_schema_doc = #document{value = #od_atm_workflow_schema{
        stores = AtmStoreSchemas
    }},
    initial_values = InitialValues
}) ->
    lists:reverse(lists:foldl(fun(#atm_store_schema{id = AtmStoreSchemaId} = AtmStoreSchema, Acc) ->
        InitialValue = utils:null_to_undefined(maps:get(
            AtmStoreSchemaId, InitialValues, undefined
        )),
        try
            {ok, AtmStoreDoc} = create(AtmWorkflowExecutionId, InitialValue, AtmStoreSchema),
            [AtmStoreDoc | Acc]
        catch _:Reason ->
            catch delete_all([Doc#document.key || Doc <- Acc]),
            throw(?ERROR_ATM_STORE_CREATION_FAILED(AtmStoreSchemaId, Reason))
        end
    end, [], AtmStoreSchemas)).


-spec create(
    atm_workflow_execution:id(),
    undefined | initial_value(),
    atm_store_schema:record()
) ->
    {ok, atm_store:doc()} | no_return().
create(_AtmWorkflowExecutionId, undefined, #atm_store_schema{
    requires_initial_value = true,
    default_initial_value = undefined
}) ->
    throw(?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_VALUE);

create(AtmWorkflowExecutionId, InitialValue, #atm_store_schema{
    id = AtmStoreSchemaId,
    default_initial_value = DefaultInitialValue,
    type = StoreType,
    data_spec = AtmDataSpec
}) ->
    ContainerModel = store_type_to_container_type(StoreType),
    ActualInitialValue = utils:ensure_defined(InitialValue, DefaultInitialValue),

    {ok, _} = atm_store:create(#atm_store{
        workflow_execution_id = AtmWorkflowExecutionId,
        schema_id = AtmStoreSchemaId,
        initial_value = ActualInitialValue,
        frozen = false,
        type = StoreType,
        container = atm_container:create(ContainerModel, AtmDataSpec, ActualInitialValue)
    }).


-spec update(atm_store:id(), atm_container:update_operation(),
    atm_container:update_options(), json_utils:json_term()) -> ok | no_return().
update(AtmStoreId, Operation, Options, Item) ->
    case atm_store:get(AtmStoreId) of
        {ok, #atm_store{container = AtmContainer, frozen = false}} -> 
            UpdatedContainer = atm_container:update(AtmContainer, Operation, Options, Item),
            atm_store:update(AtmStoreId, fun(#atm_store{} = PrevStore) ->
                {ok, PrevStore#atm_store{container = UpdatedContainer}}
            end);
        {ok, #atm_store{container = AtmContainer, frozen = true}} -> 
            throw(?ERROR_ATM_STORE_FROZEN(AtmContainer));
        {error, _} = Error ->
            throw(Error)
    end.


-spec delete_all([atm_store:id()]) -> ok.
delete_all(AtmStoreIds) ->
    lists:foreach(fun delete/1, AtmStoreIds).


-spec delete(atm_store:id()) -> ok | {error, term()}.
delete(AtmStoreId) ->
    {ok, #atm_store{container = AtmContainer}} = atm_store:get(AtmStoreId),
    ok = atm_container:delete(AtmContainer),
    atm_store:delete(AtmStoreId).


-spec acquire_iterator(atm_workflow_execution_env:record(), atm_store_iterator_spec:record()) ->
    atm_store_iterator:record().
acquire_iterator(AtmExecutionState, #atm_store_iterator_spec{
    store_schema_id = AtmStoreSchemaId
} = AtmStoreIteratorConfig) ->
    AtmStoreId = atm_workflow_execution_env:get_store_id(AtmStoreSchemaId, AtmExecutionState),
    {ok, #atm_store{container = AtmContainer}} = atm_store:get(AtmStoreId),
    atm_store_iterator:build(AtmStoreIteratorConfig, AtmContainer).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec store_type_to_container_type(automation:store_type()) ->
    atm_container:type().
store_type_to_container_type(single_value) -> atm_single_value_container;
store_type_to_container_type(range) -> atm_range_container;
store_type_to_container_type(list) -> atm_list_container.
