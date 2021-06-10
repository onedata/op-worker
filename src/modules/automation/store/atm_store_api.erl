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
    freeze/1, unfreeze/1,
    apply_operation/5,
    delete_all/1, delete/1,
    acquire_iterator/2
]).

-type initial_value() :: atm_container:initial_value().

-export_type([initial_value/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_all(atm_workflow_execution:creation_ctx()) -> [atm_store:doc()] | no_return().
create_all(#atm_workflow_execution_creation_ctx{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
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
            {ok, AtmStoreDoc} = create(AtmWorkflowExecutionCtx, InitialValue, AtmStoreSchema),
            [AtmStoreDoc | Acc]
        catch _:Reason ->
            catch delete_all([Doc#document.key || Doc <- Acc]),
            throw(?ERROR_ATM_STORE_CREATION_FAILED(AtmStoreSchemaId, Reason))
        end
    end, [], AtmStoreSchemas)).


-spec create(
    atm_workflow_execution_ctx:record(),
    undefined | initial_value(),
    atm_store_schema:record()
) ->
    {ok, atm_store:doc()} | no_return().
create(_AtmWorkflowExecutionCtx, undefined, #atm_store_schema{
    requires_initial_value = true,
    default_initial_value = undefined
}) ->
    throw(?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_VALUE);

create(AtmWorkflowExecutionCtx, InitialValue, #atm_store_schema{
    id = AtmStoreSchemaId,
    default_initial_value = DefaultInitialValue,
    type = StoreType,
    data_spec = AtmDataSpec
}) ->
    ContainerModel = store_type_to_container_type(StoreType),
    ActualInitialValue = utils:ensure_defined(InitialValue, DefaultInitialValue),

    {ok, _} = atm_store:create(#atm_store{
        workflow_execution_id = atm_workflow_execution_ctx:get_workflow_execution_id(
            AtmWorkflowExecutionCtx
        ),
        schema_id = AtmStoreSchemaId,
        initial_value = ActualInitialValue,
        frozen = false,
        type = StoreType,
        container = atm_container:create(
            ContainerModel, AtmDataSpec, ActualInitialValue, AtmWorkflowExecutionCtx
        )
    }).


-spec freeze(atm_store:id()) -> ok.
freeze(AtmStoreId) ->
    ok = atm_store:update(AtmStoreId, fun(#atm_store{} = AtmStore) ->
        {ok, AtmStore#atm_store{frozen = true}}
    end).


-spec unfreeze(atm_store:id()) -> ok.
unfreeze(AtmStoreId) ->
    ok = atm_store:update(AtmStoreId, fun(#atm_store{} = AtmStore) ->
        {ok, AtmStore#atm_store{frozen = false}}
    end).


-spec apply_operation(
    atm_workflow_execution_ctx:record(),
    atm_container:operation_type(),
    atm_api:item(),
    atm_container:operation_options(),
    atm_store:id()
) ->
    ok | no_return().
apply_operation(AtmWorkflowExecutionCtx, Operation, Item, Options, AtmStoreId) ->
    % NOTE: no need to use critical section here as containers either:
    %   * are based on structure that support transaction operation on their own 
    %   * store only one value and it will be overwritten 
    %   * do not support any operation
    case atm_store:get(AtmStoreId) of
        {ok, #atm_store{container = AtmContainer, frozen = false}} ->
            % TODO VFS-7691 maybe perform data validation here instead of specific container ??
            UpdatedContainer = atm_container:apply_operation(AtmContainer, #atm_container_operation{
                type = Operation,
                options = Options,
                value = Item,
                workflow_execution_ctx = AtmWorkflowExecutionCtx
            }),
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
acquire_iterator(AtmWorkflowExecutionEnv, #atm_store_iterator_spec{
    store_schema_id = AtmStoreSchemaId
} = AtmStoreIteratorConfig) ->
    AtmStoreId = atm_workflow_execution_env:get_store_id(AtmStoreSchemaId, AtmWorkflowExecutionEnv),
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
