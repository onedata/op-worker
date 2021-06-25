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
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    create_all/1, create/3,
    view_content/3, acquire_iterator/2,
    freeze/1, unfreeze/1,
    apply_operation/5,
    delete_all/1, delete/1
]).

-type initial_value() :: atm_store_container:initial_value().

% index() TODO WRITEME .
-type index() :: binary().
-type offset() :: integer().
-type limit() :: pos_integer().

-type view_opts() :: #{
    limit := limit(),
    start_index => index(),
    offset => offset()
}.

-export_type([initial_value/0, index/0, offset/0, limit/0, view_opts/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_all(atm_workflow_execution:creation_ctx()) -> [atm_store:doc()] | no_return().
create_all(#atm_workflow_execution_creation_ctx{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
    workflow_schema_doc = #document{value = #od_atm_workflow_schema{
        stores = AtmStoreSchemas
    }},
    store_initial_values = StoreInitialValues
}) ->
    lists:reverse(lists:foldl(fun(#atm_store_schema{id = AtmStoreSchemaId} = AtmStoreSchema, Acc) ->
        StoreInitialValue = utils:null_to_undefined(maps:get(
            AtmStoreSchemaId, StoreInitialValues, undefined
        )),
        try
            {ok, AtmStoreDoc} = create(AtmWorkflowExecutionCtx, StoreInitialValue, AtmStoreSchema),
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
    ActualInitialValue = utils:ensure_defined(InitialValue, DefaultInitialValue),

    {ok, _} = atm_store:create(#atm_store{
        workflow_execution_id = atm_workflow_execution_ctx:get_workflow_execution_id(
            AtmWorkflowExecutionCtx
        ),
        schema_id = AtmStoreSchemaId,
        initial_value = ActualInitialValue,
        frozen = false,
        container = atm_store_container:create(
            StoreType, AtmWorkflowExecutionCtx, AtmDataSpec, ActualInitialValue
        )
    }).


-spec view_content(
    atm_workflow_execution_ctx:record(),
    view_opts(),
    atm_store:id() | atm_store:record()
) ->
    {ok, [{index(), automation:item()}], IsLast :: boolean()} | no_return().
view_content(AtmWorkflowExecutionCtx, ViewOpts, #atm_store{container = AtmStoreContainer}) ->
    SanitizedViewOpts = middleware_sanitizer:sanitize_data(ViewOpts, #{
        required => #{
            limit => {integer, {not_lower_than, 1}}
        },
        at_least_one => #{
            offset => {integer, any},
            start_index => {binary, any}
        }
    }),
    atm_store_container:view_content(AtmWorkflowExecutionCtx, SanitizedViewOpts, AtmStoreContainer);

view_content(AtmWorkflowExecutionCtx, ViewOpts, AtmStoreId) ->
    case atm_store:get(AtmStoreId) of
        {ok, AtmStore} ->
            view_content(AtmWorkflowExecutionCtx, ViewOpts, AtmStore);
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND
    end.


-spec acquire_iterator(atm_workflow_execution_env:record(), atm_store_iterator_spec:record()) ->
    atm_store_iterator:record().
acquire_iterator(AtmWorkflowExecutionEnv, #atm_store_iterator_spec{
    store_schema_id = AtmStoreSchemaId
} = AtmStoreIteratorConfig) ->
    AtmStoreId = atm_workflow_execution_env:get_store_id(AtmStoreSchemaId, AtmWorkflowExecutionEnv),
    {ok, #atm_store{container = AtmStoreContainer}} = atm_store:get(AtmStoreId),
    atm_store_iterator:build(AtmStoreIteratorConfig, AtmStoreContainer).


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
    atm_store_container:operation_type(),
    automation:item(),
    atm_store_container:operation_options(),
    atm_store:id()
) ->
    ok | no_return().
apply_operation(AtmWorkflowExecutionCtx, Operation, Item, Options, AtmStoreId) ->
    % NOTE: no need to use critical section here as containers either:
    %   * are based on structure that support transaction operation on their own 
    %   * store only one value and it will be overwritten 
    %   * do not support any operation
    case atm_store:get(AtmStoreId) of
        {ok, #atm_store{container = AtmStoreContainer, frozen = false}} ->
            AtmStoreContainerOperation = #atm_store_container_operation{
                type = Operation,
                options = Options,
                value = Item,
                workflow_execution_ctx = AtmWorkflowExecutionCtx
            },
            UpdatedAtmStoreContainer = atm_store_container:apply_operation(
                AtmStoreContainer, AtmStoreContainerOperation
            ),
            atm_store:update(AtmStoreId, fun(#atm_store{} = PrevStore) ->
                {ok, PrevStore#atm_store{container = UpdatedAtmStoreContainer}}
            end);
        {ok, #atm_store{schema_id = AtmStoreSchemaId, frozen = true}} ->
            throw(?ERROR_ATM_STORE_FROZEN(AtmStoreSchemaId));
        {error, _} = Error ->
            throw(Error)
    end.


-spec delete_all([atm_store:id()]) -> ok.
delete_all(AtmStoreIds) ->
    lists:foreach(fun delete/1, AtmStoreIds).


-spec delete(atm_store:id()) -> ok | {error, term()}.
delete(AtmStoreId) ->
    case atm_store:get(AtmStoreId) of
        {ok, #atm_store{container = AtmStoreContainer}} ->
            atm_store_container:delete(AtmStoreContainer),
            atm_store:delete(AtmStoreId);
        ?ERROR_NOT_FOUND ->
            ok
    end.
