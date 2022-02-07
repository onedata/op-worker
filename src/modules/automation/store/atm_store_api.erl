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
    create/3,
    get/1, browse_content/3, acquire_iterator/2,
    freeze/1, unfreeze/1,
    apply_operation/5,
    delete/1
]).

-compile({no_auto_import, [get/1]}).

-type initial_value() :: atm_store_container:initial_value().

% Index of automation:item() stored in atm_store_container that uniquely identifies it.
-type index() :: binary().
-type offset() :: integer().
-type limit() :: pos_integer().

-type browse_options() :: atm_store_container:browse_options().
-type browse_result() :: {[{index(), {ok, automation:item()} | errors:error()}], IsLast :: boolean()}.

-export_type([initial_value/0]).
-export_type([index/0, offset/0, limit/0, browse_options/0, browse_result/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(
    atm_workflow_execution_auth:record(),
    undefined | initial_value(),
    atm_store_schema:record()
) ->
    {ok, atm_store:doc()} | no_return().
create(_AtmWorkflowExecutionAuth, undefined, #atm_store_schema{
    requires_initial_content = true,
    default_initial_content = undefined
}) ->
    throw(?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_CONTENT);

create(AtmWorkflowExecutionAuth, InitialContent, #atm_store_schema{
    id = AtmStoreSchemaId,
    default_initial_content = DefaultInitialContent,
    type = StoreType,
    config = AtmStoreConfig
}) ->
    ActualInitialContent = utils:ensure_defined(InitialContent, DefaultInitialContent),

    {ok, _} = atm_store:create(#atm_store{
        workflow_execution_id = atm_workflow_execution_auth:get_workflow_execution_id(
            AtmWorkflowExecutionAuth
        ),
        schema_id = AtmStoreSchemaId,
        initial_content = ActualInitialContent,
        frozen = false,
        container = atm_store_container:create(
            StoreType, AtmWorkflowExecutionAuth, AtmStoreConfig, ActualInitialContent
        )
    }).


-spec get(atm_store:id()) -> {ok, atm_store:record()} | ?ERROR_NOT_FOUND.
get(AtmStoreId) ->
    case atm_store:get(AtmStoreId) of
        {ok, #document{value = AtmStore}} ->
            {ok, AtmStore};
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND
    end.


%%-------------------------------------------------------------------
%% @doc
%% Returns batch of items (and their indices) directly kept at store
%% in accordance to specified browse_opts().
%% @end
%%-------------------------------------------------------------------
-spec browse_content(
    atm_workflow_execution_auth:record(),
    browse_options(),
    atm_store:id() | atm_store:record()
) ->
    browse_result() | no_return().
browse_content(AtmWorkflowExecutionAuth, BrowseOpts, #atm_store{container = AtmStoreContainer}) ->
    atm_store_container:browse_content(AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreContainer);

browse_content(AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId) ->
    case get(AtmStoreId) of
        {ok, AtmStore} ->
            browse_content(AtmWorkflowExecutionAuth, BrowseOpts, AtmStore);
        ?ERROR_NOT_FOUND ->
            throw(?ERROR_NOT_FOUND)
    end.


%%-------------------------------------------------------------------
%% @doc
%% Returns 'atm_store_iterator' allowing to iterate over all values produced by
%% store. Those values are not only items directly kept in store but also objects
%% associated/inferred from them (e.g. in case of file tree forest store entire
%% files subtree for each file kept in store will be traversed and returned).
%% @end
%%-------------------------------------------------------------------
-spec acquire_iterator(atm_store:id(), atm_store_iterator_spec:record()) ->
    atm_store_iterator:record().
acquire_iterator(AtmStoreId, AtmStoreIteratorSpec) ->
    {ok, #atm_store{container = AtmStoreContainer}} = get(AtmStoreId),
    atm_store_iterator:build(AtmStoreIteratorSpec, AtmStoreContainer).


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
    atm_workflow_execution_auth:record(),
    atm_store_container:operation_type(),
    automation:item(),
    atm_store_container:operation_options(),
    atm_store:id()
) ->
    ok | no_return().
apply_operation(AtmWorkflowExecutionAuth, Operation, Item, Options, AtmStoreId) ->
    % NOTE: no need to use critical section here as containers either:
    %   * are based on structure that support transaction operation on their own 
    %   * store only one value and it will be overwritten 
    %   * do not support any operation
    case get(AtmStoreId) of
        {ok, #atm_store{container = AtmStoreContainer, frozen = false}} ->
            AtmStoreContainerOperation = #atm_store_container_operation{
                type = Operation,
                options = Options,
                argument = Item,
                workflow_execution_auth = AtmWorkflowExecutionAuth
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


-spec delete(atm_store:id()) -> ok | {error, term()}.
delete(AtmStoreId) ->
    case get(AtmStoreId) of
        {ok, #atm_store{container = AtmStoreContainer}} ->
            atm_store_container:delete(AtmStoreContainer),
            atm_store:delete(AtmStoreId);
        ?ERROR_NOT_FOUND ->
            ok
    end.
