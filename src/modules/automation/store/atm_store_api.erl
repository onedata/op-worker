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
    create/4,
    copy/2,
    get/1, acquire_iterator/2,
    freeze/1, unfreeze/1,
    browse_content/3,
    update_content/4,
    delete/1
]).

-compile({no_auto_import, [get/1]}).

-type initial_content() :: atm_store_container:initial_content().
-type schema() :: atm_store_schema:record() | #atm_system_store_schema{}.

-export_type([initial_content/0, schema/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(
    atm_workflow_execution_auth:record(),
    audit_log:entry_severity_int(),
    undefined | initial_content(),
    schema()
) ->
    {ok, atm_store:doc()} | no_return().
create(_AtmWorkflowExecutionAuth, _LogLevel, undefined, #atm_store_schema{
    requires_initial_content = true,
    default_initial_content = undefined
}) ->
    throw(?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_CONTENT);

create(AtmWorkflowExecutionAuth, LogLevel, InitialContent, AtmStoreSchema) ->
    StoreType = get_store_type(AtmStoreSchema),
    ActualInitialContent = get_initial_content(InitialContent, AtmStoreSchema),

    {ok, _} = atm_store:create(#atm_store{
        workflow_execution_id = atm_workflow_execution_auth:get_workflow_execution_id(
            AtmWorkflowExecutionAuth
        ),
        schema_id = get_schema_id(AtmStoreSchema),
        initial_content = ActualInitialContent,
        frozen = false,
        container = atm_store_container:create(StoreType, #atm_store_container_creation_args{
            workflow_execution_auth = AtmWorkflowExecutionAuth,
            log_level = LogLevel,
            store_config = get_store_config(AtmStoreSchema),
            initial_content = ActualInitialContent
        })
    }).


-spec copy(atm_store:id(), undefined | boolean()) -> atm_store:doc() | no_return().
copy(AtmStoreId, Frozen) ->
    {ok, AtmStore = #atm_store{frozen = OriginFrozen, container = AtmStoreContainer}} = get(
        AtmStoreId
    ),

    {ok, Doc} = atm_store:create(AtmStore#atm_store{
        frozen = utils:ensure_defined(Frozen, OriginFrozen),
        container = atm_store_container:copy(AtmStoreContainer)
    }),
    Doc.


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
%% Returns 'atm_store_iterator' allowing to iterate over all items produced by
%% store. Those items are not only items directly kept in store but also objects
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


%%-------------------------------------------------------------------
%% @doc
%% Returns batch of exact items directly kept at store in opposition to
%% iteration which can return items inferred from store content.
%% @end
%%-------------------------------------------------------------------
-spec browse_content(
    atm_workflow_execution_auth:record(),
    atm_store_content_browse_options:record(),
    atm_store:id() | atm_store:record()
) ->
    atm_store_content_browse_result:record() | no_return().
browse_content(AtmWorkflowExecutionAuth, BrowseOpts, #atm_store{
    schema_id = AtmStoreSchemaId,
    container = AtmStoreContainer
}) ->
    atm_store_container:browse_content(AtmStoreContainer, #atm_store_content_browse_req{
        store_schema_id = AtmStoreSchemaId,
        workflow_execution_auth = AtmWorkflowExecutionAuth,
        options = BrowseOpts
    });

browse_content(AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId) ->
    case get(AtmStoreId) of
        {ok, AtmStore} ->
            browse_content(AtmWorkflowExecutionAuth, BrowseOpts, AtmStore);
        ?ERROR_NOT_FOUND ->
            throw(?ERROR_NOT_FOUND)
    end.


-spec update_content(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_store:content_update_options(),
    atm_store:id()
) ->
    ok | no_return().
update_content(AtmWorkflowExecutionAuth, Item, Options, AtmStoreId) ->
    % NOTE: no need to use critical section here as containers either:
    %   * are based on structure that support transaction operation on their own 
    %   * store only one item and it will be overwritten
    %   * do not support any operation
    case get(AtmStoreId) of
        {ok, #atm_store{container = AtmStoreContainer, frozen = false}} ->
            UpdatedAtmStoreContainerResult = atm_store_container:update_content(
                AtmStoreContainer, #atm_store_content_update_req{
                    workflow_execution_auth = AtmWorkflowExecutionAuth,
                    argument = Item,
                    options = Options
                }
            ),
            case UpdatedAtmStoreContainerResult of
                ok ->
                    ok;
                {ok, UpdatedAtmStoreContainer} ->
                    atm_store:update(AtmStoreId, fun(#atm_store{} = PrevStore) ->
                        {ok, PrevStore#atm_store{container = UpdatedAtmStoreContainer}}
                    end)
            end;
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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_schema_id(schema()) -> automation:name().
get_schema_id(#atm_store_schema{id = AtmStoreSchemaId}) -> AtmStoreSchemaId;
get_schema_id(#atm_system_store_schema{id = AtmStoreSchemaId}) -> AtmStoreSchemaId.


%% @private
-spec get_store_type(schema()) -> atm_store:type().
get_store_type(#atm_store_schema{type = StoreType}) -> StoreType;
get_store_type(#atm_system_store_schema{type = StoreType}) -> StoreType.


%% @private
-spec get_store_config(schema()) -> atm_store:config().
get_store_config(#atm_store_schema{config = AtmStoreConfig}) -> AtmStoreConfig;
get_store_config(#atm_system_store_schema{config = AtmStoreConfig}) -> AtmStoreConfig.


%% @private
-spec get_initial_content(undefined | initial_content(), schema()) ->
    undefined | initial_content().
get_initial_content(undefined, #atm_store_schema{default_initial_content = DefaultInitialContent}) ->
    DefaultInitialContent;
get_initial_content(InitialContent, _) ->
    InitialContent.
