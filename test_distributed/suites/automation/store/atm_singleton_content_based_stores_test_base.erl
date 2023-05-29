%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides base for testing automation stores capable of
%%% storing only single item.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_singleton_content_based_stores_test_base).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


%% API
-export([
    modules_to_load/0,
    init_per_group/1,
    end_per_group/1
]).
-export([
    create_test_base/2,
    update_content_test_base/4,
    browse_content_test_base/5
]).


-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).
-define(erpc(Expr), ?erpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% API functions
%%%===================================================================


modules_to_load() ->
    [?MODULE, atm_store_test_utils].


init_per_group(Config) ->
    time_test_utils:freeze_time(Config),
    Config.


end_per_group(Config) ->
    time_test_utils:unfreeze_time(Config).


-spec create_test_base(
    [atm_store_config:record()],
    fun((atm_store_config:record()) -> atm_data_spec:record())
) ->
    ok.
create_test_base(AtmStoreConfigs, GetItemDataSpec) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    ?assertEqual(
        ?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_CONTENT,
        ?rpc(catch atm_store_api:create(
            AtmWorkflowExecutionAuth,
            ?LOGGER_DEBUG_LEVEL,
            undefined,
            atm_store_test_utils:build_store_schema(?RAND_ELEMENT(AtmStoreConfigs), true)
        ))
    ),
    ?assertMatch(
        {ok, #document{value = #atm_store{initial_content = undefined, frozen = false}}},
        ?rpc(atm_store_api:create(
            AtmWorkflowExecutionAuth,
            ?LOGGER_DEBUG_LEVEL,
            undefined,
            atm_store_test_utils:build_store_schema(?RAND_ELEMENT(AtmStoreConfigs), false)
        ))
    ),

    lists:foreach(fun(AtmStoreConfig) ->
        ItemDataSpec = GetItemDataSpec(AtmStoreConfig),
        DefaultItem = gen_valid_data(AtmWorkflowExecutionAuth, ItemDataSpec),
        CreateStoreFun = atm_store_test_utils:build_create_store_with_initial_content_fun(
            AtmWorkflowExecutionAuth, AtmStoreConfig, DefaultItem
        ),

        InvalidItem = gen_invalid_data(AtmWorkflowExecutionAuth, ItemDataSpec),
        ?assertEqual(
            atm_store_test_utils:infer_exp_invalid_data_error(InvalidItem, ItemDataSpec),
            ?rpc(catch CreateStoreFun(InvalidItem))
        ),

        ValidItem = gen_valid_data(AtmWorkflowExecutionAuth, ItemDataSpec),
        ?assertMatch(
            {ok, #document{value = #atm_store{initial_content = ValidItem, frozen = false}}},
            ?rpc(CreateStoreFun(ValidItem))
        )
    end, AtmStoreConfigs).


-spec update_content_test_base(
    [atm_store_config:record()],
    fun((atm_store_config:record()) -> atm_data_spec:record()),
    atm_store_content_update_options:record(),
    fun((atm_workflow_execution_auth:record(), atm_store:id()) -> undefined | atm_value:expanded())
) ->
    ok.
update_content_test_base(AtmStoreConfigs, GetItemDataSpec, ContentUpdateOpts, GetContentFun) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    lists:foreach(fun(AtmStoreConfig) ->
        ItemDataSpec = GetItemDataSpec(AtmStoreConfig),
        InitialItem = case rand:uniform(2) of
            1 -> undefined;
            2 -> gen_valid_data(AtmWorkflowExecutionAuth, ItemDataSpec)
        end,
        FullyExpandedInitialItem = case InitialItem of
            undefined -> undefined;
            _ -> compress_and_expand_data(AtmWorkflowExecutionAuth, InitialItem, ItemDataSpec)
        end,
        NewItem = gen_valid_data(AtmWorkflowExecutionAuth, ItemDataSpec),
        FullyExpandedNewItem = compress_and_expand_data(AtmWorkflowExecutionAuth, NewItem, ItemDataSpec),

        AtmStoreSchema = atm_store_test_utils:build_store_schema(AtmStoreConfig),
        {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
            AtmWorkflowExecutionAuth, ?LOGGER_DEBUG_LEVEL, InitialItem, AtmStoreSchema
        ))),

        % Assert set with invalid item should fail
        InvalidItem = gen_invalid_data(AtmWorkflowExecutionAuth, ItemDataSpec),
        ?assertEqual(
            atm_store_test_utils:infer_exp_invalid_data_error(InvalidItem, ItemDataSpec),
            ?rpc(catch atm_store_api:update_content(
                AtmWorkflowExecutionAuth, InvalidItem, ContentUpdateOpts, AtmStoreId
            ))
        ),
        ?assertMatch(FullyExpandedInitialItem, GetContentFun(AtmWorkflowExecutionAuth, AtmStoreId)),

        % Assert it is not possible to perform operation on store when it is frozen
        ?rpc(atm_store_api:freeze(AtmStoreId)),
        ?assertEqual(
            ?ERROR_ATM_STORE_FROZEN(AtmStoreSchema#atm_store_schema.id),
            ?rpc(catch atm_store_api:update_content(
                AtmWorkflowExecutionAuth, NewItem, ContentUpdateOpts, AtmStoreId
            ))
        ),
        ?assertMatch(FullyExpandedInitialItem, GetContentFun(AtmWorkflowExecutionAuth, AtmStoreId)),

        % Otherwise operation should succeed
        ?rpc(atm_store_api:unfreeze(AtmStoreId)),
        ?assertEqual(ok, ?rpc(atm_store_api:update_content(
            AtmWorkflowExecutionAuth, NewItem, ContentUpdateOpts, AtmStoreId
        ))),
        ?assertMatch(FullyExpandedNewItem, GetContentFun(AtmWorkflowExecutionAuth, AtmStoreId))

    end, AtmStoreConfigs).


-spec browse_content_test_base(
    [atm_store_config:record()],
    fun((atm_store_config:record()) -> atm_data_spec:record()),
    atm_store_content_browse_options:record(),
    fun((atm_workflow_execution_auth:record(), atm_value:expanded(), atm_store:id()) -> ok),
    fun((atm_value:expanded()) -> atm_store_content_browse_result:record())
) ->
    ok.
browse_content_test_base(AtmStoreConfigs, GetItemDataSpec, ContentBrowseOpts, SetContentFun, BuildBrowseResult) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    lists:foreach(fun(AtmStoreConfig) ->
        ItemDataSpec = GetItemDataSpec(AtmStoreConfig),
        AtmStoreSchema = atm_store_test_utils:build_store_schema(AtmStoreConfig),
        {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
            AtmWorkflowExecutionAuth, ?LOGGER_DEBUG_LEVEL, undefined, AtmStoreSchema
        ))),

        ExpError = ?ERROR_ATM_STORE_CONTENT_NOT_SET(AtmStoreSchema#atm_store_schema.id),
        ?assertThrow(ExpError, ?erpc(atm_store_api:browse_content(
            AtmWorkflowExecutionAuth, ContentBrowseOpts, AtmStoreId
        ))),

        Item = gen_valid_data(AtmWorkflowExecutionAuth, ItemDataSpec),
        SetContentFun(AtmWorkflowExecutionAuth, Item, AtmStoreId),
        ExpandedItem = compress_and_expand_data(AtmWorkflowExecutionAuth, Item, ItemDataSpec),

        ?assertEqual(
            BuildBrowseResult(ExpandedItem),
            ?rpc(atm_store_api:browse_content(AtmWorkflowExecutionAuth, ContentBrowseOpts, AtmStoreId))
        )

    end, AtmStoreConfigs).


%===================================================================
% Helper functions
%===================================================================


%% @private
-spec create_workflow_execution_auth() -> atm_workflow_execution_auth:record().
create_workflow_execution_auth() ->
    atm_store_test_utils:create_workflow_execution_auth(
        ?PROVIDER_SELECTOR, user1, space_krk
    ).


%% @private
-spec gen_valid_data(atm_workflow_execution_auth:record(), atm_data_spec:record()) ->
    atm_value:expanded().
gen_valid_data(AtmWorkflowExecutionAuth, ItemDataSpec) ->
    atm_store_test_utils:gen_valid_data(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, ItemDataSpec
    ).


%% @private
-spec gen_invalid_data(atm_workflow_execution_auth:record(), atm_data_spec:record()) ->
    atm_value:expanded().
gen_invalid_data(AtmWorkflowExecutionAuth, ItemDataSpec) ->
    atm_store_test_utils:gen_invalid_data(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, ItemDataSpec
    ).


%% @private
-spec compress_and_expand_data(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_store:id()
) ->
    atm_value:expanded().
compress_and_expand_data(AtmWorkflowExecutionAuth, Item, ItemDataSpec) ->
    atm_store_test_utils:compress_and_expand_data(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, Item, ItemDataSpec
    ).
