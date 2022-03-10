%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation single value store.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_single_value_store_test_SUITE).
-author("Michal Stanisz").

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% exported for CT
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    create_test/1,
    apply_operation_test/1,
    iterator_test/1,
    browse_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_test,
        apply_operation_test,
        iterator_test,
        browse_test
    ]}
].

all() -> [
    {group, all_tests}
].


-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% Test functions
%%%===================================================================


create_test(_Config) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    ExampleAtmStoreConfigs = example_configs(),
    ExampleAtmStoreConfig = lists_utils:random_element(ExampleAtmStoreConfigs),

    ?assertEqual(
        ?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_CONTENT,
        ?rpc(catch atm_store_api:create(
            AtmWorkflowExecutionAuth,
            undefined,
            atm_store_test_utils:build_store_schema(ExampleAtmStoreConfig, true)
        ))
    ),
    ?assertMatch(
        {ok, #document{value = #atm_store{initial_content = undefined, frozen = false}}},
        ?rpc(atm_store_api:create(
            AtmWorkflowExecutionAuth,
            undefined,
            atm_store_test_utils:build_store_schema(ExampleAtmStoreConfig, false)
        ))
    ),

    lists:foreach(fun(AtmStoreConfig = #atm_single_value_store_config{item_data_spec = ItemDataSpec}) ->
        DefaultItem = gen_valid_data(AtmWorkflowExecutionAuth, ItemDataSpec),
        CreateStoreFun = atm_store_test_utils:build_create_store_with_initial_content_fun(
            AtmWorkflowExecutionAuth, AtmStoreConfig, DefaultItem
        ),

        InvalidItem = gen_invalid_data(AtmWorkflowExecutionAuth, ItemDataSpec),
        ?assertEqual(
            ?ERROR_ATM_DATA_TYPE_UNVERIFIED(InvalidItem, ItemDataSpec#atm_data_spec.type),
            ?rpc(catch CreateStoreFun(InvalidItem))
        ),

        ValidItem = gen_valid_data(AtmWorkflowExecutionAuth, ItemDataSpec),
        ?assertMatch(
            {ok, #document{value = #atm_store{initial_content = ValidItem, frozen = false}}},
            ?rpc(CreateStoreFun(ValidItem))
        )
    end, ExampleAtmStoreConfigs).


apply_operation_test(_Config) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    lists:foreach(fun(Config = #atm_single_value_store_config{item_data_spec = ItemDataSpec}) ->
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

        AtmStoreSchema = atm_store_test_utils:build_store_schema(Config),
        {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
            AtmWorkflowExecutionAuth, InitialItem, AtmStoreSchema
        ))),

        % Assert all operations but 'set' are unsupported
        lists:foreach(fun(Operation) ->
            ?assertEqual(?ERROR_NOT_SUPPORTED, ?rpc(catch atm_store_api:apply_operation(
                AtmWorkflowExecutionAuth, Operation, NewItem, #{}, AtmStoreId
            ))),
            ?assertMatch(FullyExpandedInitialItem, get_content(AtmWorkflowExecutionAuth, AtmStoreId))
        end, atm_task_schema_result_mapper:all_dispatch_functions() -- [set]),

        % Assert set with invalid item should fail
        InvalidItem = gen_invalid_data(AtmWorkflowExecutionAuth, ItemDataSpec),
        ?assertEqual(
            ?ERROR_ATM_DATA_TYPE_UNVERIFIED(InvalidItem, ItemDataSpec#atm_data_spec.type),
            ?rpc(catch atm_store_api:apply_operation(
                AtmWorkflowExecutionAuth, set, InvalidItem, #{}, AtmStoreId
            ))
        ),
        ?assertMatch(FullyExpandedInitialItem, get_content(AtmWorkflowExecutionAuth, AtmStoreId)),

        % Assert it is not possible to perform operation on store when it is frozen
        ?rpc(atm_store_api:freeze(AtmStoreId)),
        ?assertEqual(
            ?ERROR_ATM_STORE_FROZEN(AtmStoreSchema#atm_store_schema.id),
            ?rpc(catch atm_store_api:apply_operation(
                AtmWorkflowExecutionAuth, set, NewItem, #{}, AtmStoreId
            ))
        ),
        ?assertMatch(FullyExpandedInitialItem, get_content(AtmWorkflowExecutionAuth, AtmStoreId)),

        % Otherwise operation should succeed
        ?rpc(atm_store_api:unfreeze(AtmStoreId)),
        ?assertEqual(
            ok,
            ?rpc(atm_store_api:apply_operation(AtmWorkflowExecutionAuth, set, NewItem, #{}, AtmStoreId))
        ),
        ?assertMatch(FullyExpandedNewItem, get_content(AtmWorkflowExecutionAuth, AtmStoreId))

    end, example_configs()).


iterator_test(_Config) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    lists:foreach(fun(Config = #atm_single_value_store_config{item_data_spec = ItemDataSpec}) ->
        AtmStoreSchema = atm_store_test_utils:build_store_schema(Config),
        {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
            AtmWorkflowExecutionAuth, undefined, AtmStoreSchema
        ))),
        AtmStoreIteratorSpec = #atm_store_iterator_spec{
            store_schema_id = AtmStoreSchema#atm_store_schema.id,
            max_batch_size = rand:uniform(8)
        },
        AtmWorkflowExecutionEnv = atm_workflow_execution_env:build(
            atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
            atm_workflow_execution_auth:get_workflow_execution_id(AtmWorkflowExecutionAuth),
            0,
            #{AtmStoreSchema#atm_store_schema.id => AtmStoreId}
        ),

        AtmStoreIterator0 = ?rpc(atm_store_api:acquire_iterator(AtmStoreId, AtmStoreIteratorSpec)),
        ?assertEqual(stop, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, AtmStoreIterator0))),

        Item = gen_valid_data(AtmWorkflowExecutionAuth, ItemDataSpec),
        FullyExpandedItem = compress_and_expand_data(AtmWorkflowExecutionAuth, Item, ItemDataSpec),
        set_item(AtmWorkflowExecutionAuth, Item, AtmStoreId),

        AtmStoreIterator1 = ?rpc(atm_store_api:acquire_iterator(AtmStoreId, AtmStoreIteratorSpec)),
        {ok, _, AtmStoreIterator2} = ?assertMatch(
            {ok, [FullyExpandedItem], _},
            ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, AtmStoreIterator1))
        ),
        ?assertEqual(stop, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, AtmStoreIterator2))),

        %% Assert previous iterators can be reused
        ?assertMatch(
            {ok, [FullyExpandedItem], _},
            ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, AtmStoreIterator1))
        )

    end, example_configs()).


browse_test(_Config) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    lists:foreach(fun(Config = #atm_single_value_store_config{item_data_spec = ItemDataSpec}) ->
        AtmStoreSchema = atm_store_test_utils:build_store_schema(Config),
        {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
            AtmWorkflowExecutionAuth, undefined, AtmStoreSchema
        ))),
        ?assertEqual(
            {[], true},
            ?rpc(atm_store_api:browse_content(AtmWorkflowExecutionAuth, #{}, AtmStoreId))
        ),

        Item = gen_valid_data(AtmWorkflowExecutionAuth, ItemDataSpec),
        set_item(AtmWorkflowExecutionAuth, Item, AtmStoreId),
        ExpandedItem = compress_and_expand_data(AtmWorkflowExecutionAuth, Item, ItemDataSpec),

        ?assertEqual(
            {[{<<>>, {ok, ExpandedItem}}], true},
            ?rpc(atm_store_api:browse_content(AtmWorkflowExecutionAuth, #{}, AtmStoreId))
        )

    end, example_configs()).


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
-spec example_configs() -> [atm_single_value_store_config:record()].
example_configs() ->
    lists:map(fun(ItemDataType) ->
        #atm_single_value_store_config{item_data_spec = atm_store_test_utils:example_data_spec(
            ItemDataType
        )}
    end, [
        %% TODO VFS-8686 enable after implementing compress/expand for array
%%        atm_array_type,
        atm_dataset_type,
        atm_file_type,
        atm_integer_type,
        atm_object_type,
        atm_string_type,
        atm_time_series_measurements_type
    ]).


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
-spec set_item(atm_workflow_execution_auth:record(), atm_value:expanded(), atm_store:id()) ->
    ok.
set_item(AtmWorkflowExecutionAuth, Item, AtmStoreId) ->
    ?rpc(atm_store_api:apply_operation(AtmWorkflowExecutionAuth, set, Item, #{}, AtmStoreId)).


%% @private
-spec get_content(atm_workflow_execution_auth:record(), atm_store:id()) ->
    undefined | atm_value:expanded().
get_content(AtmWorkflowExecutionAuth, AtmStoreId) ->
    case ?rpc(atm_store_api:browse_content(AtmWorkflowExecutionAuth, #{}, AtmStoreId)) of
        {[], true} ->
            undefined;
        {[{_, {ok, Item}}], true} ->
            Item
    end.


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, atm_store_test_utils],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    time_test_utils:freeze_time(Config),
    Config.


end_per_group(_Group, Config) ->
    time_test_utils:unfreeze_time(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
