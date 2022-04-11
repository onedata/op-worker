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
    update_content_test/1,
    iterator_test/1,
    browse_content_test/1
]).

groups() -> [
    {singular_item_based_stores_common_tests, [parallel], [
        create_test,
        update_content_test,
        browse_content_test
    ]},
    {single_value_store_specific_tests, [parallel], [
        iterator_test
    ]}
].

all() -> [
    {group, singular_item_based_stores_common_tests},
    {group, single_value_store_specific_tests}
].


-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).
-define(erpc(Expr), ?erpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% Test functions
%%%===================================================================


create_test(_Config) ->
    atm_singleton_content_based_stores_test_base:create_test_base(
        example_configs(),
        fun get_item_data_spec/1
    ).


update_content_test(_Config) ->
    atm_singleton_content_based_stores_test_base:update_content_test_base(
        example_configs(),
        fun get_item_data_spec/1,
        #atm_single_value_store_content_update_options{},
        fun get_content/2
    ).


browse_content_test(_Config) ->
    atm_singleton_content_based_stores_test_base:browse_content_test_base(
        example_configs(),
        fun get_item_data_spec/1,
        #atm_single_value_store_content_browse_options{},
        fun set_content/3,
        fun(Content) -> #atm_single_value_store_content_browse_result{item = {ok, Content}} end
    ).


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
        set_content(AtmWorkflowExecutionAuth, Item, AtmStoreId),

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
        atm_range_type,
        atm_string_type,
        atm_time_series_measurement_type
    ]).


%% @private
-spec get_item_data_spec(atm_single_value_store_config:record()) -> atm_data_spec:record().
get_item_data_spec(#atm_single_value_store_config{item_data_spec = ItemDataSpec}) ->
    ItemDataSpec.


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
-spec set_content(atm_workflow_execution_auth:record(), atm_value:expanded(), atm_store:id()) ->
    ok.
set_content(AtmWorkflowExecutionAuth, Item, AtmStoreId) ->
    ?rpc(atm_store_api:update_content(
        AtmWorkflowExecutionAuth,
        Item,
        #atm_single_value_store_content_update_options{},
        AtmStoreId
    )).


%% @private
-spec get_content(atm_workflow_execution_auth:record(), atm_store:id()) ->
    undefined | atm_value:expanded().
get_content(AtmWorkflowExecutionAuth, AtmStoreId) ->
    BrowseOpts = #atm_single_value_store_content_browse_options{},
    try
        #atm_single_value_store_content_browse_result{
            item = {ok, Item}
        } = ?erpc(atm_store_api:browse_content(
            AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId
        )),
        Item
    catch throw:?ERROR_ATM_STORE_CONTENT_NOT_SET(_) ->
        undefined
    end.


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE | atm_singleton_content_based_stores_test_base:modules_to_load()],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(singular_item_based_stores_common_tests, Config) ->
    atm_singleton_content_based_stores_test_base:init_per_group(Config);
init_per_group(single_value_store_specific_tests, Config) ->
    time_test_utils:freeze_time(Config),
    Config.


end_per_group(singular_item_based_stores_common_tests, Config) ->
    atm_singleton_content_based_stores_test_base:end_per_group(Config);
end_per_group(single_value_store_specific_tests, Config) ->
    time_test_utils:unfreeze_time(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
