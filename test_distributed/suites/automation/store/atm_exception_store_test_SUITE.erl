%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation exception store.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_exception_store_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
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
    browse_content_by_index_test/1,
    browse_content_by_offset_test/1
]).

groups() -> [
    {infinite_log_based_stores_common_tests, [parallel], [
        create_test,
        update_content_test,
        iterator_test,
        browse_content_by_index_test,
        browse_content_by_offset_test
    ]}
].

all() -> [
    {group, infinite_log_based_stores_common_tests}
].


-define(STORE_SCHEMA(__CONFIG), #atm_system_store_schema{
    id = ?RAND_STR(16),
    name = ?RAND_STR(16),
    type = exception,
    config = __CONFIG
}).

-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% API functions
%%%===================================================================


create_test(_Config) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    lists:foreach(fun(AtmStoreConfig) ->
        InputItemGeneratorSeedDataSpec = get_input_item_generator_seed_data_spec(AtmStoreConfig),

        ValidInputItemDataSeed = gen_valid_data(
            AtmWorkflowExecutionAuth, InputItemGeneratorSeedDataSpec
        ),
        ValidInputItem = input_item_formatter(ValidInputItemDataSeed),

        % Assert creating store with initial content fails
        ?assertMatch(
            {'EXIT', {function_clause, _}},
            ?rpc(catch create_store(AtmWorkflowExecutionAuth, [ValidInputItem], AtmStoreConfig))
        ),

        ?assertMatch(
            {ok, #document{value = #atm_store{initial_content = undefined, frozen = false}}},
            ?rpc(create_store(AtmWorkflowExecutionAuth, undefined, AtmStoreConfig))
        )
    end, example_configs()).


update_content_test(_Config) ->
    atm_infinite_log_based_stores_test_base:update_content_test_base(#{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        describe_item => fun describe_item/4,
        build_content_update_options => fun build_content_update_options/1,
        browse_content => fun browse_content/2
    }),

    % Assert it is possible to insert e.g. not existing file as exception store
    % does not validate its input
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    {ok, #document{key = AtmStoreId}} = ?rpc(create_store(
        AtmWorkflowExecutionAuth, undefined, #atm_exception_store_config{
            item_data_spec = #atm_file_data_spec{file_type = 'REG'}
        }
    )),
    {ok, NonExistingObjectId} = file_id:guid_to_objectid(<<"none">>),
    ?assertEqual(ok, ?rpc(atm_store_api:update_content(
        AtmWorkflowExecutionAuth,
        #atm_item_execution{trace_id = <<"ASD">>, value = #{<<"file_id">> => NonExistingObjectId}},
        #atm_exception_store_content_update_options{function = append},
        AtmStoreId
    ))).


iterator_test(_Config) ->
    atm_infinite_log_based_stores_test_base:iterator_test_base(#{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        input_item_to_exp_iterated_item => fun input_item_to_exp_iterated_item/4,
        randomly_remove_entity_referenced_by_item => fun randomly_remove_entity_referenced_by_item/3,
        iterator_get_next => fun(AtmWorkflowExecutionEnv, Iterator) ->
            ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator))
        end
    }).


browse_content_by_index_test(_Config) ->
    atm_infinite_log_based_stores_test_base:browse_content_test_base(index, #{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        describe_item => fun describe_item/4,
        randomly_remove_entity_referenced_by_item => fun randomly_remove_entity_referenced_by_item/3,
        build_content_browse_options => fun build_content_browse_options/1,
        build_content_browse_result => fun build_content_browse_result/2
    }).


browse_content_by_offset_test(_Config) ->
    atm_infinite_log_based_stores_test_base:browse_content_test_base(offset, #{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        describe_item => fun describe_item/4,
        randomly_remove_entity_referenced_by_item => fun randomly_remove_entity_referenced_by_item/3,
        build_content_browse_options => fun build_content_browse_options/1,
        build_content_browse_result => fun build_content_browse_result/2
    }).


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
    automation:item().
gen_valid_data(AtmWorkflowExecutionAuth, ItemDataSpec) ->
    atm_store_test_utils:gen_valid_data(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, ItemDataSpec
    ).


%% @private
create_store(AtmWorkflowExecutionAuth, ContentInitializer, AtmStoreConfig) ->
    atm_store_api:create(
        AtmWorkflowExecutionAuth, ?DEBUG_AUDIT_LOG_SEVERITY_INT,
        ContentInitializer, ?STORE_SCHEMA(AtmStoreConfig)
    ).


%% @private
-spec example_configs() -> [atm_single_value_store_config:record()].
example_configs() ->
    lists:map(fun(ItemDataType) ->
        #atm_exception_store_config{item_data_spec = atm_store_test_utils:example_data_spec(
            ItemDataType
        )}
    end, atm_data_type:all_data_types()).


%% @private
-spec get_input_item_generator_seed_data_spec(atm_exception_store_config:record()) ->
    atm_data_spec:record().
get_input_item_generator_seed_data_spec(#atm_exception_store_config{item_data_spec = ItemDataSpec}) ->
    ItemDataSpec.


%% @private
-spec input_item_formatter(automation:item()) -> atm_workflow_execution_handler:item().
input_item_formatter(Item) ->
    #atm_item_execution{trace_id = ?RAND_STR(10), value = Item}.


%% @private
-spec describe_item(
    atm_workflow_execution_auth:record(),
    atm_workflow_execution_handler:item(),
    atm_store:id(),
    non_neg_integer()
) ->
    json_utils:json_term().
describe_item(AtmWorkflowExecutionAuth, ItemInitializer, ItemDataSpec, _Index) ->
    atm_store_test_utils:to_described_item(
        ?PROVIDER_SELECTOR,
        AtmWorkflowExecutionAuth,
        ItemInitializer#atm_item_execution.value,
        ItemDataSpec
    ).


%% @private
-spec input_item_to_exp_iterated_item(
    atm_workflow_execution_auth:record(),
    atm_workflow_execution_handler:item(),
    atm_store:id(),
    non_neg_integer()
) ->
    atm_workflow_execution_handler:item().
input_item_to_exp_iterated_item(AtmWorkflowExecutionAuth, ItemInitializer, ItemDataSpec, _Index) ->
    ItemInitializer#atm_item_execution{value = atm_store_test_utils:to_iterated_item(
        ?PROVIDER_SELECTOR,
        AtmWorkflowExecutionAuth,
        ItemInitializer#atm_item_execution.value,
        ItemDataSpec
    )}.


%% @private
-spec randomly_remove_entity_referenced_by_item(
    atm_workflow_execution_auth:record(),
    atm_workflow_execution_handler:item(),
    atm_data_spec:record()
) ->
    false | {true, errors:error()}.
randomly_remove_entity_referenced_by_item(AtmWorkflowExecutionAuth, Item, ItemDataSpec) ->
    atm_store_test_utils:randomly_remove_entity_referenced_by_item(
        ?PROVIDER_SELECTOR,
        AtmWorkflowExecutionAuth,
        case Item of
            #atm_item_execution{value = Value} -> Value;
            _ -> Item
        end,
        ItemDataSpec
    ).


%% @private
-spec build_content_update_options(atm_exception_store_content_update_options:update_function()) ->
    atm_exception_store_content_update_options:record().
build_content_update_options(UpdateFun) ->
    #atm_exception_store_content_update_options{function = UpdateFun}.


%% @private
-spec browse_content(atm_workflow_execution_auth:record(), atm_store:id()) ->
    [automation:item()].
browse_content(AtmWorkflowExecutionAuth, AtmStoreId) ->
    BrowseOpts = build_content_browse_options(#{<<"limit">> => 1000}),
    #atm_exception_store_content_browse_result{
        items = Items,
        is_last = true
    } = ?rpc(?PROVIDER_SELECTOR, atm_store_api:browse_content(
        AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId
    )),
    lists:map(fun({_, {ok, Item}}) -> Item end, Items).


%% @private
-spec build_content_browse_options(json_utils:json_map()) ->
    atm_exception_store_content_browse_options:record().
build_content_browse_options(OptsJson) ->
    atm_exception_store_content_browse_options:sanitize(OptsJson#{
        <<"type">> => <<"exceptionStoreContentBrowseOptions">>
    }).


%% @private
-spec build_content_browse_result([atm_store_container_infinite_log_backend:entry()], boolean()) ->
    atm_exception_store_content_browse_result:record().
build_content_browse_result(Entries, IsLast) ->
    #atm_exception_store_content_browse_result{items = Entries, is_last = IsLast}.


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE | atm_infinite_log_based_stores_test_base:modules_to_load()],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(infinite_log_based_stores_common_tests, Config) ->
    atm_infinite_log_based_stores_test_base:init_per_group(Config).


end_per_group(infinite_log_based_stores_common_tests, Config) ->
    atm_infinite_log_based_stores_test_base:end_per_group(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
