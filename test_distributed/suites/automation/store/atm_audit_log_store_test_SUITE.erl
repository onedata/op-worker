%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation audit_log store.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_audit_log_store_test_SUITE).
-author("Lukasz Opiola").

-include("modules/automation/atm_tmp.hrl").
-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").

-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% exported for CT
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    create_store_with_invalid_args_test/1,
    create_store_with_severity_details_test/1,
    apply_operation_test/1,
    apply_operation_with_severity_details_test/1,
    iterate_one_by_one_test/1,
    iterate_in_chunks_test/1,
    reuse_iterator_test/1,
    browse_by_index_test/1,
    browse_by_offset_test/1,
    browse_by_timestamp_test/1
]).

groups() -> [
    {parallel, [parallel], [
        create_store_with_invalid_args_test,
        create_store_with_severity_details_test,
        apply_operation_test,
        apply_operation_with_severity_details_test,
        iterate_one_by_one_test,
        iterate_in_chunks_test,
        reuse_iterator_test,
        browse_by_index_test,
        browse_by_offset_test
    ]},
    {sequential, [sequential], [
        browse_by_timestamp_test
    ]}
].

all() -> [
    {group, parallel},
    {group, sequential}
].


-define(ATM_AUDIT_LOG_STORE_SCHEMA, #atm_store_schema{
    id = <<"dummyId">>,
    name = <<"audit_log_store">>,
    description = <<"description">>,
    requires_initial_value = false,
    type = audit_log,
    data_spec = #atm_data_spec{type = atm_integer_type}
}).

-define(ATTEMPTS, 30).

-define(ITERATION_RESULT_MAPPER, fun(
    #{<<"entry">> := Entry, <<"timestamp">> := _Timestamp, <<"severity">> := _Severity}) -> 
        Entry;
    (BadFormatResult) -> 
        ct:print("Audit log result in bad format: ~p", [BadFormatResult]),
        throw(error)
end).

%%%===================================================================
%%% API functions
%%%===================================================================


create_store_with_invalid_args_test(_Config) ->
    atm_infinite_log_based_stores_test_common:create_store_with_invalid_args_test_base(?ATM_AUDIT_LOG_STORE_SCHEMA).


create_store_with_severity_details_test(_Config) ->
    AtmWorkflowExecutionCtx = atm_store_test_utils:create_workflow_execution_ctx(
        krakow, user1, space_krk
    ),
    
    ?assertMatch({ok, _}, atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionCtx, [#{<<"entry">> => 8, <<"severity">> => <<"error">>}], ?ATM_AUDIT_LOG_STORE_SCHEMA
    )).


apply_operation_test(_Config) ->
    atm_infinite_log_based_stores_test_common:apply_operation_test_base(?ATM_AUDIT_LOG_STORE_SCHEMA).


apply_operation_with_severity_details_test(_Config) ->
    AtmWorkflowExecutionCtx = atm_store_test_utils:create_workflow_execution_ctx(
        krakow, user1, space_krk
    ),
    
    {ok, AtmStoreId} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionCtx, undefined, ?ATM_AUDIT_LOG_STORE_SCHEMA
    ),
    ?assertEqual(ok, atm_store_test_utils:apply_operation(
        krakow, AtmWorkflowExecutionCtx, append, [8, #{<<"entry">> => 9, <<"severity">> => <<"notice">>}], #{<<"isBatch">> => true}, AtmStoreId
    )).


iterate_one_by_one_test(_Config) ->
    atm_infinite_log_based_stores_test_common:iterate_one_by_one_test_base(?ATM_AUDIT_LOG_STORE_SCHEMA, ?ITERATION_RESULT_MAPPER).


iterate_in_chunks_test(_Config) ->
    atm_infinite_log_based_stores_test_common:iterate_in_chunks_test_base(?ATM_AUDIT_LOG_STORE_SCHEMA, ?ITERATION_RESULT_MAPPER).


reuse_iterator_test(_Config) ->
    atm_infinite_log_based_stores_test_common:reuse_iterator_test_base(?ATM_AUDIT_LOG_STORE_SCHEMA, ?ITERATION_RESULT_MAPPER).


browse_by_index_test(_Config) ->
    atm_infinite_log_based_stores_test_common:browse_by_index_test_base(?ATM_AUDIT_LOG_STORE_SCHEMA, ?ITERATION_RESULT_MAPPER).


browse_by_offset_test(_Config) ->
    atm_infinite_log_based_stores_test_common:browse_by_offset_test_base(?ATM_AUDIT_LOG_STORE_SCHEMA, ?ITERATION_RESULT_MAPPER).


browse_by_timestamp_test(_Config) ->
    ok = clock_freezer_mock:set_current_time_millis(123),
    AtmWorkflowExecutionCtx = atm_store_test_utils:create_workflow_execution_ctx(
        krakow, user1, space_krk
    ),
    [Node | _] = oct_background:get_provider_nodes(krakow),
    ItemsNum = rand:uniform(1000),
    Items = lists:seq(1, ItemsNum),
    {ok, AtmStoreId} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionCtx, undefined, 
        ?ATM_AUDIT_LOG_STORE_SCHEMA#atm_store_schema{data_spec = #atm_data_spec{type = atm_object_type}}
    ),
    [FirstTimestamp | _] = lists:map(fun(Index) ->
        Timestamp = rpc:call(Node, global_clock, timestamp_millis, []),
        Entry = #{<<"value">> => Index},
        ItemToAdd = case rand:uniform(4) of
            1 -> Entry;
            2 -> #{<<"entry">> => Entry, <<"severity">> => <<"info">>};
            3 -> #{<<"entry">> => Entry};
            4 -> Entry#{<<"severity">> => <<"info">>}
        end,
        ?assertEqual(ok, atm_store_test_utils:apply_operation(
            krakow, AtmWorkflowExecutionCtx, append, ItemToAdd, #{}, AtmStoreId
        )),
        clock_freezer_mock:simulate_millis_passing(1),
        Timestamp
    end, Items),
    {ok, AtmStore} = atm_store_test_utils:get(krakow, AtmStoreId),
    lists:foreach(fun(_) ->
        StartIndex = rand:uniform(ItemsNum),
        Limit = rand:uniform(ItemsNum),
        Expected = lists:map(fun(Index) ->
            {
                integer_to_binary(Index), 
                {ok, #{
                    <<"entry">> => #{<<"value">> => Index + 1}, 
                    <<"timestamp">> => FirstTimestamp + Index, 
                    <<"severity">> => <<"info">>}
                }
            }
        end, lists:seq(StartIndex, min(StartIndex + Limit - 1, ItemsNum - 1))),
        {Result, IsLast} = atm_store_test_utils:browse_content(krakow, AtmWorkflowExecutionCtx, #{
            start_timestamp => StartIndex + FirstTimestamp,
            limit => Limit
        }, AtmStore),
        ?assertEqual( {Expected, StartIndex + Limit >= ItemsNum}, {Result, IsLast})
    end, lists:seq(1, 8)).


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite([{?LOAD_MODULES, [?MODULE]} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(browse_by_timestamp_test, Config) ->
    ct:timetrap({minutes, 5}),
    
    ok = clock_freezer_mock:setup_on_nodes(oct_background:get_all_providers_nodes(), [global_clock, ?MODULE]),
    Config;
init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
