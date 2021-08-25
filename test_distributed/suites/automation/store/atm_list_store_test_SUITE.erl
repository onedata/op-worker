%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation list store.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_list_store_test_SUITE).
-author("Michal Stanisz").

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").

-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
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
    apply_operation_test/1,
    iterate_one_by_one_test/1,
    iterate_in_chunks_test/1,
    reuse_iterator_test/1,
    browse_by_index_test/1,
    browse_by_offset_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_store_with_invalid_args_test,
        apply_operation_test,
        iterate_one_by_one_test,
        iterate_in_chunks_test,
        reuse_iterator_test,
        browse_by_index_test,
        browse_by_offset_test
    ]}
].

all() -> [
    {group, all_tests}
].


-define(ATM_LIST_STORE_SCHEMA, #atm_store_schema{
    id = <<"dummyId">>,
    name = <<"list_store">>,
    description = <<"description">>,
    requires_initial_value = false,
    type = list,
    data_spec = #atm_data_spec{type = atm_integer_type}
}).

-define(ATTEMPTS, 30).

-define(ITERATION_RESULT_MAPPER, fun(R) -> R end).

%%%===================================================================
%%% API functions
%%%===================================================================


create_store_with_invalid_args_test(_Config) ->
    atm_infinite_log_based_stores_test_common:create_store_with_invalid_args_test_base(?ATM_LIST_STORE_SCHEMA).


apply_operation_test(_Config) ->
    atm_infinite_log_based_stores_test_common:apply_operation_test_base(?ATM_LIST_STORE_SCHEMA).


iterate_one_by_one_test(_Config) ->
    atm_infinite_log_based_stores_test_common:iterate_one_by_one_test_base(?ATM_LIST_STORE_SCHEMA, ?ITERATION_RESULT_MAPPER).


iterate_in_chunks_test(_Config) ->
    atm_infinite_log_based_stores_test_common:iterate_in_chunks_test_base(?ATM_LIST_STORE_SCHEMA, ?ITERATION_RESULT_MAPPER).


reuse_iterator_test(_Config) ->
    atm_infinite_log_based_stores_test_common:reuse_iterator_test_base(?ATM_LIST_STORE_SCHEMA, ?ITERATION_RESULT_MAPPER).


browse_by_index_test(_Config) ->
    atm_infinite_log_based_stores_test_common:browse_by_index_test_base(?ATM_LIST_STORE_SCHEMA, ?ITERATION_RESULT_MAPPER).


browse_by_offset_test(_Config) ->
    atm_infinite_log_based_stores_test_common:browse_by_offset_test_base(?ATM_LIST_STORE_SCHEMA, ?ITERATION_RESULT_MAPPER).


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
