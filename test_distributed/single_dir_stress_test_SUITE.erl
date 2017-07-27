%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This SUITE contains save stress test for single provider. SUITE tests
%%% creation of large dir by single process and tree of dirs by many processes.
%%% @end
%%%--------------------------------------------------------------------
-module(single_dir_stress_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("cluster_worker/include/elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/oz/oz_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([stress_test/1, stress_test_base/1,
    single_large_dir_creation_test/1, single_large_dir_creation_test_base/1,
    single_dir_creation_test/1, single_dir_creation_test_base/1]).

-define(STRESS_CASES, [single_dir_creation_test]).
-define(STRESS_NO_CLEARING_CASES, [
    single_large_dir_creation_test
]).

-define(TIMEOUT, timer:minutes(20)).

all() ->
    ?STRESS_ALL(?STRESS_CASES, ?STRESS_NO_CLEARING_CASES).

%%%===================================================================
%%% Test functions
%%%===================================================================

stress_test(Config) ->
    ?STRESS(Config,[
            {description, "Main stress test function. Links together all cases to be done multiple times as one continous test."},
            {success_rate, 90},
            {config, [{name, stress}, {description, "Basic config for stress test"}]}
        ]
    ).
stress_test_base(Config) ->
    ?STRESS_TEST_BASE(Config).

%%%===================================================================

single_dir_creation_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, files_num}, {value, 1000}, {description, "Numer of files in dir"}]
        ]},
        {description, "Creates files in dir using single process"}
    ]).
single_dir_creation_test_base(Config) ->
    files_stress_test_base:single_dir_creation_test_base(Config, true).

single_large_dir_creation_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, files_num}, {value, 1000}, {description, "Numer of files in dir"}]
        ]},
        {description, "Creates files in dir using single process"}
    ]).
single_large_dir_creation_test_base(Config) ->
    files_stress_test_base:single_dir_creation_test_base(Config, false).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    files_stress_test_base:init_per_suite(Config).

init_per_testcase(Case, Config) ->
    files_stress_test_base:init_per_testcase(Case, Config).

end_per_testcase(Case, Config) ->
    files_stress_test_base:end_per_testcase(Case, Config).
