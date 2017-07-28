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
-module(files_stress_test_SUITE).
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
-export([stress_test/1, stress_test_base/1, many_files_creation_tree_test/1,
    many_files_creation_tree_test_base/1]).

-define(STRESS_CASES, []).
-define(STRESS_NO_CLEARING_CASES, [
    many_files_creation_tree_test
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

many_files_creation_tree_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, spawn_beg_level}, {value, 4}, {description, "Level of tree to start spawning processes"}],
            [{name, spawn_end_level}, {value, 5}, {description, "Level of tree to stop spawning processes"}],
            [{name, dir_level}, {value, 6}, {description, "Level of last test directory"}],
            [{name, dirs_per_parent}, {value, 6}, {description, "Child directories in single dir"}],
            [{name, files_per_dir}, {value, 40}, {description, "Number of files in single directory"}]
        ]},
        {description, "Creates directories' and files' tree using multiple process"}
    ]).
many_files_creation_tree_test_base(Config) ->
    files_stress_test_base:many_files_creation_tree_test_base(Config, true).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    files_stress_test_base:init_per_suite(Config).

init_per_testcase(Case, Config) ->
    files_stress_test_base:init_per_testcase(Case, Config).

end_per_testcase(Case, Config) ->
    files_stress_test_base:end_per_testcase(Case, Config).
