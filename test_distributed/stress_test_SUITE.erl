%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This test checks requests routing inside OP cluster.
%%% @end
%%%--------------------------------------------------------------------
-module(stress_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("cluster_elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("annotations/include/annotations.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([stress_test/1, t1_test/1, t2_test/1]).

-performance([{stress, [t1_test, t2_test]}, {stress_no_clearing, [t1_test, t2_test]}]).
all() ->
    [].

%%%===================================================================
%%% Test function
%% ====================================================================

-performance([
    {description, "Performs stress test"},
    {config, [{name, stress}, {description, "Basic config for stress test"}]}
]).
stress_test(Config) ->
    performance:stress_test(Config).

%%%===================================================================

-performance([
    {parameters, [
        [{name, p1}, {value, 10}, {description, "xxx"}]
    ]},
    {config, [{name, stress_test},
        {parameters, [
            [{name, p1}, {value, 100}]
        ]},
        {description, "Basic config for stress test"}
    ]}
]).
t1_test(Config) ->
    [
        #parameter{name = v1, value = ?config(p1, Config), unit = "us",
            description = "xxx"}
    ].

%%%===================================================================

-performance([
    {parameters, [
        [{name, p1}, {value, 10}, {description, "xxx"}]
    ]},
    {config, [{name, stress_test},
        {parameters, [
            [{name, p1}, {value, 100}]
        ]},
        {description, "Basic config for stress test"}
    ]}
]).
t2_test(Config) ->
    [
        #parameter{name = v1, value = ?config(p1, Config), unit = "us",
            description = "xxx"}
    ].

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================
