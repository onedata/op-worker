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
-include_lib("cluster_worker/include/elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("annotations/include/annotations.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([stress_test/1, file_meta_basic_operations_test/1]).

-define(STRESS_CASES, [
    file_meta_basic_operations_test
    %% TODO add simmilar test without mocks within cluster
    %% sequencer_manager_multiple_streams_messages_ordering_test, connection_multi_ping_pong_test,
    %% event_stream_different_file_id_aggregation_test,
    %% event_manager_multiple_subscription_test, event_manager_multiple_clients_test
]).
-define(STRESS_NO_CLEARING_CASES, [
    file_meta_basic_operations_test
    %% TODO add no clearing option to other tests
]).

all() ->
    ?ALL(?STRESS_CASES, ?STRESS_NO_CLEARING_CASES).

%%%===================================================================
%%% Test functions
%%%===================================================================

stress_test(Config) ->   ?STRESS(Config,[
            {description, "Main stress test function. Links together all cases to be done multiple times as one continous test."},
            {success_rate, 99.9},
            {config, [{name, stress}, {description, "Basic config for stress test"}]}
        ],
        fun(Config) ->
            performance_macros:stress_test(Config)
        end
    ).

%%%===================================================================

%%-performance([
%%    {description, "Performs operations on file meta model"}
%%]).
file_meta_basic_operations_test(Config) ->
    ?PERFORMANCE(Config, [
        {description, "Performs operations on file meta model"}
      ],
      fun(Config) ->
        model_file_meta_test_SUITE:basic_operations_test_core(Config)
      end
    ).

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
