%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests reliability of events for 1 op.
%%% @end
%%%--------------------------------------------------------------------
-module(events_reliability_test_SUITE).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("modules/events/definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, init_per_testcase/2,
    end_per_testcase/2, end_per_suite/1
]).

%%tests
-export([
    events_aggregation_test/1,
    events_flush_test/1,
    events_aggregation_stream_error_test/1,
    events_aggregation_stream_error_test2/1,
    events_aggregation_manager_error_test/1,
    events_aggregation_manager_error_test2/1,
    events_flush_stream_error_test/1,
    events_flush_handler_error_test/1
]).

all() -> ?ALL([
    events_aggregation_test,
    events_flush_test,
    events_aggregation_stream_error_test,
    events_aggregation_stream_error_test2,
    events_aggregation_manager_error_test,
    % TODO VFS-5293
%%    events_aggregation_manager_error_test2,
    events_flush_stream_error_test,
    events_flush_handler_error_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================


events_aggregation_stream_error_test(Config) ->
    events_reliability_test_base:events_aggregation_stream_error_test(Config).

events_aggregation_stream_error_test2(Config) ->
    events_reliability_test_base:events_aggregation_stream_error_test2(Config).

events_aggregation_manager_error_test(Config) ->
    events_reliability_test_base:events_aggregation_manager_error_test(Config).

events_aggregation_manager_error_test2(Config) ->
    events_reliability_test_base:events_aggregation_manager_error_test2(Config).

events_aggregation_test(Config) ->
    events_reliability_test_base:events_aggregation_test(Config).


events_flush_stream_error_test(Config) ->
    events_reliability_test_base:events_flush_stream_error_test(Config).

events_flush_handler_error_test(Config) ->
    events_reliability_test_base:events_flush_handler_error_test(Config).

events_flush_test(Config) ->
    events_reliability_test_base:events_flush_test(Config).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    events_reliability_test_base:init_per_suite(Config).


init_per_testcase(Case, Config) ->
    events_reliability_test_base:init_per_testcase(Case, Config).


end_per_suite(Config) ->
    events_reliability_test_base:end_per_suite(Config).


end_per_testcase(Case, Config) ->
    events_reliability_test_base:end_per_testcase(Case, Config).
