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
    events_aggregationmanager_error_test2/1
]).

all() -> ?ALL([
    events_aggregation_test,
    events_flush_test,
    events_aggregation_stream_error_test,
    events_aggregation_stream_error_test2,
    events_aggregation_manager_error_test,
    events_aggregationmanager_error_test2
]).

%%%===================================================================
%%% Test functions
%%%===================================================================


events_aggregation_stream_error_test(Config) ->
    [WorkerP1] = Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, event_stream, [passthrough]),

    test_utils:mock_expect(Workers, event_stream, send,
        fun(Stream, Message) ->
            case get(first_tested) of
                undefined ->
                    put(first_tested, true),
                    meck:passthrough([undefined, Message]);
                _ ->
                    meck:passthrough([Stream, Message])

            end
        end
    ),

    events_reliability_test_base:events_aggregation_test_base(Config, WorkerP1, WorkerP1),
    test_utils:mock_unload(Workers, event_stream).

events_aggregation_stream_error_test2(Config) ->
    [WorkerP1] = Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, event_stream, [passthrough]),

    test_utils:mock_expect(Workers, event_stream, handle_call, fun
        (#event{type = #file_read_event{}} = Request, From, State) ->
            case application:get_env(?APP_NAME, ?FUNCTION_NAME) of
                {ok, _} ->
                    meck:passthrough([Request, From, State]);
                _ ->
                    application:set_env(?APP_NAME, ?FUNCTION_NAME, true),
                    throw(test_error)

            end;
        (Request, From, State) ->
            meck:passthrough([Request, From, State])
    end),

    events_reliability_test_base:events_aggregation_test_base(Config, WorkerP1, WorkerP1),
    test_utils:mock_unload(Workers, event_stream).

events_aggregation_manager_error_test(Config) ->
    [WorkerP1] = Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, event_manager, [passthrough]),

    test_utils:mock_expect(Workers, event_manager, send,
        fun(Stream, Message) ->
            case get(first_tested) of
                undefined ->
                    put(first_tested, true),
                    meck:passthrough([undefined, Message]);
                _ ->
                    meck:passthrough([Stream, Message])

            end
        end
    ),

    events_reliability_test_base:events_aggregation_test_base(Config, WorkerP1, WorkerP1),
    test_utils:mock_unload(Workers, event_manager).

events_aggregationmanager_error_test2(Config) ->
    [WorkerP1] = Workers = ?config(op_worker_nodes, Config),
    test_utils:set_env(WorkerP1, ?APP_NAME, fuse_session_ttl_seconds, 5),
    test_utils:mock_new(Workers, event_manager, [passthrough]),

    test_utils:mock_expect(Workers, event_manager, handle_call, fun
        (#event{type = #file_read_event{}} = Request, From, State) ->
            case application:get_env(?APP_NAME, ?FUNCTION_NAME) of
                {ok, _} ->
                    meck:passthrough([Request, From, State]);
                _ ->
                    application:set_env(?APP_NAME, ?FUNCTION_NAME, true),
                    throw(test_error)

            end;
         (Request, From, State) ->
             meck:passthrough([Request, From, State])
    end),

    events_reliability_test_base:events_aggregation_failed_test_base(Config, WorkerP1, WorkerP1),
    test_utils:mock_unload(Workers, event_manager).

events_aggregation_test(Config) ->
    [WorkerP1] = ?config(op_worker_nodes, Config),
    events_reliability_test_base:events_aggregation_test_base(Config, WorkerP1, WorkerP1).


events_flush_test(Config) ->
    [WorkerP1] = ?config(op_worker_nodes, Config),
    events_reliability_test_base:events_flush_test_base(Config, WorkerP1, WorkerP1).


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
