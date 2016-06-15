%%%--------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests monitoring.
%%% @end
%%%--------------------------------------------------------------------
-module(monitoring_test_SUITE).
-author("Michal Wrona").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2, counter/3]).

-export([rrd_test/1, monitoring_test/1, rrdtool_pool_test/1]).

all() ->
    ?ALL([rrd_test, monitoring_test, rrdtool_pool_test]).

-define(TIMEOUT, timer:seconds(60)).
-define(COUNTER_TIMEOUT, timer:seconds(30)).

-define(SPACE_ID, <<"s1">>).
-define(MONITORING_TYPES, [
    {space, ?SPACE_ID, storage_used},
    {space, ?SPACE_ID, storage_quota}
]).

-define(FORMATS, [json, xml]).
-define(STEPS, ['5m', '1h', '1d', '1m']).
-define(reg(W, P), rpc:call(W, worker_proxy, call, [monitoring_worker, P])).
-define(EXPECTED_SIZE, 100).

%%%===================================================================
%%% Test functions
%%%===================================================================

rrd_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ProvID = rpc:call(Worker, oneprovider, get_provider_id, []),

    lists:foreach(fun({SubjectType, SubjectId, MetricType}) ->
        %% create
        ?assertEqual(false, rpc:call(Worker, monitoring_state, exists,
            [SubjectType, SubjectId, MetricType, ProvID])),

        ?assertEqual(ok, rpc:call(Worker, rrd_utils, create_rrd,
            [SubjectType, SubjectId, MetricType])),
        ?assertEqual(true, rpc:call(Worker, monitoring_state, exists,
            [SubjectType, SubjectId, MetricType, ProvID])),

        {ok, #monitoring_state{rrd_file = RRDFile} = State} =
            rpc:call(Worker, monitoring_state, get, [SubjectType, SubjectId, MetricType]),
        ?assertNotEqual(undefinied, RRDFile),

        %% second create
        ?assertEqual(already_exists, rpc:call(Worker, rrd_utils, create_rrd,
            [SubjectType, SubjectId, MetricType])),
        ?assertEqual({ok, State}, rpc:call(Worker, monitoring_state, get,
            [SubjectType, SubjectId, MetricType])),

        %% update
        {ok, #monitoring_state{rrd_file = UpdatedRRDFile}} = ?assertMatch(
            {ok, #monitoring_state{}}, rpc:call(Worker, rrd_utils, update_rrd,
                [SubjectType, SubjectId, MetricType, 100])),
        ?assertNotEqual(RRDFile, UpdatedRRDFile),

        ProviderId = rpc:call(Worker, oneprovider, get_provider_id, []),
        %% export
        lists:foreach(fun(Step) ->
            lists:foreach(fun(Format) ->
                ?assertMatch({ok, _}, rpc:call(Worker, rrd_utils, export_rrd,
                    [SubjectType, SubjectId, MetricType, Step, Format, ProviderId]))
            end,
                ?FORMATS)
        end,
            ?STEPS)
    end,
        ?MONITORING_TYPES).

monitoring_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ProvID = rpc:call(Worker, oneprovider, get_provider_id, []),

    lists:foreach(fun({SubjectType, SubjectId, MetricType}) ->
        %% start
        ?assertEqual(false, rpc:call(Worker, monitoring_state, exists,
            [SubjectType, SubjectId, MetricType, ProvID])),

        ?assertEqual(ok, ?reg(Worker, {start, SubjectType, SubjectId, MetricType})),

        ?assertEqual(true, rpc:call(Worker, monitoring_state, exists,
            [SubjectType, SubjectId, MetricType, ProvID])),

        {ok, #monitoring_state{rrd_file = RRDFile, active = Active} = State} =
            rpc:call(Worker, monitoring_state, get, [SubjectType, SubjectId, MetricType]),
        ?assertNotEqual(undefinied, RRDFile),
        ?assertEqual(true, Active),

        %% second start
        ?assertEqual(ok, ?reg(Worker, {start, SubjectType, SubjectId, MetricType})),
        ?assertEqual({ok, State}, rpc:call(Worker, monitoring_state, get,
            [SubjectType, SubjectId, MetricType])),

        %% update
        ?assertEqual(ok, ?reg(Worker, {update, SubjectType, SubjectId, MetricType})),
        ?assertEqual(ok, ?reg(Worker, {update, SubjectType, SubjectId, MetricType})),

        %% export
        lists:foreach(fun(Step) ->
            lists:foreach(fun(Format) ->
                ?assertMatch({ok, _}, ?reg(Worker, {export, SubjectType,
                    SubjectId, MetricType, Step, Format, ProvID}))
            end,
                ?FORMATS)
        end,
            ?STEPS),

        %% stop
        ?assertEqual(ok, ?reg(Worker, {stop, SubjectType, SubjectId, MetricType})),

        {ok, #monitoring_state{active = UnActive}} =
            rpc:call(Worker, monitoring_state, get, [SubjectType, SubjectId, MetricType]),
        ?assertEqual(false, UnActive)
    end,
        ?MONITORING_TYPES).

rrdtool_pool_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    CounterPid = spawn(?MODULE, counter, [0, ?EXPECTED_SIZE, self()]),

    lists:foreach(fun(Id) ->
        spawn(fun() ->
            ?assertEqual(ok, ?reg(Worker, {start, space, integer_to_binary(Id), storage_used})),
            ?assertEqual(ok, ?reg(Worker, {update, space, integer_to_binary(Id), storage_used})),
            ?assertEqual(ok, ?reg(Worker, {update, space, integer_to_binary(Id), storage_used})),
            CounterPid ! ok
        end)
    end, lists:seq(1, ?EXPECTED_SIZE)),

    ?assertReceivedMatch({count, ?EXPECTED_SIZE}, ?TIMEOUT).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    EnvUpResult = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")),
    [Worker | _] = ?config(op_worker_nodes, EnvUpResult),
    test_utils:mock_new(Worker, space_info),
    test_utils:mock_expect(Worker, space_info, 'after',
        fun(_, _, _, _, _) -> ok end),
    ?assertMatch({ok, _}, rpc:call(Worker, space_quota, create, [#document{
        key = ?SPACE_ID,
        value = #space_quota{
            current_size = 100
        }
    }])),
    ?assertMatch({ok, _}, rpc:call(Worker, space_info, create, [#document{
        key = ?SPACE_ID,
        value = #space_info{
            providers_supports = [{oneprovider:get_provider_id(), 1000}]
        }
    }])),
    EnvUpResult.


end_per_suite(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker, space_info),
    test_node_starter:clean_environment(Config).

init_per_testcase(rrdtool_pool_test, Config)->
    [Worker | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Id) ->
        ?assertMatch({ok, _}, rpc:call(Worker, space_quota, create, [#document{
            key = integer_to_binary(Id),
            value = #space_quota{
                current_size = 100
            }
        }]))
    end, lists:seq(1, ?EXPECTED_SIZE)),
    Config;

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(rrdtool_pool_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Id) ->
        ?assertMatch(ok, rpc:call(Worker, space_quota, delete, [integer_to_binary(Id)]))
    end, lists:seq(1, ?EXPECTED_SIZE)),
    end_per_testcase(all, Config);

end_per_testcase(_, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    clear_state(Worker),
    ok.

clear_state(Worker) ->
    lists:foreach(fun({SubjectType, SubjectId, MetricType}) ->
        ?assertMatch(ok, rpc:call(Worker, monitoring_state, delete,
            [SubjectType, SubjectId, MetricType]))
    end,
        ?MONITORING_TYPES).

counter(CurrentCount, ExpectedCount, ResponsePid) ->
    case CurrentCount == ExpectedCount of
        true ->
            ResponsePid ! {count, CurrentCount};
        false ->
            receive
                ok ->
                    counter(CurrentCount + 1, ExpectedCount, ResponsePid)
            after
                ?COUNTER_TIMEOUT ->
                    ResponsePid ! {count, CurrentCount}
            end
    end.
