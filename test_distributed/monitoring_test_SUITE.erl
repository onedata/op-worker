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

-define(SPACE_ID, <<"674a4b28461d31f662c8bcce592594bf674a4b28461d31f662c8bcce592594bf674a4b28461d31f662c8bcce592594bf674a4b28461d31f662c8bcce592594bf">>).
-define(USER_ID, <<"674a4b28461d31f662c8bcce592594bf674a4b28461d31f662c8bcce592594bf674a4b28461d31f662c8bcce592594bf674a4b28461d31f662c8bcce592594bf">>).

-define(BLOCK_ACCESS_UPDATE, #{
    update_value => #{write_operations_counter => 10, read_operations_counter => 40},
    is_updated => fun(BufferState) ->
        case maps:get(write_operations_counter, BufferState) of
            10 ->
                case maps:get(read_operations_counter, BufferState) of
                    40 ->
                        true;
                    _ ->
                        false
                end;
            _ ->
                false
        end
    end
}).

-define(DATA_ACCESS_UPDATE, #{
    update_value => #{write_counter => 25, read_counter => 80},
    is_updated => fun(BufferState) ->
        case maps:get(write_counter, BufferState) of
            25 ->
                case maps:get(read_counter, BufferState) of
                    80 ->
                        true;
                    _ ->
                        false
                end;
            _ ->
                false
        end
    end
}).

-define(REMOTE_TRANSFER_UPDATE, #{
    update_value => #{transfer_in => 10},
    is_updated => fun(BufferState) ->
        case maps:get(transfer_in, BufferState) of
            80 ->
                true;
            _ ->
                false
        end
    end
}).

-define(MONITORING_TYPES, [
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = storage_used
    }, none, [100]},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = storage_quota
    }, none, [100]},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = connected_users
    }, none, [100]},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = storage_used,
        secondary_subject_type = user,
        secondary_subject_id = ?USER_ID
    }, #{
        update_value => 60,
        is_updated => fun(BufferState) ->
            case maps:get(storage_used, BufferState) of
                60 ->
                   true;
                _ ->
                   false
            end
        end
    }, [100]},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = block_access
    }, ?BLOCK_ACCESS_UPDATE, [100, 100]},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = block_access,
        secondary_subject_type = user,
        secondary_subject_id = ?USER_ID
    }, ?BLOCK_ACCESS_UPDATE, [100, 100]},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = data_access
    }, ?DATA_ACCESS_UPDATE, [100, 100]},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = data_access,
        secondary_subject_type = user,
        secondary_subject_id = ?USER_ID
    }, ?DATA_ACCESS_UPDATE, [100, 100]},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = remote_transfer
    }, ?REMOTE_TRANSFER_UPDATE, [100]},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = remote_transfer,
        secondary_subject_type = user,
        secondary_subject_id = ?USER_ID
    }, ?REMOTE_TRANSFER_UPDATE, [100]}
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

    lists:foreach(fun({MonitoringId, _, UpdateValue}) ->
        %% create
        ?assertEqual(false, rpc:call(Worker, monitoring_state, exists, [MonitoringId])),

        ?assertEqual(ok, rpc:call(Worker, rrd_utils, create_rrd, [MonitoringId, #{}])),
        ?assertEqual(true, rpc:call(Worker, monitoring_state, exists, [MonitoringId])),

        {ok, #document{value = #monitoring_state{rrd_file = RRDFile} = State} = Doc} =
            rpc:call(Worker, monitoring_state, get, [MonitoringId]),
        ?assertNotEqual(undefinied, RRDFile),

        %% second create
        ?assertEqual(already_exists, rpc:call(Worker, rrd_utils, create_rrd, [MonitoringId, #{}])),
        ?assertEqual({ok, Doc}, rpc:call(Worker, monitoring_state, get, [MonitoringId])),

        %% restart monitoring
        ?assertMatch({ok, _}, rpc:call(Worker, monitoring_state, save,
            [Doc#document{value = State#monitoring_state{active = false}}])),
        ?assertEqual(ok, rpc:call(Worker, rrd_utils, create_rrd, [MonitoringId, #{}])),
        ?assertMatch({ok, #document{value = #monitoring_state{active = true}}},
            rpc:call(Worker, monitoring_state, get, [MonitoringId])),

        %% update
        {ok, #monitoring_state{rrd_file = UpdatedRRDFile}} = ?assertMatch(
            {ok, #monitoring_state{}}, rpc:call(Worker, rrd_utils, update_rrd,
                [MonitoringId, State, UpdateValue])),
        ?assertNotEqual(RRDFile, UpdatedRRDFile),

        %% export
        lists:foreach(fun(Step) ->
            lists:foreach(fun(Format) ->
                ?assertMatch({ok, _}, rpc:call(Worker, rrd_utils, export_rrd,
                    [MonitoringId, Step, Format]))
            end,
                ?FORMATS)
        end,
            ?STEPS)
    end,
        ?MONITORING_TYPES).

monitoring_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    lists:foreach(fun({MonitoringId, StateBufferTest, _}) ->
        %% start
        ?assertEqual(false, rpc:call(Worker, monitoring_state, exists, [MonitoringId])),

        ?assertEqual(ok, ?reg(Worker, {start, MonitoringId})),

        ?assertEqual(true, rpc:call(Worker, monitoring_state, exists, [MonitoringId])),

        {ok, #document{value = #monitoring_state{rrd_file = RRDFile, active = Active} = State}} =
            rpc:call(Worker, monitoring_state, get, [MonitoringId]),
        ?assertNotEqual(undefinied, RRDFile),
        ?assertEqual(true, Active),

        %% second start
        ?assertEqual(ok, ?reg(Worker, {start, MonitoringId})),

        %% update
        ?assertEqual(ok, ?reg(Worker, {update, MonitoringId})),

        case StateBufferTest of
            none -> ok;
            _ ->
                #monitoring_state{state_buffer = StateBuffer} = State,
                UpdateValue = maps:get(update_value, StateBufferTest),
                IsUpdated = maps:get(is_updated, StateBufferTest),

                ?assertEqual(false, IsUpdated(StateBuffer)),
                ?reg(Worker, {update_buffer_state, MonitoringId, UpdateValue}),

                {ok, #document{value = #monitoring_state{state_buffer = UpdatedStateBuffer}}} =
                    rpc:call(Worker, monitoring_state, get, [MonitoringId]),

                ?assertEqual(true, IsUpdated(UpdatedStateBuffer))
        end,

        ?assertEqual(ok, ?reg(Worker, {update, MonitoringId})),

        %% export
        lists:foreach(fun(Step) ->
            lists:foreach(fun(Format) ->
                ?assertMatch({ok, _}, ?reg(Worker, {export, MonitoringId, Step, Format}))
            end,
                ?FORMATS)
        end,
            ?STEPS),

        %% stop
        ?assertEqual(ok, ?reg(Worker, {stop, MonitoringId})),

        {ok, #document{value = #monitoring_state{active = UnActive}}} =
            rpc:call(Worker, monitoring_state, get, [MonitoringId]),
        ?assertEqual(false, UnActive)
    end,
        ?MONITORING_TYPES).

rrdtool_pool_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    CounterPid = spawn(?MODULE, counter, [0, ?EXPECTED_SIZE, self()]),

    lists:foreach(fun(Id) ->
        spawn(fun() ->
            MonitoringId = #monitoring_id{
                main_subject_type = space,
                main_subject_id = integer_to_binary(Id),
                metric_type = storage_used
            },
            ?assertEqual(ok, ?reg(Worker, {start, MonitoringId})),
            ?assertEqual(ok, ?reg(Worker, {update, MonitoringId})),
            ?assertEqual(ok, ?reg(Worker, {update, MonitoringId})),
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
    lists:foreach(fun({MonitoringId, _, _}) ->
        ?assertMatch(ok, rpc:call(Worker, monitoring_state, delete, [MonitoringId]))
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
