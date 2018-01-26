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

-include("modules/datastore/datastore_models.hrl").
-include("modules/events/definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("modules/monitoring/rrd_definitions.hrl").


%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2, counter/3]).

-export([rrd_test/1, monitoring_test/1, rrdtool_pool_test/1]).

all() ->
    ?ALL([rrd_test, monitoring_test, rrdtool_pool_test]).

-define(TIMEOUT, timer:seconds(60)).
-define(COUNTER_TIMEOUT, timer:seconds(30)).

-define(SPACE_ID, <<"space1">>).
-define(USER_ID, <<"674a4b28461d31f662c8bcce592594bf674a4b28461d31f662c8bcce592594bf674a4b28461d31f662c8bcce592594bf674a4b28461d31f662c8bcce592594bf">>).

-define(GET_PROVIDER_ID(__Worker), rpc:call(__Worker, oneprovider, get_id, [])).

-define(MONITORING_TYPES(__Worker), [
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = storage_used,
        provider_id = ?GET_PROVIDER_ID(__Worker)
    }, [100], <<"<row><v>1.0000000000e+02</v></row>">>},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = storage_quota,
        provider_id = ?GET_PROVIDER_ID(__Worker)
    }, [100], <<"<row><v>1.0000000000e+03</v></row>">>},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = connected_users,
        provider_id = ?GET_PROVIDER_ID(__Worker)
    }, [100], <<"<row><v>1.0000000000e+00</v></row>">>},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = storage_used,
        secondary_subject_type = user,
        secondary_subject_id = ?USER_ID,
        provider_id = ?GET_PROVIDER_ID(__Worker)
    }, [100], <<"<row><v>1.0000000000e+06</v></row>">>},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = block_access,
        provider_id = ?GET_PROVIDER_ID(__Worker)
    }, [100, 100], <<"<row><v>1.0000000000e+00</v><v>1.0000000000e+00</v></row>">>},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = block_access,
        secondary_subject_type = user,
        secondary_subject_id = ?USER_ID,
        provider_id = ?GET_PROVIDER_ID(__Worker)
    }, [100, 100], <<"<row><v>1.0000000000e+00</v><v>1.0000000000e+00</v></row>">>},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = data_access,
        provider_id = ?GET_PROVIDER_ID(__Worker)
    }, [100, 100], <<"<row><v>1.0000000000e+00</v><v>1.0000000000e+00</v></row>">>},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = data_access,
        secondary_subject_type = user,
        secondary_subject_id = ?USER_ID,
        provider_id = ?GET_PROVIDER_ID(__Worker)
    }, [100, 100], <<"<row><v>1.0000000000e+00</v><v>1.0000000000e+00</v></row>">>},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = remote_transfer,
        provider_id = ?GET_PROVIDER_ID(__Worker)
    }, [100], <<"<row><v>8.0000000000e+00</v></row>">>},
    {#monitoring_id{
        main_subject_type = space,
        main_subject_id = ?SPACE_ID,
        metric_type = remote_transfer,
        secondary_subject_type = user,
        secondary_subject_id = ?USER_ID,
        provider_id = ?GET_PROVIDER_ID(__Worker)
    }, [100], <<"<row><v>8.0000000000e+00</v></row>">>}
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

    lists:foreach(fun({MonitoringId, UpdateValue, _}) ->
        %% create
        CurrentTime = time_utils:system_time_seconds(),
        ?assertEqual(false, rpc:call(Worker, monitoring_state, exists, [MonitoringId])),

        ?assertEqual(ok, rpc:call(Worker, rrd_utils, create_rrd,
            [?SPACE_ID, MonitoringId, #{}, CurrentTime - ?STEP_IN_SECONDS])),
        ?assertEqual(true, rpc:call(Worker, monitoring_state, exists, [MonitoringId])),

        %% second create
        ?assertEqual(ok, rpc:call(Worker, rrd_utils, create_rrd, [?SPACE_ID,
            MonitoringId, #{}, CurrentTime - ?STEP_IN_SECONDS])),

        %% update
        {ok, #document{value = #monitoring_state{} = State}} =
            rpc:call(Worker, monitoring_state, get, [MonitoringId]),
        ?assertEqual(ok, rpc:call(Worker, rrd_utils, update_rrd,
            [MonitoringId, State, CurrentTime, UpdateValue])),

        %% export
        lists:foreach(fun(Step) ->
            lists:foreach(fun(Format) ->
                ?assertMatch({ok, _}, rpc:call(Worker, rrd_utils, export_rrd,
                    [MonitoringId, Step, Format]))
            end, ?FORMATS)
        end, ?STEPS)
    end, ?MONITORING_TYPES(Worker)).

monitoring_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {ok, Docs} = rpc:call(Worker, monitoring_state, list, []),
    ?assertEqual(0, length(Docs)),

    rpc:call(Worker, monitoring_event, emit_od_space_updated, [?SPACE_ID]),

    rpc:call(Worker, monitoring_event, emit_storage_used_updated, [?SPACE_ID, ?USER_ID, 950000]),
    rpc:call(Worker, monitoring_event, emit_storage_used_updated, [?SPACE_ID, ?USER_ID, 50000]),

    rpc:call(Worker, monitoring_event, emit_file_read_statistics, [?SPACE_ID, ?USER_ID, 0, 300]),
    rpc:call(Worker, monitoring_event, emit_file_read_statistics, [?SPACE_ID, ?USER_ID, 300, 0]),

    rpc:call(Worker, monitoring_event, emit_file_written_statistics, [?SPACE_ID, ?USER_ID, 150, 299]),
    rpc:call(Worker, monitoring_event, emit_file_written_statistics, [?SPACE_ID, ?USER_ID, 150, 1]),

    rpc:call(Worker, monitoring_event, emit_rtransfer_statistics, [?SPACE_ID, ?USER_ID, 100]),
    rpc:call(Worker, monitoring_event, emit_rtransfer_statistics, [?SPACE_ID, ?USER_ID, 200]),

    Self = self(),
    Ref = make_ref(),
    ProviderId = ?GET_PROVIDER_ID(Worker),
    rpc:call(Worker, event, flush, [#flush_events{
        provider_id = ?GET_PROVIDER_ID(Worker),
        subscription_id = ?MONITORING_SUB_ID,
        notify = fun(_) -> Self ! {Ref, ok} end
    }, ?ROOT_SESS_ID]),
    ?assertReceivedMatch({Ref, ok}, ?TIMEOUT),

    timer:sleep(timer:seconds(?STEP_IN_SECONDS)),

    {ok, UpdatedDocs} = rpc:call(Worker, monitoring_state, list, []),
    ?assertEqual(10, length(UpdatedDocs)),

    lists:foreach(fun({MonitoringId, _, ExpectedValue}) ->
        {ok, ExportedMetric} = ?assertMatch({ok, _}, ?reg(Worker,
            {export, MonitoringId#monitoring_id{provider_id = ProviderId}, '5m', xml})),
        ?assertNotEqual(nomatch, binary:match(ExportedMetric, ExpectedValue))
    end, ?MONITORING_TYPES(Worker)).

rrdtool_pool_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    CounterPid = spawn(?MODULE, counter, [0, ?EXPECTED_SIZE, self()]),

    lists:foreach(fun(Id) ->
        spawn(fun() ->
            MonitoringId = #monitoring_id{
                main_subject_type = space,
                main_subject_id = integer_to_binary(Id),
                metric_type = storage_used,
                provider_id = ?GET_PROVIDER_ID(Worker)
            },
            CurrentTime = time_utils:system_time_seconds(),
            ?assertEqual(ok, rpc:call(Worker, monitoring_utils, create,
                [?SPACE_ID, MonitoringId, CurrentTime - ?STEP_IN_SECONDS])),
            {ok, #document{value = MonitoringState}} =
                rpc:call(Worker, monitoring_state, get, [MonitoringId]),

            ?assertEqual(ok, rpc:call(Worker, monitoring_utils, update,
                [MonitoringId, MonitoringState, CurrentTime, #{}])),
            ?assertEqual(ok, rpc:call(Worker, monitoring_utils, update,
                [MonitoringId, MonitoringState, CurrentTime + ?STEP_IN_SECONDS, #{}])),
            CounterPid ! ok
        end)
    end, lists:seq(1, ?EXPECTED_SIZE)),

    ?assertReceivedMatch({count, ?EXPECTED_SIZE}, ?TIMEOUT).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        [Worker | _] = ?config(op_worker_nodes, NewConfig),
        start_monitoring_worker(Worker),
        ?assertMatch({ok, _}, rpc:call(Worker, space_quota, create, [#document{
            key = ?SPACE_ID, value = #space_quota{current_size = 100}}])),
        initializer:setup_storage(NewConfig)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(monitoring_test = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, space_quota),
    test_utils:mock_expect(Worker, space_quota, get, fun(_) ->
        {ok, #document{value = #space_quota{current_size = 100}}} end),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(rrdtool_pool_test = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Id) ->
        ?assertMatch({ok, _}, rpc:call(Worker, space_quota, create, [#document{
            key = integer_to_binary(Id),
            value = #space_quota{
                current_size = 100
            }
        }]))
    end, lists:seq(1, ?EXPECTED_SIZE)),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(
        ?TEST_FILE(Config, "env_desc.json"), Config),
    clear_state(Worker),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(monitoring_test = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker, space_quota),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(rrdtool_pool_test = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Id) ->
        ?assertMatch(ok, rpc:call(Worker, space_quota, delete, [integer_to_binary(Id)]))
    end, lists:seq(1, ?EXPECTED_SIZE)),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config).

clear_state(Worker) ->
    {ok, Docs} = rpc:call(Worker, monitoring_state, list, []),
    lists:foreach(fun(#document{key = Id, value = #monitoring_state{rrd_guid = Guid}}) ->
        lfm_proxy:unlink(Worker, ?ROOT_SESS_ID, {guid, Guid}),
        ?assertMatch(ok, rpc:call(Worker, monitoring_state, delete, [Id]))
    end, Docs).

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

start_monitoring_worker(Worker) ->
    Args = [
        {supervisor_flags, rpc:call(Worker, monitoring_worker, supervisor_flags, [])},
        {supervisor_children_spec, rpc:call(Worker, monitoring_worker, supervisor_children_spec, [])}
    ],
    ok = rpc:call(Worker, node_manager, start_worker, [monitoring_worker, Args]).