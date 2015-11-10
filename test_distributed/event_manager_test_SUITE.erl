%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of event manager API.
%%% @end
%%%-------------------------------------------------------------------
-module(event_manager_test_SUITE).
-author("Krzysztof Trzepla").

-include("cluster/worker/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("annotations/include/annotations.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    event_stream_the_same_file_id_aggregation_test/1,
    event_stream_different_file_id_aggregation_test/1,
    event_stream_counter_emission_rule_test/1,
    event_stream_size_emission_rule_test/1,
    event_stream_time_emission_rule_test/1,
    event_stream_crash_test/1,
    event_manager_subscription_creation_and_cancellation_test/1,
    event_manager_multiple_subscription_test/1,
    event_manager_multiple_handlers_test/1,
    event_manager_multiple_clients_test/1
]).

-performance({test_cases, [
    event_stream_the_same_file_id_aggregation_test,
    event_stream_different_file_id_aggregation_test,
    event_stream_counter_emission_rule_test,
    event_stream_size_emission_rule_test,
    event_manager_multiple_subscription_test,
    event_manager_multiple_clients_test
]}).
all() -> [
    event_stream_the_same_file_id_aggregation_test,
    event_stream_different_file_id_aggregation_test,
    event_stream_counter_emission_rule_test,
    event_stream_size_emission_rule_test,
    event_stream_time_emission_rule_test,
    event_stream_crash_test,
    event_manager_subscription_creation_and_cancellation_test,
    event_manager_multiple_subscription_test,
    event_manager_multiple_handlers_test,
    event_manager_multiple_clients_test
].

-define(TIMEOUT, timer:seconds(15)).
-define(FILE_ID(Id), <<"file_id_", (integer_to_binary(Id))/binary>>).
-define(CTR_THR(Value), [
    {name, ctr_thr}, {value, Value}, {description, "Summary events counter threshold."}
]).
-define(SIZE_THR(Value), [
    {name, size_thr}, {value, Value}, {description, "Summary events size threshold."}
]).
-define(EVT_NUM(Value), [
    {name, evt_num}, {value, Value}, {description, "Number of emitted events."}
]).
-define(SUB_NUM(Value), [
    {name, sub_num}, {value, Value}, {description, "Number of subscriptions."}
]).
-define(CLI_NUM(Value), [
    {name, cli_num}, {value, Value}, {description, "Number of connected clients."}
]).
-define(FILE_NUM(Value), [
    {name, file_num}, {value, Value},
    {description, "Number of files associated with events."}
]).
-define(EVT_SIZE(Value), [
    {name, evt_size}, {value, Value}, {description, "Size of each event."},
    {unit, "B"}
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

-performance([
    {repeats, 10},
    {parameters, [?CTR_THR(5), ?EVT_NUM(20), ?EVT_SIZE(10)]},
    {description, "Check whether events for the same file are properly aggregated."},
    {config, [{name, small_counter_threshold},
        {description, "Aggregates multiple events for stream with small counter threshold."},
        {parameters, [?CTR_THR(10), ?EVT_NUM(50000)]}
    ]},
    {config, [{name, medium_counter_threshold},
        {description, "Aggregates multiple events for stream with medium counter threshold."},
        {parameters, [?CTR_THR(100), ?EVT_NUM(50000)]}
    ]},
    {config, [{name, large_counter_threshold},
        {description, "Aggregates multiple events for stream with large counter threshold."},
        {parameters, [?CTR_THR(1000), ?EVT_NUM(50000)]}
    ]}
]).
event_stream_the_same_file_id_aggregation_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    FileId = <<"file_id">>,
    SessId = ?config(session_id, Config),
    CtrThr = ?config(ctr_thr, Config),
    EvtNum = ?config(evt_num, Config),
    EvtSize = ?config(evt_size, Config),

    {ok, SubId} = subscribe(Worker,
        all,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(Meta) -> Meta >= CtrThr end,
        [fun(Evts) -> Self ! {handler, Evts} end]
    ),

    % Emit events.
    {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
        lists:foreach(fun(N) ->
            emit(Worker, #write_event{file_uuid = FileId, size = EvtSize,
                counter = 1, file_size = N * EvtSize, blocks = [#file_block{
                    offset = (N - 1) * EvtSize, size = EvtSize
                }]}, SessId)
        end, lists:seq(1, EvtNum))
    end),

%%     #document{value = #file_location{file_id = FID, storage_id = SID}} = fslogic_utils:get_local_file_location({uuid, FileId}),

    % Check whether events have been aggregated and handler has been executed.
    {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
        lists:foreach(fun(N) ->
            Size = CtrThr * EvtSize,
            Offset = (N - 1) * Size,
            FileSize = N * CtrThr * EvtSize,
            ?assertReceived({handler, [#write_event{
                file_uuid = FileId, counter = CtrThr, size = Size,
                file_size = FileSize, blocks = [#file_block{
                    offset = Offset, size = Size}]
            }]}, ?TIMEOUT)
        end, lists:seq(1, EvtNum div CtrThr))
    end),

    unsubscribe(Worker, SubId),
    remove_pending_messages(),

    [emit_time(EmitTime, EmitUnit), aggr_time(AggrTime, AggrUnit),
        evt_per_sec(EvtNum, EmitUs + AggrUs)].

-performance([
    {repeats, 10},
    {parameters, [?CTR_THR(10), ?EVT_NUM(1000), ?EVT_SIZE(10), ?FILE_NUM(2)]},
    {description, "Check whether events for different files are properly aggregated."},
    {config, [{name, small_files_number},
        {description, "Aggregates multiple events for small number of files."},
        {parameters, [?CTR_THR(100), ?FILE_NUM(10)]}
    ]},
    {config, [{name, medium_files_number},
        {description, "Aggregates multiple events for medium number of files."},
        {parameters, [?CTR_THR(200), ?FILE_NUM(20)]}
    ]},
    {config, [{name, large_files_number},
        {description, "Aggregates multiple events for large number of files."},
        {parameters, [?CTR_THR(500), ?FILE_NUM(50)]}
    ]}
]).
event_stream_different_file_id_aggregation_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId = ?config(session_id, Config),
    CtrThr = ?config(ctr_thr, Config),
    EvtNum = ?config(evt_num, Config),
    EvtSize = ?config(evt_size, Config),
    FileNum = ?config(file_num, Config),

    {ok, SubId} = subscribe(Worker,
        all,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(Meta) -> Meta >= CtrThr end,
        [fun(Evts) -> Self ! {handler, Evts} end]
    ),

    Evts = lists:map(fun(Id) ->
        #write_event{source = {session,<<"session_id">>}, file_uuid = ?FILE_ID(Id), size = EvtSize, counter = 1,
            file_size = EvtSize, blocks = [#file_block{
                offset = 0, size = EvtSize
            }]}
    end, lists:seq(1, FileNum)),

    % List of events that are supposed to be received multiple times as a result
    % of event handler execution.
    BatchSize = CtrThr div FileNum,
    EvtsToRecv = lists:sort(lists:map(fun(Evt) ->
        Evt#write_event{counter = BatchSize, size = BatchSize * EvtSize}
    end, Evts)),

    % Emit events for different files.
    {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
        lists:foreach(fun(_) ->
            lists:foreach(fun(Evt) ->
                emit(Worker, Evt, SessId)
            end, Evts)
        end, lists:seq(1, EvtNum))
    end),

    % Check whether events have been aggregated in terms of the same file ID
    % and handler has been executed.
    {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
        lists:foreach(fun(_) ->
            {ok, {handler, AggrEvts}} = test_utils:receive_any(?TIMEOUT),
            ?assertEqual(EvtsToRecv, lists:sort(AggrEvts))
        end, lists:seq(1, EvtNum div CtrThr))
    end),

    unsubscribe(Worker, SubId),
    remove_pending_messages(),

    [emit_time(EmitTime, EmitUnit), aggr_time(AggrTime, AggrUnit),
        evt_per_sec(FileNum * EvtNum, EmitUs + AggrUs)].

-performance([
    {repeats, 10},
    {parameters, [?CTR_THR(5), ?EVT_NUM(20)]},
    {description, "Check whether event stream executes handlers when events number "
    "exceeds counter threshold."},
    {config, [{name, small_counter_threshold},
        {description, "Executes event handler for stream with small counter threshold."},
        {parameters, [?CTR_THR(10), ?EVT_NUM(50000)]}
    ]},
    {config, [{name, medium_counter_threshold},
        {description, "Executes event handler for stream with medium counter threshold."},
        {parameters, [?CTR_THR(100), ?EVT_NUM(50000)]}
    ]},
    {config, [{name, large_counter_threshold},
        {description, "Executes event handler for stream with large counter threshold."},
        {parameters, [?CTR_THR(1000), ?EVT_NUM(50000)]}
    ]}
]).
event_stream_counter_emission_rule_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    FileId = <<"file_id">>,
    SessId = ?config(session_id, Config),
    CtrThr = ?config(ctr_thr, Config),
    EvtNum = ?config(evt_num, Config),

    {ok, SubId} = subscribe(Worker,
        all,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(Meta) -> Meta >= CtrThr end,
        [fun(_) -> Self ! handler end]
    ),

    % Emit events.
    {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
        lists:foreach(fun(_) ->
            emit(Worker, #write_event{
                file_uuid = FileId, counter = 1, size = 0, file_size = 0
            }, SessId)
        end, lists:seq(1, EvtNum))
    end),

    % Check whether events have been aggregated and handler has been executed
    % when emission rule has been satisfied.
    {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
        lists:foreach(fun(_) ->
            ?assertReceived(handler, ?TIMEOUT)
        end, lists:seq(1, EvtNum div CtrThr))
    end),

    unsubscribe(Worker, SubId),
    remove_pending_messages(),

    [emit_time(EmitTime, EmitUnit), aggr_time(AggrTime, AggrUnit),
        evt_per_sec(EvtNum, EmitUs + AggrUs)].

-performance([
    {repeats, 10},
    {parameters, [?SIZE_THR(100), ?EVT_NUM(20), ?EVT_SIZE(10)]},
    {description, "Check whether event stream executes handlers when summary events size "
    "exceeds size threshold."},
    {config, [{name, small_size_threshold},
        {description, "Executes event handler for stream with small size threshold."},
        {parameters, [?EVT_NUM(50000)]}
    ]},
    {config, [{name, medium_size_threshold},
        {description, "Executes event handler for stream with medium size threshold."},
        {parameters, [?SIZE_THR(1000), ?EVT_NUM(50000)]}
    ]},
    {config, [{name, large_size_threshold},
        {description, "Executes event handler for stream with large size threshold."},
        {parameters, [?SIZE_THR(10000), ?EVT_NUM(50000)]}
    ]}
]).
event_stream_size_emission_rule_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    FileId = <<"file_id">>,
    SessId = ?config(session_id, Config),
    SizeThr = ?config(size_thr, Config),
    EvtNum = ?config(evt_num, Config),
    EvtSize = ?config(evt_size, Config),

    {ok, SubId} = subscribe(Worker,
        all,
        infinity,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(Meta) -> Meta >= SizeThr end,
        fun(Meta, #write_event{size = Size}) -> Meta + Size end,
        [fun(_) -> Self ! handler end]
    ),

    % Emit events.
    {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
        lists:foreach(fun(_) ->
            emit(Worker, #write_event{
                counter = 1, file_uuid = FileId, size = EvtSize, file_size = 0
            }, SessId)
        end, lists:seq(1, EvtNum))
    end),

    % Check whether events have been aggregated and handler has been executed
    % when emission rule has been satisfied.
    {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
        lists:foreach(fun(_) ->
            ?assertReceived(handler, ?TIMEOUT)
        end, lists:seq(1, (EvtNum * EvtSize) div SizeThr))
    end),

    unsubscribe(Worker, SubId),
    remove_pending_messages(),

    [emit_time(EmitTime, EmitUnit), aggr_time(AggrTime, AggrUnit),
        evt_per_sec(EvtNum, EmitUs + AggrUs)].

%% Check whether event stream executes handlers when emission time expires.
event_stream_time_emission_rule_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Self = self(),
    EmTime = timer:seconds(2),
    EvtsCount = 100,

    {ok, SubId} = subscribe(Worker,
        gui,
        EmTime,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(_) -> false end,
        [fun(Evts) -> Self ! {handler, Evts} end]
    ),

    % Emit events.
    lists:foreach(fun(N) ->
        emit(Worker, #write_event{size = 1, counter = 1, file_size = N + 1,
            blocks = [#file_block{offset = N, size = 1}]}, SessId)
    end, lists:seq(0, EvtsCount - 1)),

    % Check whether event handlers have been executed.
    ?assertReceived({handler, [#write_event{
        size = EvtsCount, counter = EvtsCount, file_size = EvtsCount,
        blocks = [#file_block{offset = 0, size = EvtsCount}]}]}, ?TIMEOUT + EmTime),

    ?assertEqual({error, timeout}, test_utils:receive_any(EmTime)),

    unsubscribe(Worker, SubId),

    ok.

%% Check whether event stream is reinitialized in previous state in case of crash.
event_stream_crash_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Self = self(),
    EvtsCount = 100,
    HalfEvtsCount = round(EvtsCount / 2),

    {ok, SubId} = subscribe(Worker,
        gui,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(Meta) -> Meta >= EvtsCount end,
        [fun(Evts) -> Self ! {handler, Evts} end]
    ),

    % Emit first part of events.
    lists:foreach(fun(N) ->
        emit(Worker, #write_event{size = 1, counter = 1, file_size = N + 1,
            blocks = [#file_block{offset = N, size = 1}]}, SessId)
    end, lists:seq(0, HalfEvtsCount - 1)),

    % Get event stream pid.
    {ok, {SessSup, _}} = rpc:call(Worker, session,
        get_session_supervisor_and_node, [SessId]),
    {ok, EvtManSup} = get_child(SessSup, event_manager_sup),
    {ok, EvtStmSup} = get_child(EvtManSup, event_stream_sup),
    {ok, EvtStm} = get_child(EvtStmSup, undefined),

    % Send crash message and wait for event stream recovery.
    gen_server:cast(EvtStm, kill),
    timer:sleep(?TIMEOUT),

    % Emit second part of events.
    lists:foreach(fun(N) ->
        emit(Worker, #write_event{size = 1, counter = 1, file_size = N + 1,
            blocks = [#file_block{offset = N, size = 1}]}, SessId)
    end, lists:seq(HalfEvtsCount, EvtsCount - 1)),

    % Check whether event handlers have been executed.
    ?assertReceived({handler, [#write_event{
        size = EvtsCount, counter = EvtsCount, file_size = EvtsCount,
        blocks = [#file_block{offset = 0, size = EvtsCount}]}]}, ?TIMEOUT),
    ?assertEqual({error, timeout}, test_utils:receive_any()),

    unsubscribe(Worker, SubId),

    ok.

%% Check whether subscription can be created and cancelled.
event_manager_subscription_creation_and_cancellation_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId1 = <<"session_id_1">>,
    SessId2 = <<"session_id_2">>,
    Iden1 = #identity{user_id = <<"user_id_1">>},
    Iden2 = #identity{user_id = <<"user_id_2">>},

    session_setup(Worker1, SessId1, Iden1, Self),

    {ok, SubId} = subscribe(Worker2,
        all,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(Meta) -> Meta >= 6 end,
        [fun(Evts) -> Self ! {handler, Evts} end]
    ),

    session_setup(Worker2, SessId2, Iden2, Self),

    % Check whether subscription message has been sent to clients.
    ?assertMatch({ok, #write_event_subscription{}}, test_utils:receive_any(?TIMEOUT)),
    ?assertMatch({ok, #write_event_subscription{}}, test_utils:receive_any(?TIMEOUT)),

    %% FSLogic's subscription
    ?assertMatch({ok, #write_event_subscription{}}, test_utils:receive_any(?TIMEOUT)),
    ?assertMatch({ok, #write_event_subscription{}}, test_utils:receive_any(?TIMEOUT)),

    ?assertEqual({error, timeout}, test_utils:receive_any()),

    % Check subscription has been added to distributed cache.
    ?assertMatch({ok, [_, _]}, rpc:call(Worker1, subscription, list, [])),

    % Unsubscribe and check subscription cancellation message has been sent to
    % clients
    unsubscribe(Worker1, SubId),
    ?assertReceived(#event_subscription_cancellation{id = SubId}, ?TIMEOUT),
    ?assertReceived(#event_subscription_cancellation{id = SubId}, ?TIMEOUT),
    ?assertEqual({error, timeout}, test_utils:receive_any()),

    % Check subscription has been removed from distributed cache.
    ?assertMatch({ok, [_]}, rpc:call(Worker1, subscription, list, [])),

    session_teardown(Worker1, SessId2),
    session_teardown(Worker2, SessId1),

    ok.

-performance([
    {repeats, 10},
    {parameters, [?SUB_NUM(2), ?EVT_NUM(1000)]},
    {description, "Check whether multiple subscriptions are properly processed."},
    {config, [{name, small_subs_num},
        {description, "Creates subscriptions for events associated with small "
        "number of different files."},
        {parameters, [?SUB_NUM(10)]}
    ]},
    {config, [{name, medium_subs_num},
        {description, "Creates subscriptions for events associated with medium "
        "number of different files."},
        {parameters, [?SUB_NUM(20)]}
    ]},
    {config, [{name, large_subs_num},
        {description, "Creates subscriptions for events associated with large "
        "number of different files."},
        {parameters, [?SUB_NUM(50)]}
    ]}
]).
event_manager_multiple_subscription_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    Self = self(),
    SubsNum = ?config(sub_num, Config),
    EvtsNum = ?config(evt_num, Config),

    % Create subscriptions for events associated with different files.
    {SubIds, FileIds} = lists:unzip(lists:map(fun(N) ->
        FileId = <<"file_id_", (integer_to_binary(N))/binary>>,
        {ok, SubId} = subscribe(Worker,
            gui,
            fun(#write_event{file_uuid = Id}) -> Id =:= FileId; (_) -> false end,
            fun(Meta) -> Meta >= EvtsNum end,
            [fun(Evts) -> Self ! {handler, Evts} end]
        ),
        {SubId, FileId}
    end, lists:seq(1, SubsNum))),

    % Emit events.
    utils:pforeach(fun(FileId) ->
        lists:foreach(fun(N) ->
            emit(Worker, #write_event{file_uuid = FileId, size = 1, counter = 1,
                file_size = N + 1, blocks = [#file_block{offset = N, size = 1}]},
                SessId)
        end, lists:seq(0, EvtsNum - 1))
    end, FileIds),

    % Check whether event handlers have been executed.
    lists:foreach(fun(FileId) ->
        ?assertReceived({handler, [#write_event{
            file_uuid = FileId, size = EvtsNum, counter = EvtsNum,
            file_size = EvtsNum, blocks = [#file_block{
                offset = 0, size = EvtsNum
            }]
        }]}, ?TIMEOUT)
    end, FileIds),
    ?assertEqual({error, timeout}, test_utils:receive_any()),

    lists:foreach(fun(SubId) ->
        unsubscribe(Worker, SubId)
    end, SubIds),
    remove_pending_messages(),

    ok.

%% Check whether multiple handlers are executed in terms of one event stream.
event_manager_multiple_handlers_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    FileId = <<"file_id">>,
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},

    session_setup(Worker, SessId, Iden, Self),

    {ok, SubId} = subscribe(Worker,
        all,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(Meta) -> Meta >= 10 end,
        [
            fun(Evts) -> Self ! {handler1, Evts} end,
            fun(Evts) -> Self ! {handler2, Evts} end,
            fun(Evts) -> Self ! {handler3, Evts} end
        ]
    ),

    % Emit events.
    lists:foreach(fun(N) ->
        emit(Worker, #write_event{file_uuid = FileId, size = 1, counter = 1,
            file_size = N + 1, blocks = [#file_block{offset = N, size = 1}]}, SessId)
    end, lists:seq(0, 9)),

    % Check whether events have been aggregated and each handler has been executed.
    lists:foreach(fun(Handler) ->
        ?assertReceived({Handler, [#write_event{
            file_uuid = FileId, counter = 10, size = 10, file_size = 10,
            blocks = [#file_block{offset = 0, size = 10}]
        }]}, ?TIMEOUT)
    end, [handler1, handler2, handler3]),

    unsubscribe(Worker, SubId),
    session_teardown(Worker, SessId),

    ok.

-performance([
    {repeats, 10},
    {parameters, [?CLI_NUM(3), ?CTR_THR(5), ?EVT_NUM(1000)]},
    {description, "Check whether event stream executes handlers for multiple clients."},
    {config, [{name, small_client_number},
        {description, "Small number of clients connected to the server."},
        {parameters, [?CLI_NUM(10)]}
    ]},
    {config, [{name, medium_client_number},
        {description, "Medium number of clients connected to the server."},
        {parameters, [?CLI_NUM(100)]}
    ]},
    {config, [{name, large_client_number},
        {description, "Large number of clients connected to the server."},
        {parameters, [?CLI_NUM(500)]}
    ]}
]).
event_manager_multiple_clients_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    FileId = <<"file_id">>,
    CliNum = ?config(cli_num, Config),
    CtrThr = ?config(ctr_thr, Config),
    EvtNum = ?config(evt_num, Config),

    SessIds = lists:map(fun(N) ->
        SessId = <<"session_id_", (integer_to_binary(N))/binary>>,
        Iden = #identity{user_id = <<"user_id_", (integer_to_binary(N))/binary>>},
        session_setup(Worker, SessId, Iden, Self),
        SessId
    end, lists:seq(1, CliNum)),

    {ok, SubId} = subscribe(Worker,
        all,
        fun(#write_event{}) -> true; (_) -> false end,
        fun(Meta) -> Meta >= CtrThr end,
        [fun(_) -> Self ! handler end]
    ),

    % Emit events.
    {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
        utils:pforeach(fun(SessId) ->
            lists:foreach(fun(_) ->
                emit(Worker, #write_event{
                    file_uuid = FileId, counter = 1, size = 0, file_size = 0
                }, SessId)
            end, lists:seq(1, EvtNum))
        end, SessIds)
    end),

    % Check whether events have been aggregated and handler has been executed
    % when emission rule has been satisfied.
    {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
        lists:foreach(fun(_) ->
            lists:foreach(fun(_) ->
                ?assertReceived(handler, ?TIMEOUT)
            end, lists:seq(1, EvtNum div CtrThr))
        end, SessIds)
    end),

    unsubscribe(Worker, SubId),
    lists:foreach(fun(SessId) ->
        session_teardown(Worker, SessId)
    end, SessIds),
    remove_pending_messages(),

    [emit_time(EmitTime, EmitUnit), aggr_time(AggrTime, AggrUnit),
        evt_per_sec(CliNum * EvtNum, EmitUs + AggrUs)].

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(event_stream_crash_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},
    test_utils:mock_new(Worker, [communicator, logger]),
    test_utils:mock_expect(Worker, communicator, send, fun
        (_, _) -> ok
    end),
    test_utils:mock_expect(Worker, logger, dispatch_log, fun
        (_, _, _, [_, _, kill], _) -> meck:exception(throw, crash);
        (A, B, C, D, E) -> meck:passthrough([A, B, C, D, E])
    end),
    session_setup(Worker, SessId, Iden, Self),
    [{session_id, SessId} | Config];

init_per_testcase(Case, Config) when
    Case =:= event_manager_subscription_creation_and_cancellation_test;
    Case =:= event_manager_multiple_handlers_test;
    Case =:= event_manager_multiple_clients_test ->
    Self = self(),
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, communicator),
    test_utils:mock_expect(Workers, communicator, send, fun
        (#write_event_subscription{} = Msg, _) -> Self ! Msg, ok;
        (#event_subscription_cancellation{} = Msg, _) -> Self ! Msg, ok;
        (_, _) -> ok
    end),
    Config;

init_per_testcase(Case, Config) when
    Case =:= event_stream_counter_emission_rule_test;
    Case =:= event_stream_size_emission_rule_test;
    Case =:= event_stream_time_emission_rule_test;
    Case =:= event_manager_multiple_subscription_test;
    Case =:= event_stream_the_same_file_id_aggregation_test;
    Case =:= event_stream_different_file_id_aggregation_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},
    test_utils:mock_new(Worker, communicator),
    test_utils:mock_expect(Worker, communicator, send, fun
        (_, _) -> ok
    end),
    session_setup(Worker, SessId, Iden, Self),
    [{session_id, SessId} | Config].

end_per_testcase(event_stream_crash_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    remove_pending_messages(),
    session_teardown(Worker, SessId),
    test_utils:mock_validate(Worker, [communicator, logger]),
    test_utils:mock_unload(Worker, [communicator, logger]),
    proplists:delete(session_id, Config);

end_per_testcase(Case, Config) when
    Case =:= event_manager_subscription_creation_and_cancellation_test;
    Case =:= event_manager_multiple_handlers_test;
    Case =:= event_manager_multiple_clients_test ->
    Workers = ?config(op_worker_nodes, Config),
    remove_pending_messages(),
    test_utils:mock_validate(Workers, communicator),
    test_utils:mock_unload(Workers, communicator),
    Config;

end_per_testcase(Case, Config) when
    Case =:= event_stream_counter_emission_rule_test;
    Case =:= event_stream_size_emission_rule_test;
    Case =:= event_stream_time_emission_rule_test;
    Case =:= event_manager_multiple_subscription_test;
    Case =:= event_stream_the_same_file_id_aggregation_test;
    Case =:= event_stream_different_file_id_aggregation_test ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    remove_pending_messages(),
    session_teardown(Worker, SessId),
    test_utils:mock_validate(Worker, communicator),
    test_utils:mock_unload(Worker, communicator),
    proplists:delete(session_id, Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new test session.
%% @end
%%--------------------------------------------------------------------
-spec session_setup(Worker :: node(), SessId :: session:id(),
    Iden :: session:identity(), Con :: pid()) -> ok.
session_setup(Worker, SessId, Iden, Con) ->
    ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, Iden, Con])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Remove existing test session.
%% @end
%%--------------------------------------------------------------------
-spec session_teardown(Worker :: node(), SessId :: session:id()) -> ok.
session_teardown(Worker, SessId) ->
    ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Emits an event.
%% @end
%%--------------------------------------------------------------------
-spec emit(Worker :: node(), Evt :: event_manager:event(), SessId :: session:id()) ->
    ok.
emit(Worker, Evt, SessId) ->
    ?assertEqual(ok, rpc:call(Worker, event_manager, emit, [Evt, SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv subscribe(Worker, Producer, infinity, AdmRule, EmRule, Handlers)
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), Producer :: event_manager:producer(),
    AdmRule :: event_stream:admission_rule(),
    EmRule :: event_stream:emission_rule(),
    Handlers :: [event_stream:event_handler()]) ->
    {ok, SubId :: event_manager:subscription_id()}.
subscribe(Worker, Producer, AdmRule, EmRule, Handlers) ->
    subscribe(Worker, Producer, infinity, AdmRule, EmRule, Handlers).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv subscribe(Worker, Producer, infinity, AdmRule, EmRule, TrRule, Handlers)
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), Producer :: event_manager:producer(),
    EmTime :: timeout(), AdmRule :: event_stream:admission_rule(),
    EmRule :: event_stream:emission_rule(),
    Handlers :: [event_stream:event_handler()]) ->
    {ok, SubId :: event_manager:subscription_id()}.
subscribe(Worker, Producer, EmTime, AdmRule, EmRule, Handlers) ->
    TrRule = fun
        (Meta, #read_event{counter = Counter}) -> Meta + Counter;
        (Meta, #write_event{counter = Counter}) -> Meta + Counter
    end,
    subscribe(Worker, Producer, EmTime, AdmRule, EmRule, TrRule, Handlers).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates event subscription.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), Producer :: event_manager:producer(),
    EmTime :: timeout(), AdmRule :: event_stream:admission_rule(),
    EmRule :: event_stream:emission_rule(),
    TrRule :: event_stream:transition_rule(),
    Handlers :: [event_stream:event_handler()]) ->
    {ok, SubId :: event_manager:subscription_id()}.
subscribe(Worker, Producer, EmTime, AdmRule, EmRule, TrRule, Handlers) ->
    Sub = #write_event_subscription{
        producer = Producer,
        event_stream = ?WRITE_EVENT_STREAM#event_stream{
            metadata = 0,
            admission_rule = AdmRule,
            emission_rule = EmRule,
            transition_rule = TrRule,
            emission_time = EmTime,
            handlers = Handlers
        }
    },
    SubAnswer = rpc:call(Worker, event_manager, subscribe, [Sub]),
    ?assertMatch({ok, _}, SubAnswer),
    SubAnswer.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes event subscription.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(Worker :: node(), SubId :: event_manager:subscription_id()) ->
    ok.
unsubscribe(Worker, SubId) ->
    ?assertEqual(ok, rpc:call(Worker, event_manager, unsubscribe, [SubId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns supervisor child.
%% @end
%%--------------------------------------------------------------------
-spec get_child(Sup :: pid(), ChildId :: term()) ->
    {ok, Child :: pid()} | {error, not_found}.
get_child(Sup, ChildId) ->
    Children = supervisor:which_children(Sup),
    case lists:keyfind(ChildId, 1, Children) of
        {ChildId, Child, _, _} -> {ok, Child};
        false -> {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes messages for process messages queue.
%% @end
%%--------------------------------------------------------------------
-spec remove_pending_messages() -> ok.
remove_pending_messages() ->
    case test_utils:receive_any() of
        {error, timeout} -> ok;
        _ -> remove_pending_messages()
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns summary events emission time parameter.
%% @end
%%--------------------------------------------------------------------
-spec emit_time(Value :: integer() | float(), Unit :: string()) -> #parameter{}.
emit_time(Value, Unit) ->
    #parameter{name = emit_time, description = "Summary events emission time.",
        value = Value, unit = Unit}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns summary events aggregation time parameter.
%% @end
%%--------------------------------------------------------------------
-spec aggr_time(Value :: integer() | float(), Unit :: string()) -> #parameter{}.
aggr_time(Value, Unit) ->
    #parameter{name = aggr_time, description = "Summary events aggregation time.",
        value = Value, unit = Unit}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns number of events per second parameter.
%% @end
%%--------------------------------------------------------------------
-spec evt_per_sec(EvtNum :: integer(), Time :: integer()) -> #parameter{}.
evt_per_sec(EvtNum, Time) ->
    #parameter{name = evtps, unit = "event/s", description = "Number of events per second.",
        value = 1000000 * EvtNum / Time}.