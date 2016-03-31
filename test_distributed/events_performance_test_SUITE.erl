%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains event API performance tests.
%%% @end
%%%-------------------------------------------------------------------
-module(events_performance_test_SUITE).
-author("Krzysztof Trzepla").

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/events/definitions.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    emit_should_aggregate_events_with_the_same_key/1,
    emit_should_not_aggregate_events_with_different_key/1,
    emit_should_execute_event_handler_when_counter_threshold_exceeded/1,
    emit_should_execute_event_handler_when_size_threshold_exceeded/1,
    multiple_subscribe_should_create_multiple_subscriptions/1,
    subscribe_should_work_for_multiple_sessions/1]).

%% test_bases
-export([
    emit_should_aggregate_events_with_the_same_key_base/1,
    emit_should_not_aggregate_events_with_different_key_base/1,
    emit_should_execute_event_handler_when_counter_threshold_exceeded_base/1,
    emit_should_execute_event_handler_when_size_threshold_exceeded_base/1,
    multiple_subscribe_should_create_multiple_subscriptions_base/1,
    subscribe_should_work_for_multiple_sessions_base/1]).

-define(TEST_CASES, [
    emit_should_aggregate_events_with_the_same_key,
    emit_should_not_aggregate_events_with_different_key,
    emit_should_execute_event_handler_when_counter_threshold_exceeded,
    emit_should_execute_event_handler_when_size_threshold_exceeded,
    multiple_subscribe_should_create_multiple_subscriptions,
    subscribe_should_work_for_multiple_sessions
]).
all() ->
    ?ALL(?TEST_CASES, ?TEST_CASES).

-define(TIMEOUT, timer:seconds(15)).
-define(FILE_UUID(Id), <<"file_id_", (integer_to_binary(Id))/binary>>).
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

emit_should_aggregate_events_with_the_same_key(Config) ->
    ?PERFORMANCE(Config, [
            {repeats, 10},
            {success_rate, 90},
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
        ]
    ).
emit_should_aggregate_events_with_the_same_key_base(Config) ->
        [Worker | _] = ?config(op_worker_nodes, Config),
        Self = self(),
        FileUuid = <<"file_uuid">>,
        SessId = ?config(session_id, Config),
        CtrThr = ?config(ctr_thr, Config),
        EvtNum = ?config(evt_num, Config),
        EvtSize = ?config(evt_size, Config),

        initializer:remove_pending_messages(),
        {ok, SubId} = subscribe(Worker,
            fun(#event{object = #write_event{}}) -> true; (_) -> false end,
            fun(Meta) -> Meta >= CtrThr end,
            fun(Evts, _) -> Self ! {event_handler, Evts} end
        ),

        % Emit events.
        {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
            lists:foreach(fun(N) ->
                emit(Worker, #write_event{file_uuid = FileUuid, size = EvtSize,
                    file_size = N * EvtSize, blocks = [#file_block{
                        offset = (N - 1) * EvtSize, size = EvtSize
                    }]}, SessId)
            end, lists:seq(1, EvtNum))
        end),

        % Check whether events have been aggregated and handler has been executed.
        {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
            lists:foreach(fun(N) ->
                Size = CtrThr * EvtSize,
                Offset = (N - 1) * Size,
                FileSize = N * CtrThr * EvtSize,
                ?assertReceivedMatch({event_handler, [#event{
                    counter = CtrThr,
                    object = #write_event{
                        file_uuid = FileUuid, size = Size, file_size = FileSize,
                        blocks = [#file_block{offset = Offset, size = Size}]
                    }
                }]}, ?TIMEOUT)
            end, lists:seq(1, EvtNum div CtrThr))
        end),

        unsubscribe(Worker, SubId, {event_handler, []}),

        [emit_time(EmitTime, EmitUnit), aggr_time(AggrTime, AggrUnit),
            evt_per_sec(EvtNum, EmitUs + AggrUs)].

emit_should_not_aggregate_events_with_different_key(Config) ->
    ?PERFORMANCE(Config, [
            {repeats, 10},
            {success_rate, 90},
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
emit_should_not_aggregate_events_with_different_key_base(Config) ->
        [Worker | _] = ?config(op_worker_nodes, Config),
        Self = self(),
        SessId = ?config(session_id, Config),
        CtrThr = ?config(ctr_thr, Config),
        EvtNum = ?config(evt_num, Config),
        EvtSize = ?config(evt_size, Config),
        FileNum = ?config(file_num, Config),

        initializer:remove_pending_messages(),
        {ok, SubId} = subscribe(Worker,
            fun(#event{object = #write_event{}}) -> true; (_) -> false end,
            fun(Meta) -> Meta >= CtrThr end,
            fun(Evts, _) -> Self ! {event_handler, Evts} end
        ),

        Evts = lists:map(fun(Uuid) -> #event{object = #write_event{
            file_uuid = ?FILE_UUID(Uuid), size = EvtSize, file_size = EvtSize,
            blocks = [#file_block{offset = 0, size = EvtSize}]
        }} end, lists:seq(1, FileNum)),

        % List of events that are supposed to be received multiple times as a result
        % of event handler execution.
        BatchSize = CtrThr div FileNum,
        EvtsToRecv = lists:sort(lists:map(fun(#event{object = #write_event{
            file_uuid = FileUuid
        } = Type} = Evt) ->
            Evt#event{key = FileUuid, counter = BatchSize, object = Type#write_event{
                size = BatchSize * EvtSize
            }}
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
                {event_handler, AggrEvts} = ?assertReceivedMatch(
                    {event_handler, _}, ?TIMEOUT
                ),
                ?assertEqual(EvtsToRecv, lists:sort(AggrEvts))
            end, lists:seq(1, EvtNum div CtrThr))
        end),

        unsubscribe(Worker, SubId, {event_handler, []}),

        [emit_time(EmitTime, EmitUnit), aggr_time(AggrTime, AggrUnit),
            evt_per_sec(FileNum * EvtNum, EmitUs + AggrUs)].

emit_should_execute_event_handler_when_counter_threshold_exceeded(Config) ->
    ?PERFORMANCE(Config, [
            {repeats, 10},
            {success_rate, 90},
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
emit_should_execute_event_handler_when_counter_threshold_exceeded_base(Config) ->
        [Worker | _] = ?config(op_worker_nodes, Config),
        Self = self(),
        FileUuid = <<"file_id">>,
        SessId = ?config(session_id, Config),
        CtrThr = ?config(ctr_thr, Config),
        EvtNum = ?config(evt_num, Config),

        initializer:remove_pending_messages(),
        {ok, SubId} = subscribe(Worker,
            fun(#event{object = #write_event{}}) -> true; (_) -> false end,
            fun(Meta) -> Meta >= CtrThr end,
            fun(_, _) -> Self ! event_handler end
        ),

        % Emit events.
        {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
            lists:foreach(fun(_) ->
                emit(Worker, #write_event{
                    file_uuid = FileUuid, size = 0, file_size = 0
                }, SessId)
            end, lists:seq(1, EvtNum))
        end),

        % Check whether events have been aggregated and handler has been executed
        % when emission rule has been satisfied.
        {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
            lists:foreach(fun(_) ->
                ?assertReceivedEqual(event_handler, ?TIMEOUT)
            end, lists:seq(1, EvtNum div CtrThr))
        end),

        unsubscribe(Worker, SubId, event_handler),

        [emit_time(EmitTime, EmitUnit), aggr_time(AggrTime, AggrUnit),
            evt_per_sec(EvtNum, EmitUs + AggrUs)].

emit_should_execute_event_handler_when_size_threshold_exceeded(Config) ->
    ?PERFORMANCE(Config, [
            {repeats, 10},
            {success_rate, 90},
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
emit_should_execute_event_handler_when_size_threshold_exceeded_base(Config) ->
        [Worker | _] = ?config(op_worker_nodes, Config),
        Self = self(),
        FileUuid = <<"file_id">>,
        SessId = ?config(session_id, Config),
        SizeThr = ?config(size_thr, Config),
        EvtNum = ?config(evt_num, Config),
        EvtSize = ?config(evt_size, Config),

        initializer:remove_pending_messages(),
        {ok, SubId} = subscribe(Worker,
            infinity,
            fun(#event{object = #write_event{}}) -> true; (_) -> false end,
            fun(Meta) -> Meta >= SizeThr end,
            fun(Meta, #event{object = #write_event{size = Size}}) ->
                Meta + Size end,
            fun(_, _) -> Self ! event_handler end
        ),

        % Emit events.
        {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
            lists:foreach(fun(_) ->
                emit(Worker, #write_event{
                    file_uuid = FileUuid, size = EvtSize, file_size = 0
                }, SessId)
            end, lists:seq(1, EvtNum))
        end),

        % Check whether events have been aggregated and handler has been executed
        % when emission rule has been satisfied.
        {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
            lists:foreach(fun(_) ->
                ?assertReceivedEqual(event_handler, ?TIMEOUT)
            end, lists:seq(1, (EvtNum * EvtSize) div SizeThr))
        end),

        unsubscribe(Worker, SubId, event_handler),

        [emit_time(EmitTime, EmitUnit), aggr_time(AggrTime, AggrUnit),
            evt_per_sec(EvtNum, EmitUs + AggrUs)].

multiple_subscribe_should_create_multiple_subscriptions(Config) ->
    ?PERFORMANCE(Config, [
            {repeats, 10},
            {success_rate, 90},
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
multiple_subscribe_should_create_multiple_subscriptions_base(Config) ->
        [Worker | _] = ?config(op_worker_nodes, Config),
        SessId = ?config(session_id, Config),
        Self = self(),
        SubsNum = ?config(sub_num, Config),
        EvtsNum = ?config(evt_num, Config),

        initializer:remove_pending_messages(),
        % Create subscriptions for events associated with different files.
        {SubIds, FileUuids} = lists:unzip(lists:map(fun(N) ->
            FileUuid = <<"file_id_", (integer_to_binary(N))/binary>>,
            {ok, SubId} = subscribe(Worker,
                fun
                    (#event{object = #write_event{file_uuid = Uuid}}) ->
                        Uuid =:= FileUuid;
                    (_) -> false
                end,
                fun(Meta) -> Meta >= EvtsNum end,
                fun(Evts, _) -> Self ! {event_handler, Evts} end
            ),
            {SubId, FileUuid}
        end, lists:seq(1, SubsNum))),

        % Emit events.
        utils:pforeach(fun(FileUuid) ->
            lists:foreach(fun(N) ->
                emit(Worker, #write_event{
                    file_uuid = FileUuid, size = 1, file_size = N + 1,
                    blocks = [#file_block{offset = N, size = 1}]
                }, SessId)
            end, lists:seq(0, EvtsNum - 1))
        end, FileUuids),

        % Check whether event handlers have been executed.
        lists:foreach(fun(FileUuid) ->
            ?assertReceivedMatch({event_handler, [#event{
                counter = EvtsNum, object = #write_event{
                    file_uuid = FileUuid, size = EvtsNum, file_size = EvtsNum,
                    blocks = [#file_block{offset = 0, size = EvtsNum}]
                }
            }]}, ?TIMEOUT)
        end, FileUuids),

        lists:foreach(fun(SubId) ->
            unsubscribe(Worker, SubId, {event_handler, []})
        end, SubIds),

        ok.

subscribe_should_work_for_multiple_sessions(Config) ->
    ?PERFORMANCE(Config, [
            {repeats, 10},
            {success_rate, 90},
            {parameters, [?CLI_NUM(3), ?CTR_THR(5), ?EVT_NUM(1000)]},
            {description, "Check whether event stream executes handlers for multiple clients."},
            {config, [{name, small_client_number},
                {description, "Small number of clients connected to the server."},
                {parameters, [?CLI_NUM(10)]}
            ]},
            {config, [{name, medium_client_number},
                {description, "Medium number of clients connected to the server."},
                {parameters, [?CLI_NUM(100)]}
            ]}
            % TODO - tune mnesia not to be overloaded at Bamboo in such case (VFS-1575)
%%             {config, [{name, large_client_number},
%%                 {description, "Large number of clients connected to the server."},
%%                 {parameters, [?CLI_NUM(500)]}
%%             ]}
        ]).
subscribe_should_work_for_multiple_sessions_base(Config) ->
        [Worker | _] = ?config(op_worker_nodes, Config),
        Self = self(),
        FileUuid = <<"file_id">>,
        CliNum = ?config(cli_num, Config),
        CtrThr = ?config(ctr_thr, Config),
        EvtNum = ?config(evt_num, Config),

        initializer:remove_pending_messages(),
        SessIds = lists:map(fun(N) ->
            SessId = <<"session_id_", (integer_to_binary(N))/binary>>,
            Iden = #identity{user_id = <<"user_id_", (integer_to_binary(N))/binary>>},
            session_setup(Worker, SessId, Iden, Self),
            SessId
        end, lists:seq(1, CliNum)),

        {ok, SubId} = subscribe(Worker,
            fun(#event{object = #write_event{}}) -> true; (_) -> false end,
            fun(Meta) -> Meta >= CtrThr end,
            fun(_, _) -> Self ! event_handler end
        ),

        % Emit events.
        {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
            utils:pforeach(fun(SessId) ->
                lists:foreach(fun(_) ->
                    emit(Worker, #write_event{
                        file_uuid = FileUuid, size = 0, file_size = 0
                    }, SessId)
                end, lists:seq(1, EvtNum))
            end, SessIds)
        end),

        % Check whether events have been aggregated and handler has been executed
        % when emission rule has been satisfied.
        {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
            lists:foreach(fun(_) ->
                lists:foreach(fun(_) ->
                    ?assertReceivedEqual(event_handler, ?TIMEOUT)
                end, lists:seq(1, EvtNum div CtrThr))
            end, SessIds)
        end),

        unsubscribe(Worker, SubId, event_handler),
        lists:foreach(fun(SessId) ->
            session_teardown(Worker, SessId)
        end, SessIds),

        [emit_time(EmitTime, EmitUnit), aggr_time(AggrTime, AggrUnit),
            evt_per_sec(CliNum * EvtNum, EmitUs + AggrUs)].

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    NewConfig = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    [Worker | _] = ?config(op_worker_nodes, NewConfig),
    initializer:clear_models(Worker, [subscription]),
    NewConfig.

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(subscribe_should_work_for_multiple_sessions, Config) ->
    Self = self(),
    Workers = ?config(op_worker_nodes, Config),
    initializer:remove_pending_messages(),
    test_utils:mock_new(Workers, communicator),
    test_utils:mock_expect(Workers, communicator, send, fun
        (#write_subscription{} = Msg, _) -> Self ! Msg, ok;
        (#subscription_cancellation{} = Msg, _) -> Self ! Msg, ok;
        (_, _) -> ok
    end),
    ok = initializer:assume_all_files_in_space(Config, <<"spaceid">>),
    initializer:create_test_users_and_spaces(Config);

init_per_testcase(_, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},
    initializer:remove_pending_messages(),
    test_utils:mock_new(Worker, communicator),
    test_utils:mock_expect(Worker, communicator, send, fun
        (_, _) -> ok
    end),

    ok = initializer:assume_all_files_in_space(Config, <<"spaceid">>),
    session_setup(Worker, SessId, Iden, Self),
    initializer:create_test_users_and_spaces([{session_id, SessId} | Config]).

end_per_testcase(subscribe_should_work_for_multiple_sessions, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, communicator);

end_per_testcase(_, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    session_teardown(Worker, SessId),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Worker, communicator).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session document in datastore.
%% @end
%%--------------------------------------------------------------------
-spec session_setup(Worker :: node(), SessId :: session:id(),
    Iden :: session:identity(), Con :: pid()) -> ok.
session_setup(Worker, SessId, Iden, Con) ->
    ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
        reuse_or_create_fuse_session, [SessId, Iden, Con])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes session document from datastore.
%% @end
%%--------------------------------------------------------------------
-spec session_teardown(Worker :: node(), SessId :: session:id()) -> ok.
session_teardown(Worker, SessId) ->
    rpc:call(Worker, session_manager, remove_session, [SessId]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Emits an event.
%% @end
%%--------------------------------------------------------------------
-spec emit(Worker :: node(), Evt :: #event{} | event:object(),
    SessId :: session:id()) -> ok.
emit(Worker, Evt, SessId) ->
    ?assertEqual(ok, rpc:call(Worker, event, emit, [Evt, SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv subscribe(Worker, Producer, infinity, AdmRule, EmRule, Handler)
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), AdmRule :: event_stream:admission_rule(),
    EmRule :: event_stream:emission_rule(), Handler :: event_stream:event_handler()) ->
    {ok, SubId :: subscription:id()}.
subscribe(Worker, AdmRule, EmRule, Handler) ->
    subscribe(Worker, infinity, AdmRule, EmRule, Handler).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv subscribe(Worker, Producer, infinity, AdmRule, EmRule, TrRule, Handler)
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), EmTime :: event_stream:emission_time(),
    AdmRule :: event_stream:admission_rule(), EmRule :: event_stream:emission_rule(),
    Handler :: event_stream:event_handler()) -> {ok, SubId :: subscription:id()}.
subscribe(Worker, EmTime, AdmRule, EmRule, Handler) ->
    TrRule = fun(Meta, #event{counter = Counter}) -> Meta + Counter end,
    subscribe(Worker, EmTime, AdmRule, EmRule, TrRule, Handler).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates event subscription.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), EmTime :: event_stream:emission_time(),
    AdmRule :: event_stream:admission_rule(), EmRule :: event_stream:emission_rule(),
    TrRule :: event_stream:transition_rule(), Handler :: event_stream:event_handler()) ->
    {ok, SubId :: subscription:id()}.
subscribe(Worker, EmTime, AdmRule, EmRule, TrRule, Handler) ->
    Sub = #subscription{
        event_stream = ?WRITE_EVENT_STREAM#event_stream_definition{
            metadata = 0,
            admission_rule = AdmRule,
            emission_rule = EmRule,
            transition_rule = TrRule,
            emission_time = EmTime,
            event_handler = Handler
        }
    },
    ?assertMatch({ok, _}, rpc:call(Worker, event, subscribe, [Sub])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes event subscription.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(Worker :: node(), SubId :: subscription:id(), HandlerMsg :: term()) ->
    ok.
unsubscribe(Worker, SubId, HandlerMsg) ->
    ?assertEqual(ok, rpc:call(Worker, event, unsubscribe, [SubId])),
    ?assertReceivedMatch(HandlerMsg, ?TIMEOUT),
    ok.

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