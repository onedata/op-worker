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
-include("proto/common/credentials.hrl").
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

-define(TIMEOUT, timer:minutes(1)).
-define(FILE_UUID(Id), <<"file_id_", (integer_to_binary(Id))/binary>>).
-define(STM_ID(N), list_to_atom("stream_id_" ++ integer_to_list(N))).
-define(CTR_THR(Value), [
    {name, ctr_thr}, {value, Value}, {description, "Summary events counter threshold."}
]).
-define(SIZE_THR(Value), [
    {name, size_thr}, {value, Value}, {description, "Summary events size threshold."}
]).
-define(EVT_NUM(Value), [
    {name, evt_num}, {value, Value}, {description, "Number of emitted events per file."}
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
    FileSize = EvtNum * EvtSize,

    initializer:remove_pending_messages(),
    {ok, SubId} = subscribe(Worker,
        fun(Meta) -> Meta >= CtrThr end,
        forward_events_handler(Self)
    ),
    ?assertReceivedMatch({stream_initialized, _, SessId}, ?TIMEOUT),

    % Emit events.
    {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
        lists:foreach(fun(N) ->
            emit(Worker, #write_event{file_uuid = FileUuid, size = EvtSize,
                file_size = N * EvtSize, blocks = [#file_block{
                    offset = (N - 1) * EvtSize, size = EvtSize
                }]}, SessId)
        end, lists:seq(1, EvtNum))
    end),

    flush(Worker, SubId, SessId),

    % Check whether events have been aggregated and handler has been executed.
    {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
        ?assertMatch([#event{key = FileUuid, counter = EvtNum,
            object = #write_event{
                file_uuid = FileUuid,
                file_size = FileSize,
                size = FileSize,
                blocks = [#file_block{offset = 0, size = FileSize}]
            }
        }], receive_write_events(SubId, SessId))
    end),

    unsubscribe(Worker, SubId),

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
    FileSize = EvtNum * EvtSize,

    initializer:remove_pending_messages(),
    {ok, SubId} = subscribe(Worker,
        fun(Meta) -> Meta >= CtrThr end,
        forward_events_handler(Self)
    ),
    ?assertReceivedMatch({stream_initialized, _, SessId}, ?TIMEOUT),

    % Emit events for different files.
    {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
        utils:pforeach(fun(N) ->
            lists:foreach(fun(M) ->
                emit(Worker, #write_event{file_uuid = ?FILE_UUID(N), size = EvtSize,
                    file_size = M * EvtSize, blocks = [#file_block{
                        offset = (M - 1) * EvtSize, size = EvtSize
                    }]}, SessId)
            end, lists:seq(1, EvtNum))
        end, lists:seq(1, FileNum))
    end),

    flush(Worker, SubId, SessId),

    % Check whether events have been aggregated in terms of the same file ID
    % and handler has been executed.
    {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
        RecvEvts = ?assertMatch([_ | _], receive_write_events(SubId, SessId)),
        lists:foldl(fun(Evt, FileUuids) ->
            #event{object = #write_event{file_uuid = FileUuid}} =
                ?assertMatch(#event{counter = EvtNum,
                    object = #write_event{
                        file_size = FileSize,
                        size = FileSize,
                        blocks = [#file_block{offset = 0, size = FileSize}]
                    }
                }, Evt),
            ?assert(lists:member(FileUuid, FileUuids)),
            lists:delete(FileUuid, FileUuids)
        end, [?FILE_UUID(N) || N <- lists:seq(1, FileNum)], RecvEvts)
    end),

    unsubscribe(Worker, SubId),

    [emit_time(EmitTime, EmitUnit), aggr_time(AggrTime, AggrUnit),
        evt_per_sec(EvtNum, EmitUs + AggrUs)].

emit_should_execute_event_handler_when_counter_threshold_exceeded(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 90},
        {parameters, [?EVT_NUM(20)]},
        {description, "Check whether event stream executes handlers when events number "
        "exceeds counter threshold which is equal to the number of events."},
        {config, [{name, small_counter_threshold},
            {description, "Executes event handler for stream with small counter threshold."},
            {parameters, [?EVT_NUM(500)]}
        ]},
        {config, [{name, medium_counter_threshold},
            {description, "Executes event handler for stream with medium counter threshold."},
            {parameters, [?EVT_NUM(5000)]}
        ]},
        {config, [{name, large_counter_threshold},
            {description, "Executes event handler for stream with large counter threshold."},
            {parameters, [?EVT_NUM(50000)]}
        ]}
    ]).
emit_should_execute_event_handler_when_counter_threshold_exceeded_base(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    FileUuid = <<"file_id">>,
    SessId = ?config(session_id, Config),
    EvtNum = ?config(evt_num, Config),

    initializer:remove_pending_messages(),
    {ok, SubId} = subscribe(Worker,
        fun(Meta) -> Meta >= EvtNum end,
        forward_events_handler(Self)
    ),
    ?assertReceivedMatch({stream_initialized, _, SessId}, ?TIMEOUT),

    % Emit events.
    {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
        lists:foreach(fun(_) ->
            emit(Worker, #write_event{file_uuid = FileUuid}, SessId)
        end, lists:seq(1, EvtNum))
    end),

    % Check whether events have been aggregated and handler has been executed.
    {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
        ?assertReceivedMatch({event_handler, SubId, SessId, [#event{
            key = FileUuid, counter = EvtNum, object = #write_event{}
        }]}, ?TIMEOUT)
    end),

    unsubscribe(Worker, SubId),

    [emit_time(EmitTime, EmitUnit), aggr_time(AggrTime, AggrUnit),
        evt_per_sec(EvtNum, EmitUs + AggrUs)].

emit_should_execute_event_handler_when_size_threshold_exceeded(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 90},
        {parameters, [?EVT_NUM(100), ?EVT_SIZE(1)]},
        {description, "Check whether event stream executes handlers when summary events size "
        "exceeds size threshold which is equal to the number of events."},
        {config, [{name, small_size_threshold},
            {description, "Executes event handler for stream with small size threshold."},
            {parameters, [?EVT_NUM(500)]}
        ]},
        {config, [{name, medium_size_threshold},
            {description, "Executes event handler for stream with medium size threshold."},
            {parameters, [?EVT_NUM(5000)]}
        ]},
        {config, [{name, large_size_threshold},
            {description, "Executes event handler for stream with large size threshold."},
            {parameters, [?EVT_NUM(50000)]}
        ]}
    ]).
emit_should_execute_event_handler_when_size_threshold_exceeded_base(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    FileUuid = <<"file_id">>,
    SessId = ?config(session_id, Config),
    EvtNum = ?config(evt_num, Config),
    EvtSize = ?config(evt_size, Config),

    initializer:remove_pending_messages(),
    {ok, SubId} = subscribe(Worker,
        infinity,
        fun(Meta) -> Meta >= EvtNum end,
        fun(Meta, #event{object = #write_event{size = Size}}) ->
            Meta + Size
        end,
        forward_events_handler(Self)
    ),
    ?assertReceivedMatch({stream_initialized, _, SessId}, ?TIMEOUT),

    % Emit events.
    {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
        lists:foreach(fun(_) ->
            emit(Worker, #write_event{file_uuid = FileUuid, size = EvtSize}, SessId)
        end, lists:seq(1, EvtNum))
    end),

    % Check whether events have been aggregated and handler has been executed.
    {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
        ?assertReceivedMatch({event_handler, SubId, SessId, [#event{
            key = FileUuid, counter = EvtNum, object = #write_event{}
        }]}, ?TIMEOUT)
    end),

    unsubscribe(Worker, SubId),

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
    FileUuid = <<"file_id">>,
    SubNum = ?config(sub_num, Config),
    EvtNum = ?config(evt_num, Config),

    initializer:remove_pending_messages(),
    % Create subscriptions for events.
    SubIds = lists:map(fun(N) ->
        StmId = ?STM_ID(N),
        {ok, SubId} = subscribe(Worker,
            StmId,
            infinity,
            fun(Meta) -> Meta >= EvtNum end,
            fun(Meta, #event{counter = Counter}) -> Meta + Counter end,
            forward_events_handler(Self)
        ),
        ?assertReceivedMatch({stream_initialized, StmId, SessId}, ?TIMEOUT),
        SubId
    end, lists:seq(1, SubNum)),

    % Emit events for different subscriptions.
    {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
        utils:pforeach(fun(N) ->
            lists:foreach(fun(M) ->
                emit(Worker, #event{stream_id = ?STM_ID(N), object = #write_event{
                    file_uuid = FileUuid, size = 1, file_size = M,
                    blocks = [#file_block{offset = M - 1, size = 1}]
                }}, SessId)
            end, lists:seq(1, EvtNum))
        end, lists:seq(1, SubNum))
    end),

    lists:foreach(fun(SubId) ->
        flush(Worker, SubId, SessId)
    end, SubIds),

    % Check whether events have been aggregated and handler has been executed
    % for each subscription.
    {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
        lists:foreach(fun(SubId) ->
            ?assertMatch([#event{counter = EvtNum,
                object = #write_event{
                    file_uuid = FileUuid,
                    file_size = EvtNum,
                    size = EvtNum,
                    blocks = [#file_block{offset = 0, size = EvtNum}]
                }
            }], receive_write_events(SubId, SessId))
        end, SubIds)
    end),

    lists:foreach(fun(SubId) ->
        unsubscribe(Worker, SubId)
    end, SubIds),

    [emit_time(EmitTime, EmitUnit), aggr_time(AggrTime, AggrUnit),
        evt_per_sec(EvtNum, EmitUs + AggrUs)].

subscribe_should_work_for_multiple_sessions(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 90},
        {parameters, [?CLI_NUM(3), ?CTR_THR(5), ?EVT_NUM(1000), ?EVT_SIZE(1)]},
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
subscribe_should_work_for_multiple_sessions_base(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    FileUuid = <<"file_id">>,
    CliNum = ?config(cli_num, Config),
    CtrThr = ?config(ctr_thr, Config),
    EvtNum = ?config(evt_num, Config),
    EvtSize = ?config(evt_size, Config),
    FileSize = EvtNum * EvtSize,

    initializer:remove_pending_messages(),
    SessIds = lists:map(fun(N) ->
        SessId = <<"session_id_", (integer_to_binary(N))/binary>>,
        Iden = #user_identity{user_id = <<"user_id_", (integer_to_binary(N))/binary>>},
        session_setup(Worker, SessId, Iden, Self),
        SessId
    end, lists:seq(1, CliNum)),

    {ok, SubId} = subscribe(Worker,
        fun(Meta) -> Meta >= CtrThr end,
        forward_events_handler(Self)
    ),

    lists:foreach(fun(SessId) ->
        ?assertReceivedMatch({stream_initialized, _, SessId}, ?TIMEOUT)
    end, SessIds),

    % Emit events.
    {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
        utils:pforeach(fun(SessId) ->
            lists:foreach(fun(N) ->
                emit(Worker, #write_event{file_uuid = FileUuid, size = EvtSize,
                    file_size = N * EvtSize, blocks = [#file_block{
                        offset = (N - 1) * EvtSize, size = EvtSize
                    }]}, SessId)
            end, lists:seq(1, EvtNum))
        end, SessIds)
    end),

    lists:foreach(fun(SessId) ->
        flush(Worker, SubId, SessId)
    end, SessIds),

    % Check whether events have been aggregated and handler has been executed
    % for each session.
    {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
        lists:foreach(fun(SessId) ->
            ?assertMatch([#event{key = FileUuid, counter = EvtNum,
                object = #write_event{
                    file_uuid = FileUuid,
                    file_size = FileSize,
                    size = FileSize,
                    blocks = [#file_block{offset = 0, size = FileSize}]
                }
            }], receive_write_events(SubId, SessId))
        end, SessIds)
    end),

    unsubscribe(Worker, SubId),
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
    ?TEST_STOP(Config).

init_per_testcase(subscribe_should_work_for_multiple_sessions = Case, Config) ->
    ?CASE_START(Case),
    Self = self(),
    Workers = ?config(op_worker_nodes, Config),
    initializer:remove_pending_messages(),
    test_utils:mock_new(Workers, communicator),
    test_utils:mock_expect(Workers, communicator, send, fun
        (#write_subscription{} = Msg, _) -> Self ! Msg, ok;
        (#subscription_cancellation{} = Msg, _) -> Self ! Msg, ok;
        (_, _) -> ok
    end),
    test_utils:mock_new(Workers, od_space),
    test_utils:mock_expect(Workers, od_space, get_or_fetch, fun(_, _, _) ->
        {ok, #document{value = #od_space{providers = [oneprovider:get_provider_id()]}}}
    end),
    ok = initializer:assume_all_files_in_space(Config, <<"spaceid">>),
    test_utils:mock_expect(Workers, fslogic_spaces, get_space_id,
        fun(_) -> <<"spaceid">> end),
    NewConfig = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    test_utils:mock_expect(Workers, file_meta, get, fun
        (<<"file_", _/binary>>) -> {ok, #document{}};
        (Entry) -> meck:passthrough([Entry])
    end),
    NewConfig;

init_per_testcase(Case, Config) ->
    ?CASE_START(Case),
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId = <<"session_id">>,
    Iden = #user_identity{user_id = <<"user_id">>},
    initializer:remove_pending_messages(),
    test_utils:mock_new(Worker, communicator),
    test_utils:mock_expect(Worker, communicator, send, fun
        (_, _) -> ok
    end),
    test_utils:mock_new(Workers, od_space),
    test_utils:mock_expect(Workers, od_space, get_or_fetch, fun(_, _, _) ->
        {ok, #document{value = #od_space{providers = [oneprovider:get_provider_id()]}}}
    end),
    ok = initializer:assume_all_files_in_space(Config, <<"spaceid">>),
    test_utils:mock_expect(Workers, fslogic_spaces, get_space_id,
        fun(_) -> <<"spaceid">> end),
    session_setup(Worker, SessId, Iden, Self),
    NewConfig = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), [{session_id, SessId} | Config]),
    test_utils:mock_expect(Workers, file_meta, get, fun
        (<<"file_", _/binary>>) -> {ok, #document{}};
        (Entry) -> meck:passthrough([Entry])
    end),
    NewConfig.

end_per_testcase(subscribe_should_work_for_multiple_sessions = Case, Config) ->
    ?CASE_STOP(Case),
    Workers = ?config(op_worker_nodes, Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_unload(Workers, od_space),
    test_utils:mock_validate_and_unload(Workers, communicator);

end_per_testcase(Case, Config) ->
    ?CASE_STOP(Case),
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    session_teardown(Worker, SessId),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_unload(Workers, od_space),
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
    ?assertMatch({ok, _}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, fuse, Iden, #token_auth{}, [Con]])).

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
-spec subscribe(Worker :: node(), EmRule :: event_stream:emission_rule(),
    Handler :: event_stream:event_handler()) -> {ok, SubId :: subscription:id()}.
subscribe(Worker, EmRule, Handler) ->
    subscribe(Worker, infinity, EmRule, Handler).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv subscribe(Worker, Producer, infinity, AdmRule, EmRule, TrRule, Handler)
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), EmTime :: event_stream:emission_time(),
    EmRule :: event_stream:emission_rule(), Handler :: event_stream:event_handler()) ->
    {ok, SubId :: subscription:id()}.
subscribe(Worker, EmTime, EmRule, Handler) ->
    TrRule = fun(Meta, #event{counter = Counter}) -> Meta + Counter end,
    subscribe(Worker, EmTime, EmRule, TrRule, Handler).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates event subscription.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), EmTime :: event_stream:emission_time(),
    EmRule :: event_stream:emission_rule(), TrRule :: event_stream:transition_rule(),
    Handler :: event_stream:event_handler()) -> {ok, SubId :: subscription:id()}.
subscribe(Worker, EmTime, EmRule, TrRule, Handler) ->
    subscribe(Worker, write_event_stream, EmTime, EmRule, TrRule, Handler).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates event subscription.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), StmId :: event_stream:id(),
    EmTime :: event_stream:emission_time(), EmRule :: event_stream:emission_rule(),
    TrRule :: event_stream:transition_rule(), Handler :: event_stream:event_handler()) ->
    {ok, SubId :: subscription:id()}.
subscribe(Worker, StmId, EmTime, EmRule, TrRule, Handler) ->
    Self = self(),
    Sub = #subscription{
        event_stream = ?WRITE_EVENT_STREAM#event_stream_definition{
            init_handler = fun(#subscription{id = SubId}, SessId, _) ->
                Self ! {stream_initialized, StmId, SessId},
                #{subscription_id => SubId, session_id => SessId}
            end,
            id = StmId,
            metadata = 0,
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
-spec unsubscribe(Worker :: node(), SubId :: subscription:id()) -> ok.
unsubscribe(Worker, SubId) ->
    ?assertEqual(ok, rpc:call(Worker, event, unsubscribe, [SubId])),
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Receives write events and aggregates them by file ID.
%% @end
%%--------------------------------------------------------------------
-spec receive_write_events(SubId :: subscription:id(), SessId :: session:id()) ->
    [#event{}] | {error, timeout}.
receive_write_events(SubId, SessId) ->
    receive_write_events(SubId, SessId,
        ?WRITE_EVENT_STREAM#event_stream_definition.aggregation_rule, #{}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Receives write events and aggregates them by file ID.
%% @end
%%--------------------------------------------------------------------
-spec receive_write_events(SubId :: subscription:id(), SessId :: session:id(),
    AggRule :: event_stream:aggregation_rule(), AggEvts :: #{}) ->
    [#event{}] | {error, timeout}.
receive_write_events(SubId, SessId, AggRule, AggEvts) ->
    receive
        {event_handler, SubId, SessId, Evts} ->
            AggEvts3 = lists:foldl(fun(#event{object = #write_event{
                file_uuid = FileUuid
            }} = Evt, AggEvts2) ->
                case maps:find(FileUuid, AggEvts2) of
                    {ok, AggEvt} ->
                        maps:put(FileUuid, AggRule(AggEvt, Evt), AggEvts2);
                    error -> maps:put(FileUuid, Evt, AggEvts2)
                end
            end, AggEvts, Evts),
            receive_write_events(SubId, SessId, AggRule, AggEvts3);
        {stream_flushed, SubId, SessId} ->
            maps:values(AggEvts)
    after
        ?TIMEOUT -> {error, timeout}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Flushes event stream.
%% @end
%%--------------------------------------------------------------------
-spec flush(Worker :: node(), SubId :: subscription:id(), SessId :: session:id()) -> ok.
flush(Worker, SubId, SessId) ->
    ProviderId = rpc:call(Worker, oneprovider, get_provider_id, []),
    rpc:call(Worker, event, flush, [#flush_events{
        provider_id = ProviderId,
        subscription_id = SubId
    }, SessId]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns event handler that forwards events to the provided process and
%% notifies about a flush action.
%% @end
%%--------------------------------------------------------------------
-spec forward_events_handler(Pid :: pid()) -> Handler :: event_stream:event_handler().
forward_events_handler(Pid) ->
    fun(Evts, #{session_id := SessId, subscription_id := SubId} = Ctx) ->
        Pid ! {event_handler, SubId, SessId, Evts},
        case Ctx of
            #{notify := _} -> Pid ! {stream_flushed, SubId, SessId};
            _ -> ok
        end
    end.
