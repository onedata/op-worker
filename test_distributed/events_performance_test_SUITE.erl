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
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    emit_should_aggregate_events_with_the_same_key/1,
    emit_should_not_aggregate_events_with_different_key/1,
    emit_should_execute_event_handler_when_counter_threshold_exceeded/1,
    subscribe_should_work_for_multiple_sessions/1]).

%% test_bases
-export([
    emit_should_aggregate_events_with_the_same_key_base/1,
    emit_should_not_aggregate_events_with_different_key_base/1,
    emit_should_execute_event_handler_when_counter_threshold_exceeded_base/1,
    subscribe_should_work_for_multiple_sessions_base/1]).

-define(TEST_CASES, [
    emit_should_aggregate_events_with_the_same_key,
    emit_should_not_aggregate_events_with_different_key,
    emit_should_execute_event_handler_when_counter_threshold_exceeded,
    subscribe_should_work_for_multiple_sessions
]).
all() ->
    ?ALL(?TEST_CASES, ?TEST_CASES).

-define(TIMEOUT, timer:minutes(1)).
-define(FILE_GUID(Id), fslogic_uuid:uuid_to_guid(<<"file_id_", (integer_to_binary(Id))/binary>>, <<"spaceid">>)).
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
    FileGuid = ?FILE_GUID(0),
    SessId = ?config(session_id, Config),
    CtrThr = ?config(ctr_thr, Config),
    EvtNum = ?config(evt_num, Config),
    EvtSize = ?config(evt_size, Config),
    FileSize = EvtNum * EvtSize,

    initializer:remove_pending_messages(),
    SubId = subscribe(Worker, SessId,
        fun(Meta) -> Meta >= CtrThr end,
        forward_events_handler(Self)
    ),
    ?assertReceivedMatch({stream_initialized, _, SessId}, ?TIMEOUT),

    % Emit events.
    {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
        lists:foreach(fun(N) ->
            emit(Worker, #file_written_event{file_uuid = FileGuid, size = EvtSize,
                file_size = N * EvtSize, blocks = [#file_block{
                    offset = (N - 1) * EvtSize, size = EvtSize
                }]}, SessId)
        end, lists:seq(1, EvtNum))
    end),

    flush(Worker, SubId, SessId),

    % Check whether events have been aggregated and handler has been executed.
    {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
        ?assertMatch([#file_written_event{
            counter = EvtNum,
            file_uuid = FileGuid,
            file_size = FileSize,
            size = FileSize,
            blocks = [#file_block{offset = 0, size = FileSize}]
        }], receive_file_written_events(SubId, SessId))
    end),

    unsubscribe(Worker, SessId, SubId),

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
    SubId = subscribe(Worker, SessId,
        fun(Meta) -> Meta >= CtrThr end,
        forward_events_handler(Self)
    ),
    ?assertReceivedMatch({stream_initialized, _, SessId}, ?TIMEOUT),

    % Emit events for different files.
    {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
        utils:pforeach(fun(N) ->
            lists:foreach(fun(M) ->
                emit(Worker, #file_written_event{file_uuid = ?FILE_GUID(N), size = EvtSize,
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
        RecvEvts = ?assertMatch([_ | _], receive_file_written_events(SubId, SessId)),
        lists:foldl(fun(Evt, FileUuids) ->
            #file_written_event{file_uuid = FileUuid} =
                ?assertMatch(#file_written_event{
                    counter = EvtNum,
                    file_size = FileSize,
                    size = FileSize,
                    blocks = [#file_block{offset = 0, size = FileSize}]

                }, Evt),
            ?assert(lists:member(FileUuid, FileUuids)),
            lists:delete(FileUuid, FileUuids)
        end, [?FILE_GUID(N) || N <- lists:seq(1, FileNum)], RecvEvts)
    end),

    unsubscribe(Worker, SessId, SubId),

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
    FileGuid = ?FILE_GUID(0),
    SessId = ?config(session_id, Config),
    EvtNum = ?config(evt_num, Config),

    initializer:remove_pending_messages(),
    SubId = subscribe(Worker, SessId,
        fun(Meta) -> Meta >= EvtNum end,
        forward_events_handler(Self)
    ),
    ?assertReceivedMatch({stream_initialized, _, SessId}, ?TIMEOUT),

    % Emit events.
    {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
        lists:foreach(fun(_) ->
            emit(Worker, #file_written_event{file_uuid = FileGuid}, SessId)
        end, lists:seq(1, EvtNum))
    end),

    % Check whether events have been aggregated and handler has been executed.
    {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
        ?assertReceivedMatch({event_handler, SubId, SessId, [
            #file_written_event{counter = EvtNum}
        ]}, ?TIMEOUT)
    end),

    unsubscribe(Worker, SessId, SubId),

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
    FileGuid = ?FILE_GUID(0),
    CliNum = ?config(cli_num, Config),
    CtrThr = ?config(ctr_thr, Config),
    EvtNum = ?config(evt_num, Config),
    EvtSize = ?config(evt_size, Config),
    FileSize = EvtNum * EvtSize,

    initializer:remove_pending_messages(),
    Sessions = lists:map(fun(N) ->
        SessId = <<"session_id_", (integer_to_binary(N))/binary>>,
        Iden = #user_identity{user_id = <<"user_id_", (integer_to_binary(N))/binary>>},
        session_setup(Worker, SessId, Iden, Self),
        SubId = subscribe(Worker, SessId,
            fun(Meta) -> Meta >= CtrThr end,
            forward_events_handler(Self)
        ),
        {SessId, SubId}
    end, lists:seq(1, CliNum)),

    lists:foreach(fun({SessId, _}) ->
        ?assertReceivedMatch({stream_initialized, _, SessId}, ?TIMEOUT)
    end, Sessions),

    % Emit events.
    {_, EmitUs, EmitTime, EmitUnit} = utils:duration(fun() ->
        utils:pforeach(fun({SessId, _}) ->
            lists:foreach(fun(N) ->
                emit(Worker, #file_written_event{file_uuid = FileGuid, size = EvtSize,
                    file_size = N * EvtSize, blocks = [#file_block{
                        offset = (N - 1) * EvtSize, size = EvtSize
                    }]}, SessId)
            end, lists:seq(1, EvtNum))
        end, Sessions)
    end),

    lists:foreach(fun({SessId, SubId}) ->
        flush(Worker, SubId, SessId)
    end, Sessions),

    % Check whether events have been aggregated and handler has been executed
    % for each session.
    {_, AggrUs, AggrTime, AggrUnit} = utils:duration(fun() ->
        lists:foreach(fun({SessId, SubId}) ->
            ?assertMatch([#file_written_event{
                counter = EvtNum,
                file_uuid = FileGuid,
                file_size = FileSize,
                size = FileSize,
                blocks = [#file_block{offset = 0, size = FileSize}]
            }], receive_file_written_events(SubId, SessId))
        end, Sessions)
    end),

    lists:foreach(fun({SessId, _}) ->
        session_teardown(Worker, SessId)
    end, Sessions),

    [emit_time(EmitTime, EmitUnit), aggr_time(AggrTime, AggrUnit),
        evt_per_sec(CliNum * EvtNum, EmitUs + AggrUs)].

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        [Worker | _] = ?config(op_worker_nodes, NewConfig),
        initializer:clear_models(Worker, [subscription]),
        NewConfig
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


init_per_testcase(subscribe_should_work_for_multiple_sessions, Config) ->
    Self = self(),
    Workers = ?config(op_worker_nodes, Config),
    initializer:remove_pending_messages(),
    test_utils:mock_new(Workers, communicator),
    test_utils:mock_expect(Workers, communicator, send, fun
        (#file_written_subscription{} = Msg, _) -> Self ! Msg, ok;
        (#subscription_cancellation{} = Msg, _) -> Self ! Msg, ok;
        (_, _) -> ok
    end),
    test_utils:mock_new(Workers, od_space),
    test_utils:mock_expect(Workers, od_space, get_or_fetch, fun(_, _, _) ->
        {ok, #document{value = #od_space{providers = [oneprovider:get_provider_id()]}}}
    end),
    initializer:mock_test_file_context(Config, <<"file_id">>),
    initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config);

init_per_testcase(_Case, Config) ->
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
    session_setup(Worker, SessId, Iden, Self),
    initializer:mock_test_file_context(Config, <<"file_id">>),
    initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"),
        [{session_id, SessId} | Config]).

end_per_testcase(subscribe_should_work_for_multiple_sessions, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unmock_test_file_context(Config),
    test_utils:mock_unload(Workers, od_space),
    test_utils:mock_validate_and_unload(Workers, [communicator]);

end_per_testcase(_Case, Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    session_teardown(Worker, SessId),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unmock_test_file_context(Config),
    test_utils:mock_unload(Workers, od_space),
    test_utils:mock_validate_and_unload(Worker, [communicator]).

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
-spec emit(Worker :: node(), Evt :: event:type(), SessId :: session:id()) -> ok.
emit(Worker, Evt, SessId) ->
    ?assertEqual(ok, rpc:call(Worker, event, emit, [Evt, SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv subscribe(Worker, Producer, infinity, AdmRule, EmRule, Handler)
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), SessId :: session:id(),
    EmRule :: event_stream:emission_rule(),
    Handler :: event_stream:event_handler()) -> {ok, SubId :: subscription:id()}.
subscribe(Worker, SessId, EmRule, Handler) ->
    Self = self(),
    Stm = event_stream_factory:create(#file_written_subscription{}),
    Sub = #subscription{
        type = #file_written_subscription{},
        stream = Stm#event_stream{
            init_handler = fun(_SubId, _SessId) ->
                Self ! {stream_initialized, file_written, _SessId},
                #{subscription_id => _SubId, session_id => _SessId}
            end,
            emission_rule = EmRule,
            transition_rule = fun(Meta, #file_written_event{counter = C}) ->
                Meta + C
            end,
            event_handler = Handler
        }
    },
    rpc:call(Worker, event, subscribe, [Sub, SessId]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes event subscription.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(Worker :: node(), SessId :: session:id(),
    SubId :: subscription:id()) -> ok.
unsubscribe(Worker, SessId, SubId) ->
    ?assertEqual(ok, rpc:call(Worker, event, unsubscribe, [SubId, SessId])),
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
-spec receive_file_written_events(SubId :: subscription:id(), SessId :: session:id()) ->
    [event:type()] | {error, timeout}.
receive_file_written_events(SubId, SessId) ->
    receive_file_written_events(SubId, SessId,
        fun event_utils:aggregate_file_written_events/2, #{}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Receives write events and aggregates them by file ID.
%% @end
%%--------------------------------------------------------------------
-spec receive_file_written_events(SubId :: subscription:id(), SessId :: session:id(),
    AggRule :: event_stream:aggregation_rule(), AggEvts :: #{}) ->
    [event:type()] | {error, timeout}.
receive_file_written_events(SubId, SessId, AggRule, AggEvts) ->
    receive
        {event_handler, SubId, SessId, Evts} ->
            AggEvts3 = lists:foldl(fun(#file_written_event{
                file_uuid = FileUuid
            } = Evt, AggEvts2) ->
                case maps:find(FileUuid, AggEvts2) of
                    {ok, AggEvt} ->
                        maps:put(FileUuid, AggRule(AggEvt, Evt), AggEvts2);
                    error -> maps:put(FileUuid, Evt, AggEvts2)
                end
            end, AggEvts, Evts),
            receive_file_written_events(SubId, SessId, AggRule, AggEvts3);
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
