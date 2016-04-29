%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains event API tests.
%%% @end
%%%-------------------------------------------------------------------
-module(events_test_SUITE).
-author("Krzysztof Trzepla").

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/events/definitions.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/handshake_messages.hrl").
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
    subscribe_should_create_subscription/1,
    unsubscribe_should_remove_subscription/1,
    subscribe_should_notify_event_manager/1,
    subscribe_should_notify_all_event_managers/1,
    emit_read_event_should_execute_handler/1,
    emit_write_event_should_execute_handler/1,
    emit_file_attr_update_event_should_execute_handler/1,
    emit_file_location_update_event_should_execute_handler/1,
    flush_should_notify_awaiting_process/1
]).

all() ->
    ?ALL([
        subscribe_should_create_subscription,
        unsubscribe_should_remove_subscription,
        subscribe_should_notify_event_manager,
        subscribe_should_notify_all_event_managers,
        emit_read_event_should_execute_handler,
        emit_write_event_should_execute_handler,
        emit_file_attr_update_event_should_execute_handler,
        emit_file_location_update_event_should_execute_handler,
        flush_should_notify_awaiting_process
    ]).

-define(TIMEOUT, timer:seconds(15)).

%%%===================================================================
%%% Test functions
%%%===================================================================

subscribe_should_create_subscription(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, SubId} = subscribe(Worker),
    ?assertMatch({ok, [_]}, rpc:call(Worker, subscription, list, [])),
    unsubscribe(Worker, SubId).

unsubscribe_should_remove_subscription(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, SubId} = subscribe(Worker),
    unsubscribe(Worker, SubId),
    ?assertMatch({ok, []}, rpc:call(Worker, subscription, list, [])).

subscribe_should_notify_event_manager(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Evt = read_event(1, [{0, 1}]),
    emit(Worker, ?config(session_id, Config), Evt),
    ?assertReceivedMatch({event_handler, [#event{
        key = <<"file_uuid">>, object = Evt
    }]}, ?TIMEOUT).

subscribe_should_notify_all_event_managers(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Evt = read_event(1, [{0, 1}]),
    emit(Worker, Evt),
    lists:foreach(fun(_) ->
        ?assertReceivedMatch({event_handler, [#event{
            key = <<"file_uuid">>, object = Evt
        }]}, ?TIMEOUT)
    end, ?config(session_ids, Config)).

emit_read_event_should_execute_handler(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Evt = read_event(1, [{0, 1}]),
    emit(Worker, ?config(session_id, Config), Evt),
    ?assertReceivedMatch({event_handler, [#event{
        key = <<"file_uuid">>, object = Evt
    }]}, ?TIMEOUT).

emit_write_event_should_execute_handler(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Evt = write_event(1, 1, [{0, 1}]),
    emit(Worker, ?config(session_id, Config), Evt),
    ?assertReceivedMatch({event_handler, [#event{
        key = <<"file_uuid">>, object = Evt
    }]}, ?TIMEOUT).

emit_file_attr_update_event_should_execute_handler(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Evt = file_attr_update_event(),
    emit(Worker, ?config(session_id, Config), Evt),
    ?assertReceivedMatch({event_handler, [#event{
        key = <<"file_uuid">>, object = Evt
    }]}, ?TIMEOUT).

emit_file_location_update_event_should_execute_handler(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Evt = file_location_update_event(),
    emit(Worker, ?config(session_id, Config), Evt),
    ?assertReceivedMatch({event_handler, [#event{
        key = <<"file_uuid">>, object = Evt
    }]}, ?TIMEOUT).

flush_should_notify_awaiting_process(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Evt = read_event(1, [{0, 1}]),
    SessId = ?config(session_id, Config),
    emit(Worker, SessId, Evt),
    flush(Worker, ?config(subscription_id, Config), self(), SessId),
    ?assertReceivedEqual(event_handler, ?TIMEOUT).

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

init_per_testcase(Case, Config) when
    Case =:= emit_read_event_should_execute_handler;
    Case =:= emit_write_event_should_execute_handler;
    Case =:= emit_file_attr_update_event_should_execute_handler;
    Case =:= emit_file_location_update_event_should_execute_handler ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, SubId} = create_dafault_subscription(Case, Worker),
    init_per_testcase(default, [{subscription_id, SubId} | Config]);

init_per_testcase(Case, Config) when
    Case =:= flush_should_notify_awaiting_process ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    NewConfig = init_per_testcase(default, Config),
    SessId = ?config(session_id, NewConfig),
    {ok, SubId} = subscribe(Worker, SessId, ?READ_EVENT_STREAM,
        notify_event_handler(), fun(_) -> false end, infinity),
    [{subscription_id, SubId} | NewConfig];

init_per_testcase(Case, Config) when
    Case =:= subscribe_should_notify_event_manager ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    NewConfig = init_per_testcase(default, Config),
    SessId = ?config(session_id, NewConfig),
    {ok, SubId} = subscribe(Worker, SessId, ?READ_EVENT_STREAM,
        forward_events_event_handler(), fun(_) -> true end, infinity),
    [{subscription_id, SubId} | NewConfig];

init_per_testcase(Case, Config) when
    Case =:= subscribe_should_notify_all_event_managers ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Worker),
    SessIds = lists:map(fun(N) ->
        SessId = <<"session_id_", (integer_to_binary(N))/binary>>,
        session_setup(Worker, SessId),
        SessId
    end, lists:seq(0, 4)),
    {ok, SubId} = create_dafault_subscription(Case, Worker),
    ok = initializer:assume_all_files_in_space(Config, <<"spaceid">>),
    initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), [{session_ids, SessIds}, {subscription_id, SubId} | Config]);

init_per_testcase(_, Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Worker),
    {ok, SessId} = session_setup(Worker),
    ok = initializer:assume_all_files_in_space(Config, <<"spaceid">>),
    test_utils:mock_new(Workers, space_info),
    test_utils:mock_expect(Workers, space_info, get_or_fetch, fun(_, _, _) ->
        {ok, #document{value = #space_info{providers = [oneprovider:get_provider_id()]}}}
    end),
    initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), [{session_id, SessId} | Config]).

end_per_testcase(Case, Config) when
    Case =:= emit_read_event_should_execute_handler;
    Case =:= emit_write_event_should_execute_handler;
    Case =:= emit_file_attr_update_event_should_execute_handler;
    Case =:= emit_file_location_update_event_should_execute_handler ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    unsubscribe(Worker, ?config(subscription_id, Config)),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config) when
    Case =:= flush_should_notify_awaiting_process;
    Case =:= subscribe_should_notify_event_manager ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    unsubscribe(Worker, ?config(session_id, Config), ?config(subscription_id, Config)),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config) when
    Case =:= subscribe_should_notify_all_event_managers ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    unsubscribe(Worker, ?config(subscription_id, Config)),
    lists:foreach(fun(SessId) ->
        session_teardown(Worker, SessId)
    end, ?config(session_ids, Config)),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:clear_assume_all_files_in_space(Config),
    test_utils:mock_validate_and_unload(Worker, [communicator]);

end_per_testcase(_, Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    session_teardown(Worker, ?config(session_id, Config)),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:clear_assume_all_files_in_space(Config),
    test_utils:mock_unload(Workers, space_info),
    test_utils:mock_validate_and_unload(Worker, [communicator]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv session_setup(Worker, <<"session_id">>
%% @end
%%--------------------------------------------------------------------
-spec session_setup(Worker :: node()) -> {ok, SessId :: session:id()}.
session_setup(Worker) ->
    session_setup(Worker, <<"session_id">>).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session document in datastore.
%% @end
%%--------------------------------------------------------------------
-spec session_setup(Worker :: node(), SessId :: session:id()) ->
    {ok, SessId :: session:id()}.
session_setup(Worker, SessId) ->
    Self = self(),
    Iden = #identity{user_id = <<"user1">>},
    ?assertMatch({ok, _}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, fuse, Iden, #auth{}, [Self]]
    )),
    {ok, SessId}.

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
%% Creates subscription for read events with event handler ignoring events.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node()) -> {ok, SubId :: subscription:id()}.
subscribe(Worker) ->
    subscribe(Worker, ?READ_EVENT_STREAM, fun(_, _) -> ok end,
        fun(_) -> true end, infinity).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates subscription with custom event stream definition.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), StmDef :: event_stream:definition(),
    Handler :: event_stream:event_handler(), EmRule :: event_stream:emission_rule(),
    EmTime :: event_stream:emission_time()) -> {ok, SubId :: subscription:id()}.
subscribe(Worker, StmDef, Handler, EmRule, EmTime) ->
    ?assertMatch({ok, _}, rpc:call(Worker, event, subscribe, [#subscription{
        event_stream = StmDef#event_stream_definition{
            emission_rule = EmRule,
            emission_time = EmTime,
            event_handler = Handler
        }
    }])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates subscription with custom event stream definition associated with session.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), SessId :: session:id(),
    StmDef :: event_stream:definition(), Handler :: event_stream:event_handler(),
    EmRule :: event_stream:emission_rule(), EmTime :: event_stream:emission_time()) ->
    {ok, SubId :: subscription:id()}.
subscribe(Worker, SessId, StmDef, Handler, EmRule, EmTime) ->
    ?assertMatch({ok, _}, rpc:call(Worker, event, subscribe, [#subscription{
        event_stream = StmDef#event_stream_definition{
            emission_rule = EmRule,
            emission_time = EmTime,
            event_handler = Handler
        }
    }, SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates default subscription based on testcase name.
%% @end
%%--------------------------------------------------------------------
-spec create_dafault_subscription(Case :: atom(), Worker :: node()) ->
    {ok, SubId :: subscription:id()}.
create_dafault_subscription(Case, Worker) ->
    subscribe(
        Worker,
        event_stream_type_for_testcase_name(Case),
        forward_events_event_handler(),
        fun(_) -> true end,
        infinity
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns event handler that forwards all events to this process.
%% @end
%%--------------------------------------------------------------------
-spec forward_events_event_handler() -> Handler :: event_stream:event_handler().
forward_events_event_handler() ->
    Self = self(),
    fun(Evts, _) -> Self ! {event_handler, Evts} end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns event handler that notifies process found in context.
%% @end
%%--------------------------------------------------------------------
-spec notify_event_handler() -> Handler :: event_stream:event_handler().
notify_event_handler() ->
    fun
        (_, #{notify := Notify}) -> Notify ! event_handler;
        (_, _) -> ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes subscription for events.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(Worker :: node(), SubId :: subscription:id()) -> ok.
unsubscribe(Worker, SubId) ->
    ?assertEqual(ok, rpc:call(Worker, event, unsubscribe, [SubId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes subscription for events associated with session.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(Worker :: node(), SessId :: session:id(),
    SubId :: subscription:id()) -> ok.
unsubscribe(Worker, SessId, SubId) ->
    ?assertEqual(ok, rpc:call(Worker, event, unsubscribe, [SubId, SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Emits an event to all event managers.
%% @end
%%--------------------------------------------------------------------
-spec emit(Worker :: node(), Evt :: event:object()) -> ok.
emit(Worker, Evt) ->
    ?assertEqual(ok, rpc:call(Worker, event, emit, [Evt])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Emits an event to event manager associated with a session.
%% @end
%%--------------------------------------------------------------------
-spec emit(Worker :: node(), SessId :: session:id(), Evt :: event:object()) -> ok.
emit(Worker, SessId, Evt) ->
    ?assertEqual(ok, rpc:call(Worker, event, emit, [Evt, SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Flushes event stream associated with session and subscription.
%% @end
%%--------------------------------------------------------------------
-spec flush(Worker :: node(), SubId :: subscription:id(), Notify :: pid(),
    SessId :: session:id()) -> ok.
flush(Worker, SubId, Notify, SessId) ->
    ?assertEqual(ok, rpc:call(Worker, event, flush, [SubId, Notify, SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv read_event(<<"file_uuid">>, Size, Blocks)
%% @end
%%--------------------------------------------------------------------
-spec read_event(Size :: file_meta:size(), Blocks :: proplists:proplist()) ->
    Evt :: #read_event{}.
read_event(Size, Blocks) ->
    read_event(<<"file_uuid">>, Size, Blocks).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns read event with custom file UUID, size and blocks.
%% @end
%%--------------------------------------------------------------------
-spec read_event(FileUuid :: file_meta:uuid(), Size :: file_meta:size(),
    Blocks :: proplists:proplist()) -> Evt :: #read_event{}.
read_event(FileUuid, Size, Blocks) ->
    #read_event{
        file_uuid = FileUuid,
        size = Size,
        blocks = [#file_block{offset = O, size = S} || {O, S} <- Blocks]
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv write_event(<<"file_size">>, Size, FileSize, Blocks)
%% @end
%%--------------------------------------------------------------------
-spec write_event(Size :: file_meta:size(), FileSize :: file_meta:size(),
    Blocks :: proplists:proplist()) -> Evt :: #write_event{}.
write_event(Size, FileSize, Blocks) ->
    write_event(<<"file_uuid">>, Size, FileSize, Blocks).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns write event with custom file UUID, size, file size and blocks.
%% @end
%%--------------------------------------------------------------------
-spec write_event(FileUuid :: file_meta:uuid(), Size :: file_meta:size(),
    FileSize :: file_meta:size(), Blocks :: proplists:proplist()) ->
    Evt :: #write_event{}.
write_event(FileUuid, Size, FileSize, Blocks) ->
    #write_event{
        file_uuid = FileUuid,
        size = Size,
        file_size = FileSize,
        blocks = [#file_block{offset = O, size = S} || {O, S} <- Blocks]
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns update event for file attributes associated with default file.
%% @end
%%--------------------------------------------------------------------
-spec file_attr_update_event() -> #update_event{}.
file_attr_update_event() ->
    #update_event{object = #file_attr{uuid = <<"file_uuid">>}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns update event for file attributes associated with default file.
%% @end
%%--------------------------------------------------------------------
-spec file_location_update_event() -> #update_event{}.
file_location_update_event() ->
    #update_event{object = #file_location{uuid = <<"file_uuid">>}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns default event stream definition based on testcase name.
%% @end
%%--------------------------------------------------------------------
-spec event_stream_type_for_testcase_name(Case :: atom()) ->
    StmDef :: event_stream:definition().
event_stream_type_for_testcase_name(emit_read_event_should_execute_handler) ->
    ?READ_EVENT_STREAM;

event_stream_type_for_testcase_name(emit_write_event_should_execute_handler) ->
    ?WRITE_EVENT_STREAM;

event_stream_type_for_testcase_name(emit_file_attr_update_event_should_execute_handler) ->
    ?FILE_ATTR_EVENT_STREAM;

event_stream_type_for_testcase_name(emit_file_location_update_event_should_execute_handler) ->
    ?FILE_LOCATION_EVENT_STREAM;

event_stream_type_for_testcase_name(_) ->
    ?READ_EVENT_STREAM.