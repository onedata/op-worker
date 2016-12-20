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
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    subscribe_should_create_subscription/1,
    unsubscribe_should_remove_subscription/1,
    subscribe_should_notify_event_manager/1,
    subscribe_should_notify_all_event_managers/1,
    emit_file_read_event_should_execute_handler/1,
    emit_file_written_event_should_execute_handler/1,
    emit_file_attr_changed_event_should_execute_handler/1,
    emit_file_location_changed_event_should_execute_handler/1,
    flush_should_notify_awaiting_process/1
]).

all() ->
    ?ALL([
        subscribe_should_create_subscription,
        unsubscribe_should_remove_subscription,
        subscribe_should_notify_event_manager,
        subscribe_should_notify_all_event_managers,
        emit_file_read_event_should_execute_handler,
        emit_file_written_event_should_execute_handler,
        emit_file_attr_changed_event_should_execute_handler,
        emit_file_location_changed_event_should_execute_handler,
        flush_should_notify_awaiting_process
    ]).

-define(TIMEOUT, timer:seconds(15)).

%%%===================================================================
%%% Test functions
%%%===================================================================

subscribe_should_create_subscription(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, SubId} = create_subscription(default, Worker),
    ?assertMatch({ok, [_]}, rpc:call(Worker, subscription, list, [])),
    unsubscribe(Worker, SubId).

unsubscribe_should_remove_subscription(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),
    SubId = subscribe(Worker, SessId),
    unsubscribe(Worker, SubId),
    ?assertMatch({ok, []}, rpc:call(Worker, subscription, list, [])).

subscribe_should_notify_event_manager(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Evt = file_read_event(1, [{0, 1}]),
    emit(Worker, ?config(session_id, Config), Evt),
    ?assertReceivedMatch({event_handler, [Evt]}, ?TIMEOUT).

subscribe_should_notify_all_event_managers(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Evt = file_read_event(1, [{0, 1}]),
    lists:foreach(fun(SessId) ->
        emit(Worker, SessId, Evt),
        ?assertReceivedMatch({event_handler, [Evt]}, ?TIMEOUT)
    end, ?config(session_ids, Config)).

emit_file_read_event_should_execute_handler(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Evt = file_read_event(1, [{0, 1}]),
    emit(Worker, ?config(session_id, Config), Evt),
    ?assertReceivedMatch({event_handler, [Evt]}, ?TIMEOUT).

emit_file_written_event_should_execute_handler(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Evt = file_written_event(1, 1, [{0, 1}]),
    emit(Worker, ?config(session_id, Config), Evt),
    ?assertReceivedMatch({event_handler, [Evt]}, ?TIMEOUT).

emit_file_attr_changed_event_should_execute_handler(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Evt = file_attr_changed_event(),
    emit(Worker, ?config(session_id, Config), Evt),
    ?assertReceivedMatch({event_handler, [Evt]}, ?TIMEOUT).

emit_file_location_changed_event_should_execute_handler(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Evt = file_location_changed_event(),
    emit(Worker, ?config(session_id, Config), Evt),
    ?assertReceivedMatch({event_handler, [Evt]}, ?TIMEOUT).

flush_should_notify_awaiting_process(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Evt = file_read_event(1, [{0, 1}]),
    SessId = ?config(session_id, Config),
    emit(Worker, SessId, Evt),
    Ref = flush(Worker, ?config(subscription_id, Config), self(), SessId),
    ?assertReceivedMatch({Ref, ok}, ?TIMEOUT).

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

init_per_testcase(Case, Config) when
    Case =:= emit_file_read_event_should_execute_handler;
    Case =:= emit_file_written_event_should_execute_handler;
    Case =:= emit_file_attr_changed_event_should_execute_handler;
    Case =:= emit_file_location_changed_event_should_execute_handler ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, SubId} = create_subscription(Case, Worker),
    init_per_testcase(?DEFAULT_CASE(Case), [{subscription_id, SubId} | Config]);

init_per_testcase(Case, Config) when
    Case =:= flush_should_notify_awaiting_process ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    NewConfig = init_per_testcase(?DEFAULT_CASE(Case), Config),
    SessId = ?config(session_id, NewConfig),
    SubId = subscribe(Worker, SessId, #file_read_subscription{},
        notify_event_handler(), fun(_) -> false end, infinity),
    [{subscription_id, SubId} | NewConfig];

init_per_testcase(Case, Config) when
    Case =:= subscribe_should_notify_event_manager ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    NewConfig = init_per_testcase(?DEFAULT_CASE(Case), Config),
    SessId = ?config(session_id, NewConfig),
    SubId = subscribe(Worker, SessId, #file_read_subscription{},
        forward_events_event_handler(), fun(_) -> true end, infinity),
    [{subscription_id, SubId} | NewConfig];

init_per_testcase(Case, Config) when
    Case =:= subscribe_should_notify_all_event_managers ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Worker),
    {ok, SubId} = create_subscription(Case, Worker),
    SessIds = lists:map(fun(N) ->
        SessId = <<"session_id_", (integer_to_binary(N))/binary>>,
        session_setup(Worker, SessId),
        SessId
    end, lists:seq(0, 4)),
    ok = initializer:assume_all_files_in_space(Config, <<"spaceid">>),
    test_utils:mock_expect(Worker, fslogic_spaces, get_space_id,
        fun(_) -> <<"spaceid">> end),
    NewConfig = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"),
        [{session_ids, SessIds}, {subscription_id, SubId} | Config]),
    test_utils:mock_expect(Worker, file_meta, get, fun
        (<<"file_uuid">>) -> {ok, #document{}};
        (Entry) -> meck:passthrough([Entry])
    end),
    NewConfig;

init_per_testcase(_Case, Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Worker),
    {ok, SessId} = session_setup(Worker),
    ok = initializer:assume_all_files_in_space(Config, <<"spaceid">>),
    test_utils:mock_expect(Worker, fslogic_spaces, get_space_id,
        fun(_) -> <<"spaceid">> end),
    test_utils:mock_new(Workers, od_space),
    test_utils:mock_expect(Workers, od_space, get_or_fetch, fun(_, _, _) ->
        {ok, #document{value = #od_space{providers = [oneprovider:get_provider_id()]}}}
    end),
    NewConfig = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), [{session_id, SessId} | Config]),
    test_utils:mock_expect(Worker, file_meta, get, fun
        (<<"file_uuid">>) -> {ok, #document{}};
        (Entry) -> meck:passthrough([Entry])
    end),
    NewConfig.

end_per_testcase(Case, Config) when
    Case =:= emit_file_read_event_should_execute_handler;
    Case =:= emit_file_written_event_should_execute_handler;
    Case =:= emit_file_attr_changed_event_should_execute_handler;
    Case =:= emit_file_location_changed_event_should_execute_handler ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    unsubscribe(Worker, ?config(subscription_id, Config)),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) when
    Case =:= flush_should_notify_awaiting_process;
    Case =:= subscribe_should_notify_event_manager ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    unsubscribe(Worker, ?config(session_id, Config), ?config(subscription_id, Config)),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

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

end_per_testcase(_Case, Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    session_teardown(Worker, ?config(session_id, Config)),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:clear_assume_all_files_in_space(Config),
    test_utils:mock_unload(Workers, od_space),
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
    Iden = #user_identity{user_id = <<"user1">>},
    ?assertMatch({ok, _}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, fuse, Iden, #token_auth{}, [Self]]
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

subscription(Sub, Handler, EmRule, EmTime) ->
    Stm = subscription_type:get_stream(Sub),
    #subscription{
        type = Sub,
        stream = Stm#event_stream{
            emission_rule = EmRule,
            emission_time = EmTime,
            event_handler = Handler
        }
    }.

subscribe(Worker, SessId) ->
    subscribe(Worker, SessId, #file_read_subscription{},
        forward_events_event_handler(), fun(_) -> true end, infinity).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates subscription with custom event stream definition associated with session.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Worker :: node(), SessId :: session:id(),
    Sub :: subscription:type(), Handler :: event_stream:event_handler(),
    EmRule :: event_stream:emission_rule(), EmTime :: event_stream:emission_time()) ->
    SubId :: subscription:id().
subscribe(Worker, SessId, Sub, Handler, EmRule, EmTime) ->
    rpc:call(Worker, event, subscribe,
        [subscription(Sub, Handler, EmRule, EmTime), SessId]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates default subscription based on testcase name.
%% @end
%%--------------------------------------------------------------------
-spec create_subscription(Case :: atom(), Worker :: node()) ->
    {ok, SubId :: subscription:id()}.
create_subscription(Case, Worker) ->
    Sub = subscription_type_for_testcase_name(Case),
    rpc:call(Worker, subscription, create, [subscription(
        Sub,
        forward_events_event_handler(),
        fun(_) -> true end,
        infinity
    )]).

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
        (E, #{notify := NotifyFun}) ->
            ct:pal("Handler1: ~p", [E]),
            NotifyFun(#server_message{message_body = #status{code = ?OK}});
        (E, _) ->
            ct:pal("Handler2: ~p", [E]),
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes subscription for events.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(Worker :: node(), SubId :: subscription:id()) -> ok.
unsubscribe(Worker, SubId) ->
    ?assertEqual(ok, rpc:call(Worker, subscription, delete, [SubId])).

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
    ProvId = rpc:call(Worker, oneprovider, get_provider_id, []),
    rpc:call(Worker, event, flush, [ProvId, undefined, SubId, Notify, SessId]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv file_read_event(<<"file_uuid">>, Size, Blocks)
%% @end
%%--------------------------------------------------------------------
-spec file_read_event(Size :: file_meta:size(), Blocks :: proplists:proplist()) ->
    Evt :: #file_read_event{}.
file_read_event(Size, Blocks) ->
    file_read_event(<<"file_uuid">>, Size, Blocks).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns read event with custom file UUID, size and blocks.
%% @end
%%--------------------------------------------------------------------
-spec file_read_event(FileUuid :: file_meta:uuid(), Size :: file_meta:size(),
    Blocks :: proplists:proplist()) -> Evt :: #file_read_event{}.
file_read_event(FileUuid, Size, Blocks) ->
    #file_read_event{
        file_uuid = FileUuid,
        size = Size,
        blocks = [#file_block{offset = O, size = S} || {O, S} <- Blocks]
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv file_written_event(<<"file_size">>, Size, FileSize, Blocks)
%% @end
%%--------------------------------------------------------------------
-spec file_written_event(Size :: file_meta:size(), FileSize :: file_meta:size(),
    Blocks :: proplists:proplist()) -> Evt :: #file_written_event{}.
file_written_event(Size, FileSize, Blocks) ->
    file_written_event(<<"file_uuid">>, Size, FileSize, Blocks).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns write event with custom file UUID, size, file size and blocks.
%% @end
%%--------------------------------------------------------------------
-spec file_written_event(FileUuid :: file_meta:uuid(), Size :: file_meta:size(),
    FileSize :: file_meta:size(), Blocks :: proplists:proplist()) ->
    Evt :: #file_written_event{}.
file_written_event(FileUuid, Size, FileSize, Blocks) ->
    #file_written_event{
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
-spec file_attr_changed_event() -> #file_attr_changed_event{}.
file_attr_changed_event() ->
    #file_attr_changed_event{file_attr = #file_attr{uuid = <<"file_uuid">>}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns update event for file attributes associated with default file.
%% @end
%%--------------------------------------------------------------------
-spec file_location_changed_event() -> #file_location_changed_event{}.
file_location_changed_event() ->
    #file_location_changed_event{file_location = #file_location{uuid = <<"file_uuid">>}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns default event stream definition based on testcase name.
%% @end
%%--------------------------------------------------------------------
-spec subscription_type_for_testcase_name(Case :: atom()) ->
    StmDef :: event_stream:definition().
subscription_type_for_testcase_name(emit_file_read_event_should_execute_handler) ->
    #file_read_subscription{};

subscription_type_for_testcase_name(emit_file_written_event_should_execute_handler) ->
    #file_written_subscription{};

subscription_type_for_testcase_name(emit_file_attr_changed_event_should_execute_handler) ->
    #file_attr_changed_subscription{file_uuid = <<"file_uuid">>};

subscription_type_for_testcase_name(emit_file_location_changed_event_should_execute_handler) ->
    #file_location_changed_subscription{file_uuid = <<"file_uuid">>};

subscription_type_for_testcase_name(_) ->
    #file_read_subscription{}.