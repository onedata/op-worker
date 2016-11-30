%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains event stream tests.
%%% @end
%%%-------------------------------------------------------------------
-module(event_stream_test_SUITE).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    event_stream_should_register_with_event_manager_on_init/1,
    event_stream_should_unregister_from_event_manager_on_terminate/1,
    event_stream_should_execute_init_handler_on_init/1,
    event_stream_should_execute_terminate_handler_on_terminate/1,
    event_stream_should_execute_event_handler_on_terminate/1,
    event_stream_should_execute_event_handler_when_emission_rule_satisfied/1,
    event_stream_should_execute_event_handler_when_emission_time_satisfied/1,
    event_stream_should_aggregate_events_with_the_same_key/1,
    event_stream_should_not_aggregate_events_with_different_keys/1,
    event_stream_should_reset_metadata_after_event_handler_execution/1
]).

all() ->
    ?ALL([
        event_stream_should_register_with_event_manager_on_init,
        event_stream_should_unregister_from_event_manager_on_terminate,
        event_stream_should_execute_init_handler_on_init,
        event_stream_should_execute_terminate_handler_on_terminate,
        event_stream_should_execute_event_handler_on_terminate,
        event_stream_should_execute_event_handler_when_emission_rule_satisfied,
        event_stream_should_execute_event_handler_when_emission_time_satisfied,
        event_stream_should_aggregate_events_with_the_same_key,
        event_stream_should_not_aggregate_events_with_different_keys,
        event_stream_should_reset_metadata_after_event_handler_execution
    ]).

-define(TIMEOUT, timer:seconds(15)).

%%%===================================================================
%%% Test functions
%%%===================================================================

event_stream_should_register_with_event_manager_on_init(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, Stm} = start_event_stream(Worker),
    ?assertReceivedMatch({'$gen_cast',
        {register_stream, file_read, Stm}
    }, ?TIMEOUT),
    stop_event_stream(Stm).

event_stream_should_unregister_from_event_manager_on_terminate(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, Stm} = start_event_stream(Worker),
    stop_event_stream(Stm),
    ?assertReceivedMatch({'$gen_cast',
        {unregister_stream, file_read}
    }, ?TIMEOUT).

event_stream_should_execute_init_handler_on_init(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, Stm} = start_event_stream(Worker),
    ?assertReceivedMatch({init_handler, 1, <<_/binary>>}, ?TIMEOUT),
    stop_event_stream(Stm).

event_stream_should_execute_terminate_handler_on_terminate(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, Stm} = start_event_stream(Worker),
    stop_event_stream(Stm),
    ?assertReceivedMatch({terminate_handler, _}, ?TIMEOUT).

event_stream_should_execute_event_handler_on_terminate(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, Stm} = start_event_stream(Worker),
    emit(Worker, Stm, file_read_event(1, [{0, 1}])),
    stop_event_stream(Stm),
    ?assertReceivedMatch({event_handler, [_]}, ?TIMEOUT).

event_stream_should_execute_event_handler_when_emission_rule_satisfied(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, Stm} = start_event_stream(Worker, fun(_) -> true end, infinity),
    Evt = file_read_event(1, [{0, 1}]),
    emit(Worker, Stm, Evt),
    ?assertReceivedMatch({event_handler, [Evt]}, ?TIMEOUT),
    stop_event_stream(Stm).

event_stream_should_execute_event_handler_when_emission_time_satisfied(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, Stm} = start_event_stream(Worker, fun(_) -> false end, 500),
    Evt = file_read_event(1, [{0, 1}]),
    emit(Worker, Stm, Evt),
    ?assertReceivedMatch({event_handler, [Evt]}, ?TIMEOUT),
    stop_event_stream(Stm).

event_stream_should_aggregate_events_with_the_same_key(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    CtrThr = 5,
    {ok, Stm} = start_event_stream(Worker,
        fun(Ctr) -> Ctr >= CtrThr end, infinity),
    lists:foreach(fun(N) ->
        emit(Worker, Stm, file_read_event(1, [{N, 1}]))
    end, lists:seq(0, CtrThr - 1)),
    ?assertReceivedMatch({event_handler, [#file_read_event{counter = CtrThr}]},
        ?TIMEOUT),
    stop_event_stream(Stm).

event_stream_should_not_aggregate_events_with_different_keys(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, Stm} = start_event_stream(Worker,
        fun(Ctr) -> Ctr >= 2 end, infinity),
    emit(Worker, Stm, file_read_event(<<"file_uuid_1">>, 1, [{0, 1}])),
    emit(Worker, Stm, file_read_event(<<"file_uuid_2">>, 1, [{0, 1}])),
    ?assertReceivedMatch({event_handler, [_ | _]}, ?TIMEOUT),
    stop_event_stream(Stm).

event_stream_should_reset_metadata_after_event_handler_execution(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    CtrThr = 5,
    {ok, Stm} = start_event_stream(Worker,
        fun(Ctr) -> Ctr >= 1 end, infinity),
    lists:foreach(fun(N) ->
        emit(Worker, Stm, file_read_event(1, [{N, 1}])),
        ?assertReceivedMatch({event_handler, [#file_read_event{}]}, ?TIMEOUT)
    end, lists:seq(0, CtrThr - 1)),
    stop_event_stream(Stm).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    ?TEST_STOP(Config).

init_per_testcase(Case, Config) ->
    ?CASE_START(Case),
    Config.

end_per_testcase(Case, _Config) ->
    ?CASE_STOP(Case).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts event stream with emission rule always returning 'false' and infinite
%% emission time.
%% @end
%%--------------------------------------------------------------------
-spec start_event_stream(Worker :: node()) -> {ok, Stm :: pid()}.
start_event_stream(Worker) ->
    start_event_stream(Worker, fun(_) -> false end, infinity).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts event stream with custom emission rule and time.
%% @end
%%--------------------------------------------------------------------
-spec start_event_stream(Worker :: node(), EmRule :: event_stream:emission_rule(),
    EmTime :: event_stream:emission_time()) -> {ok, Stm :: pid()}.
start_event_stream(Worker, EmRule, EmTime) ->
    Mgr = self(),
    SessId = <<"session_id">>,
    Stm = event_stream_factory:create(#file_read_subscription{}),
    Sub = #subscription{
        id = 1,
        type = #file_read_subscription{},
        stream = Stm#event_stream{
            init_handler = fun(_SubId, _SessId) ->
                Mgr ! {init_handler, _SubId, _SessId}
            end,
            terminate_handler = fun(InitResult) ->
                Mgr ! {terminate_handler, InitResult}
            end,
            event_handler = fun(Evts, _) ->
                Mgr ! {event_handler, Evts}
            end,
            emission_rule = EmRule,
            emission_time = EmTime,
            transition_rule = fun(Ctr, _) -> Ctr + 1 end
        }
    },
    ?assertMatch({ok, _}, rpc:call(Worker, gen_server, start, [
        event_stream, [Mgr, Sub, SessId], []
    ])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops event stream.
%% @end
%%--------------------------------------------------------------------
-spec stop_event_stream(Stm :: pid()) -> true.
stop_event_stream(Stm) ->
    exit(Stm, shutdown).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Emits an event to the event stream.
%% @end
%%--------------------------------------------------------------------
-spec emit(Worker :: node(), Stm :: pid(), Evt :: #event{}) -> ok.
emit(Worker, Stm, Evt) ->
    rpc:call(Worker, event, emit, [Evt, Stm]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv file_read_event(<<"file_uuid">>, Size, Blocks)
%% @end
%%--------------------------------------------------------------------
-spec file_read_event(Size :: file_meta:size(), Blocks :: proplists:proplist()) ->
    Evt :: #event{}.
file_read_event(Size, Blocks) ->
    file_read_event(<<"file_uuid">>, Size, Blocks).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns read event.
%% @end
%%--------------------------------------------------------------------
-spec file_read_event(FileUuid :: file_meta:uuid(), Size :: file_meta:size(),
    Blocks :: proplists:proplist()) -> Evt :: #event{}.
file_read_event(FileUuid, Size, Blocks) ->
    #file_read_event{
        file_uuid = FileUuid, size = Size, blocks = [
            #file_block{offset = O, size = S} || {O, S} <- Blocks
        ]
    }.