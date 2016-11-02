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
    {ok, EvtStm} = start_event_stream(Worker),
    ?assertReceivedMatch({'$gen_cast',
        {register_stream, read_event_stream, EvtStm}
    }, ?TIMEOUT),
    stop_event_stream(EvtStm).

event_stream_should_unregister_from_event_manager_on_terminate(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, EvtStm} = start_event_stream(Worker),
    stop_event_stream(EvtStm),
    ?assertReceivedMatch({'$gen_cast',
        {unregister_stream, read_event_stream}
    }, ?TIMEOUT).

event_stream_should_execute_init_handler_on_init(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, EvtStm} = start_event_stream(Worker),
    ?assertReceivedMatch({init_handler, #subscription{}, <<_/binary>>}, ?TIMEOUT),
    stop_event_stream(EvtStm).

event_stream_should_execute_terminate_handler_on_terminate(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, EvtStm} = start_event_stream(Worker),
    stop_event_stream(EvtStm),
    ?assertReceivedMatch({terminate_handler, _}, ?TIMEOUT).

event_stream_should_execute_event_handler_on_terminate(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, EvtStm} = start_event_stream(Worker),
    emit(Worker, EvtStm, read_event(1, [{0, 1}])),
    stop_event_stream(EvtStm),
    ?assertReceivedMatch({event_handler, [_]}, ?TIMEOUT).

event_stream_should_execute_event_handler_when_emission_rule_satisfied(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, EvtStm} = start_event_stream(Worker, fun(_) -> true end, infinity),
    Evt = read_event(1, [{0, 1}]),
    emit(Worker, EvtStm, Evt),
    ?assertReceivedMatch({event_handler, [Evt]}, ?TIMEOUT),
    stop_event_stream(EvtStm).

event_stream_should_execute_event_handler_when_emission_time_satisfied(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, EvtStm} = start_event_stream(Worker, fun(_) -> false end, 500),
    Evt = read_event(1, [{0, 1}]),
    emit(Worker, EvtStm, Evt),
    ?assertReceivedMatch({event_handler, [Evt]}, ?TIMEOUT),
    stop_event_stream(EvtStm).

event_stream_should_aggregate_events_with_the_same_key(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    CtrThr = 5,
    {ok, EvtStm} = start_event_stream(Worker,
        fun(Ctr) -> Ctr >= CtrThr end, infinity),
    lists:foreach(fun(N) ->
        emit(Worker, EvtStm, read_event(1, [{N, 1}]))
    end, lists:seq(0, CtrThr - 1)),
    ?assertReceivedMatch({event_handler, [#event{counter = CtrThr}]}, ?TIMEOUT),
    stop_event_stream(EvtStm).

event_stream_should_not_aggregate_events_with_different_keys(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, EvtStm} = start_event_stream(Worker,
        fun(Ctr) -> Ctr >= 2 end, infinity),
    emit(Worker, EvtStm, read_event(<<"file_uuid_1">>, 1, [{0, 1}])),
    emit(Worker, EvtStm, read_event(<<"file_uuid_2">>, 1, [{0, 1}])),
    ?assertReceivedMatch({event_handler, [_ | _]}, ?TIMEOUT),
    stop_event_stream(EvtStm).

event_stream_should_reset_metadata_after_event_handler_execution(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    CtrThr = 5,
    {ok, EvtStm} = start_event_stream(Worker,
        fun(Ctr) -> Ctr >= 1 end, infinity),
    lists:foreach(fun(N) ->
        emit(Worker, EvtStm, read_event(1, [{N, 1}])),
        ?assertReceivedMatch({event_handler, [#event{}]}, ?TIMEOUT)
    end, lists:seq(0, CtrThr - 1)),
    stop_event_stream(EvtStm).

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
-spec start_event_stream(Worker :: node()) -> {ok, EvtStm :: pid()}.
start_event_stream(Worker) ->
    start_event_stream(Worker, fun(_) -> false end, infinity).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts event stream with custom emission rule and time.
%% @end
%%--------------------------------------------------------------------
-spec start_event_stream(Worker :: node(), EmRule :: event_stream:emission_rule(),
    EmTime :: event_stream:emission_time()) -> {ok, EvtStm :: pid()}.
start_event_stream(Worker, EmRule, EmTime) ->
    EvtMan = self(),
    Sub = #subscription{
        id = 1,
        object = #read_subscription{},
        event_stream = ?READ_EVENT_STREAM#event_stream_definition{
            init_handler = fun(Sub, SessId, _) ->
                EvtMan ! {init_handler, Sub, SessId}
            end,
            terminate_handler = fun(InitResult) ->
                EvtMan ! {terminate_handler, InitResult}
            end,
            event_handler = fun(Evts, _) ->
                EvtMan ! {event_handler, Evts}
            end,
            emission_rule = EmRule,
            emission_time = EmTime
        }
    },
    SessId = <<"session_id">>,
    ?assertMatch({ok, _}, rpc:call(Worker, gen_server, start, [
        event_stream, [fuse, EvtMan, Sub, SessId], []
    ])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops event stream.
%% @end
%%--------------------------------------------------------------------
-spec stop_event_stream(EvtStm :: pid()) -> true.
stop_event_stream(EvtStm) ->
    exit(EvtStm, shutdown).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Emits an event to the event stream.
%% @end
%%--------------------------------------------------------------------
-spec emit(Worker :: node(), EvtStm :: pid(), Evt :: #event{}) -> ok.
emit(Worker, EvtStm, Evt) ->
    rpc:call(Worker, event, emit, [Evt, EvtStm]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv read_event(<<"file_uuid">>, Size, Blocks)
%% @end
%%--------------------------------------------------------------------
-spec read_event(Size :: file_meta:size(), Blocks :: proplists:proplist()) ->
    Evt :: #event{}.
read_event(Size, Blocks) ->
    read_event(<<"file_uuid">>, Size, Blocks).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns read event.
%% @end
%%--------------------------------------------------------------------
-spec read_event(FileUuid :: file_meta:uuid(), Size :: file_meta:size(),
    Blocks :: proplists:proplist()) -> Evt :: #event{}.
read_event(FileUuid, Size, Blocks) ->
    #event{key = FileUuid, stream_id = read_event_stream, object = #read_event{
        file_uuid = FileUuid, size = Size, blocks = [
            #file_block{offset = O, size = S} || {O, S} <- Blocks
        ]
    }}.