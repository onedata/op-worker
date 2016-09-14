%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains sequencer manager tests.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_manager_test_SUITE).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("proto/oneclient/client_messages.hrl").
-include_lib("proto/oneclient/server_messages.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    sequencer_manager_should_update_session_on_init/1,
    sequencer_manager_should_update_session_on_terminate/1,
    sequencer_manager_should_register_sequencer_in_stream/1,
    sequencer_manager_should_unregister_sequencer_in_stream/1,
    sequencer_manager_should_register_sequencer_out_stream/1,
    sequencer_manager_should_unregister_sequencer_out_stream/1,
    sequencer_manager_should_create_sequencer_stream_on_open_stream/1,
    sequencer_manager_should_send_end_of_message_stream_on_close_stream/1,
    sequencer_manager_should_forward_message_stream_reset/1,
    sequencer_manager_should_forward_message_stream_reset_to_all_streams/1,
    sequencer_manager_should_forward_message_request/1,
    sequencer_manager_should_forward_message_acknowledgement/1,
    sequencer_manager_should_start_sequencer_in_stream_on_first_message/1
]).

all() ->
    ?ALL([
        sequencer_manager_should_update_session_on_init,
        sequencer_manager_should_update_session_on_terminate,
        sequencer_manager_should_register_sequencer_in_stream,
        sequencer_manager_should_unregister_sequencer_in_stream,
        sequencer_manager_should_register_sequencer_out_stream,
        sequencer_manager_should_unregister_sequencer_out_stream,
        sequencer_manager_should_create_sequencer_stream_on_open_stream,
        sequencer_manager_should_send_end_of_message_stream_on_close_stream,
        sequencer_manager_should_forward_message_stream_reset,
        sequencer_manager_should_forward_message_stream_reset_to_all_streams,
        sequencer_manager_should_forward_message_request,
        sequencer_manager_should_forward_message_acknowledgement,
        sequencer_manager_should_start_sequencer_in_stream_on_first_message
    ]).

-define(TIMEOUT, timer:seconds(15)).

%%%========================================================s===========
%%% Test functions
%%%===================================================================

sequencer_manager_should_update_session_on_init(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertMatch({ok, _}, rpc:call(Worker, session, get_sequencer_manager,
        [?config(session_id, Config)])).

sequencer_manager_should_update_session_on_terminate(Config) ->
    stop_sequencer_manager(?config(sequencer_manager, Config)),
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual({error, {not_found, missing}}, rpc:call(
        Worker, session, get_sequencer_manager, [?config(session_id, Config)]
    ), 10).

sequencer_manager_should_register_sequencer_in_stream(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    gen_server:cast(SeqMan, {register_in_stream, 1, self()}),
    gen_server:cast(SeqMan, client_message()),
    ?assertReceivedMatch({'$gen_event', #client_message{}}, ?TIMEOUT).

sequencer_manager_should_unregister_sequencer_in_stream(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    gen_server:cast(SeqMan, client_message()),
    gen_server:cast(SeqMan, {unregister_in_stream, 1}),
    gen_server:cast(SeqMan, client_message()),
    ?assertReceivedMatch({start_sequencer_stream, _}, ?TIMEOUT),
    ?assertReceivedMatch({start_sequencer_stream, _}, ?TIMEOUT).

sequencer_manager_should_register_sequencer_out_stream(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    gen_server:cast(SeqMan, {register_out_stream, 1, self()}),
    gen_server:cast(SeqMan, server_message()),
    ?assertReceivedMatch({'$gen_cast', #server_message{}}, ?TIMEOUT).

sequencer_manager_should_unregister_sequencer_out_stream(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    gen_server:cast(SeqMan, {register_out_stream, 1, self()}),
    gen_server:cast(SeqMan, {unregister_out_stream, 1}),
    gen_server:cast(SeqMan, server_message()),
    ?assertNotReceivedMatch({'$gen_cast', #server_message{}}, ?TIMEOUT).

sequencer_manager_should_create_sequencer_stream_on_open_stream(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    ?assertMatch({ok, _}, gen_server:call(SeqMan, open_stream)),
    ?assertReceivedMatch({start_sequencer_stream, _}, ?TIMEOUT).

sequencer_manager_should_send_end_of_message_stream_on_close_stream(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    gen_server:cast(SeqMan, {register_out_stream, 1, self()}),
    gen_server:cast(SeqMan, {close_stream, 1}),
    ?assertReceivedMatch({'$gen_cast', #server_message{
        message_body = #end_of_message_stream{}
    }}, ?TIMEOUT).

sequencer_manager_should_forward_message_stream_reset(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    Msg = #message_stream_reset{stream_id = 1},
    gen_server:cast(SeqMan, {register_out_stream, 1, self()}),
    gen_server:cast(SeqMan, #client_message{message_body = Msg}),
    ?assertReceivedMatch({'$gen_cast', Msg}, ?TIMEOUT).

sequencer_manager_should_forward_message_stream_reset_to_all_streams(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    Msg = #message_stream_reset{},
    lists:foreach(fun(StmId) ->
        gen_server:cast(SeqMan, {register_out_stream, StmId, self()})
    end, lists:seq(0, 4)),
    gen_server:cast(SeqMan, #client_message{message_body = Msg}),
    lists:foreach(fun(_) ->
        ?assertReceivedMatch({'$gen_cast', Msg}, ?TIMEOUT)
    end, lists:seq(0, 4)).

sequencer_manager_should_forward_message_request(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    Msg = #message_request{stream_id = 1},
    gen_server:cast(SeqMan, {register_out_stream, 1, self()}),
    gen_server:cast(SeqMan, #client_message{message_body = Msg}),
    ?assertReceivedMatch({'$gen_cast', Msg}, ?TIMEOUT).

sequencer_manager_should_forward_message_acknowledgement(Config) ->
    SeqMan = ?config(sequencer_manager, Config),
    Msg = #message_acknowledgement{stream_id = 1},
    gen_server:cast(SeqMan, {register_out_stream, 1, self()}),
    gen_server:cast(SeqMan, #client_message{message_body = Msg}),
    ?assertReceivedMatch({'$gen_cast', Msg}, ?TIMEOUT).

sequencer_manager_should_start_sequencer_in_stream_on_first_message(Config) ->
    gen_server:cast(?config(sequencer_manager, Config), client_message()),
    ?assertReceivedMatch({start_sequencer_stream, _}, ?TIMEOUT),
    ?assertReceivedMatch({'$gen_event', #client_message{}}, ?TIMEOUT).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]).

end_per_suite(Config) ->
    ?TEST_STOP(Config).

init_per_testcase(Case, Config) when
    Case =:= sequencer_manager_should_unregister_sequencer_in_stream;
    Case =:= sequencer_manager_should_create_sequencer_stream_on_open_stream;
    Case =:= sequencer_manager_should_start_sequencer_in_stream_on_first_message ->
    ?CASE_START(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    mock_sequencer_stream_sup(Worker),
    init_per_testcase(default, Config);

init_per_testcase(Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?CASE_START(Case),
    {ok, SessId} = session_setup(Worker),
    initializer:remove_pending_messages(),
    mock_communicator(Worker),
    mock_sequencer_manager_sup(Worker),
    {ok, SeqMan} = start_sequencer_manager(Worker, SessId),
    [{sequencer_manager, SeqMan}, {session_id, SessId} | Config].

end_per_testcase(Case, Config) when
    Case =:= sequencer_manager_should_unregister_sequencer_in_stream;
    Case =:= sequencer_manager_should_create_sequencer_stream_on_open_stream;
    Case =:= sequencer_manager_should_start_sequencer_in_stream_on_first_message ->
    ?CASE_STOP(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    end_per_testcase(default, Config),
    test_utils:mock_validate_and_unload(Worker, sequencer_stream_sup);

end_per_testcase(Case, Config) ->
    ?CASE_STOP(Case),
    [Worker | _] = ?config(op_worker_nodes, Config),
    stop_sequencer_manager(?config(sequencer_manager, Config)),
    test_utils:mock_validate_and_unload(Worker, [communicator, sequencer_manager_sup]),
    session_teardown(Worker, ?config(session_id, Config)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts sequencer manager for given session.
%% @end
%%--------------------------------------------------------------------
-spec start_sequencer_manager(Worker :: node(), SessId :: session:id()) ->
    {ok, SeqMan :: pid()}.
start_sequencer_manager(Worker, SessId) ->
    SeqManSup = self(),
    ?assertMatch({ok, _}, rpc:call(Worker, gen_server, start, [
        sequencer_manager, [SeqManSup, SessId], []
    ])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops event stream.
%% @end
%%--------------------------------------------------------------------
-spec stop_sequencer_manager(SeqMan :: pid()) -> true.
stop_sequencer_manager(SeqMan) ->
    exit(SeqMan, shutdown).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates session document in datastore.
%% @end
%%--------------------------------------------------------------------
-spec session_setup(Worker :: node()) -> {ok, SessId :: session:id()}.
session_setup(Worker) ->
    ?assertMatch({ok, _}, rpc:call(Worker, session, create, [#document{
        key = <<"session_id">>, value = #session{}
    }])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes session document from datastore.
%% @end
%%--------------------------------------------------------------------
-spec session_teardown(Worker :: node(), SessId :: session:id()) -> ok.
session_teardown(Worker, SessId) ->
    rpc:call(Worker, session, delete, [SessId]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns client message as part of a message stream with default stream ID.
%% @end
%%--------------------------------------------------------------------
-spec client_message() -> Msg :: #client_message{}.
client_message() ->
    #client_message{message_stream = #message_stream{stream_id = 1, sequence_number = 0}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns server message as part of a message stream with default stream ID.
%% @end
%%--------------------------------------------------------------------
-spec server_message() -> Msg :: #server_message{}.
server_message() ->
    #server_message{message_stream = #message_stream{stream_id = 1}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks sequencer manager supervisor, so that it returns this process as event
%% stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec mock_sequencer_manager_sup(Worker :: node()) -> ok.
mock_sequencer_manager_sup(Worker) ->
    SeqStmSup = self(),
    test_utils:mock_new(Worker, [sequencer_manager_sup]),
    test_utils:mock_expect(Worker, sequencer_manager_sup,
        get_sequencer_stream_sup, fun
            (_, _) -> {ok, SeqStmSup}
        end
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks sequencer stream supervisor, so that it notifies this process when
%% sequecner stream is started. Moreover, returns this process as started
%% sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec mock_sequencer_stream_sup(Worker :: node()) -> ok.
mock_sequencer_stream_sup(Worker) ->
    Self = self(),
    test_utils:mock_new(Worker, sequencer_stream_sup),
    test_utils:mock_expect(Worker, sequencer_stream_sup,
        start_sequencer_stream, fun
            (_, _, StmId, _) ->
                Self ! {start_sequencer_stream, StmId},
                {ok, Self}
        end
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Mocks communicator, so that on send it forwards all messages to this process.
%% @end
%%--------------------------------------------------------------------
-spec mock_communicator(Worker :: node()) -> ok.
mock_communicator(Worker) ->
    Self = self(),
    test_utils:mock_new(Worker, communicator),
    test_utils:mock_expect(Worker, communicator, send, fun
        (Msg, _, _) -> Self ! Msg, ok
    end).