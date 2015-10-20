%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for managing connections.
%%% @end
%%%-------------------------------------------------------------------
-module(communicator).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).
-export([send/2, communicate/2, communicate_async/2, communicate_async/3]).
-export([add_connection/2, remove_connection/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-define(DEFAULT_REQUEST_TIMEOUT, timer:seconds(30)).

-define(MSG_RETRANSMISSION_INTERVAL, timer:seconds(5)).

-record(state, {
    session_id :: session:id(),
    connections = [] :: [pid()]
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SessId :: session:id(), Con :: pid()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SessId, Con) ->
    gen_server:start_link(?MODULE, [SessId, Con], []).

%%--------------------------------------------------------------------
%% @doc
%% Sends a message to the client identified by given session_id.
%% No reply is expected.
%% @end
%%--------------------------------------------------------------------
-spec send(Msg :: #server_message{} | term(), SessId :: session:id()) -> ok.
send(#server_message{} = Msg, SessId) ->
    {ok, CommPid} = session:get_communicator(SessId),
    gen_server:cast(CommPid, {send, Msg});
send(Msg, SessId) ->
    send(#server_message{message_body = Msg}, SessId).

%%--------------------------------------------------------------------
%% @doc
%% Sends a message to the server and waits for a reply.
%% @end
%%--------------------------------------------------------------------
-spec communicate(Msg :: #server_message{}, SessId :: session:id()) ->
    {ok, #client_message{}} | {error, timeout}.
communicate(ServerMsg, SessId) ->
    {ok, MsgId} = communicate_async(ServerMsg, SessId, self()),
    receive
        #client_message{message_id = MsgId} = ClientMsg -> {ok, ClientMsg}
    after
        ?DEFAULT_REQUEST_TIMEOUT ->
            {error, timeout}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sends message and expects answer (with generated id) in message's default worker.
%% @equiv communicate_async(Msg, SessId, undefined)
%% @end
%%--------------------------------------------------------------------
-spec communicate_async(Msg :: #server_message{}, SessId :: session:id()) ->
    {ok, #message_id{}}.
communicate_async(Msg, SessId) ->
    communicate_async(Msg, SessId, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Sends server_message to client, identified by given session_id.
%% This function overrides message_id of request.
%% When ReplyPid is undefined, the answer will be routed to default handler worker.
%% Otherwise the client answer will be send to ReplyPid process as:
%% #client_message{message_id = MessageId}
%% @end
%%--------------------------------------------------------------------
-spec communicate_async(Msg :: #server_message{}, SessId :: session:id(),
    Recipient :: pid() | undefined) -> {ok, #message_id{}}.
communicate_async(Msg, SessId, Recipient) ->
    {ok, GeneratedId} = message_id:generate(Recipient),
    MsgWithId = Msg#server_message{message_id = GeneratedId},
    {ok, CommPid} = session:get_communicator(SessId),
    ok = gen_server:cast(CommPid, {send, MsgWithId}),
    {ok, GeneratedId}.

%%--------------------------------------------------------------------
%% @doc
%% Adds connection to the communicator given by pid or session_id
%% @end
%%--------------------------------------------------------------------
-spec add_connection(session:id() | pid(), ConnectionPid :: pid()) -> ok.
add_connection(CommPid, ConnectionPid) when is_pid(CommPid) ->
    gen_server:call(CommPid, {add_connection, ConnectionPid});
add_connection(SessId, ConnectionPid) ->
    {ok, CommPid} = session:get_communicator(SessId),
    add_connection(CommPid, ConnectionPid).

%%--------------------------------------------------------------------
%% @doc
%% Removes connection from the communicator associated with given session
%% @end
%%--------------------------------------------------------------------
-spec remove_connection(SessId :: session:id(), ConnectionPid :: pid()) -> ok.
remove_connection(SessId, ConnectionPid) ->
    {ok, CommPid} = session:get_communicator(SessId),
    gen_server:call(CommPid, {remove_connection, ConnectionPid}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SessId, Con]) ->
    process_flag(trap_exit, true),
    {ok, SessId} = session:update(SessId, #{communicator => self()}),
    {ok, #state{session_id = SessId, connections = [Con]}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_call({add_connection, Con}, _From, #state{connections = Cons} = State) ->
    {reply, ok, State#state{connections = [Con | Cons]}};

handle_call({remove_connection, Con}, _From, #state{connections = Cons} = State) ->
    {reply, ok, State#state{connections = lists:delete(Con, Cons)}};

handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_cast({send, #server_message{} = Msg}, State = #state{connections = ConnList}) ->
    try_send(Msg, ConnList),
    {noreply, State};

handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_info({timer, Msg}, State) ->
    gen_server:cast(self(), Msg),
    {noreply, State};

handle_info({'EXIT', _, normal}, State) ->
    {noreply, State};

handle_info(_Info, State) ->
    ?log_bad_request(_Info),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term().
terminate(Reason, #state{session_id = SessId} = State) ->
    ?log_terminate(Reason, State),
    session_manager:remove_session(SessId).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) -> {ok, NewState :: #state{}} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Send Msg to random connection.
%% @end
%%--------------------------------------------------------------------
-spec try_send(Msg :: #server_message{}, Connections :: [pid()]) ->
    ok | {error, Reason :: term()}.
try_send(Msg, Connections) ->
    RandomConnection =
        try utils:random_element(Connections)
        catch _:_ -> error_empty_connection_pool
        end,
    CommunicatorPid = self(),
    spawn_link( %todo test performance of spawning vs notifying about each message
        fun() ->
            try connection:send(RandomConnection, Msg) of
                ok -> ok;
                Error ->
                    ?warning("Could not send message ~p, due to error: ~p, retrying in ~p seconds.",
                        [Msg,  Error, ?MSG_RETRANSMISSION_INTERVAL]),
                    erlang:send_after(?MSG_RETRANSMISSION_INTERVAL, CommunicatorPid, {timer, {send, Msg}})
            catch
                _:Error ->
                    ?warning_stacktrace("Could not send message ~p, due error: ~p, retrying in ~p seconds.",
                        [Msg,  Error, ?MSG_RETRANSMISSION_INTERVAL]),
                    erlang:send_after(?MSG_RETRANSMISSION_INTERVAL, CommunicatorPid, {timer, {send, Msg}})
            end
        end),
    ok.