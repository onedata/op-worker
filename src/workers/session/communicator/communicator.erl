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

-include("proto_internal/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).
-export([send/2, communicate/2, communicate_async/2, communicate_async/3]).
-export([add_connection/2, remove_connection/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

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
send(#server_message{} = _Msg, _SessId) ->
    ok;
send(Msg, SessId) ->
    send(#server_message{message_body = Msg}, SessId).

%%--------------------------------------------------------------------
%% @doc
%% Sends a message to the server and waits for a reply.
%% @end
%%--------------------------------------------------------------------
-spec communicate(Msg :: #server_message{}, SessId :: session:id()) -> ok.
communicate(_ServerMsg, _SessId) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Sends message and expects answer (with generated id) in message's default worker.
%% @equiv communicate_async(Msg, SessId, undefined)
%% @end
%%--------------------------------------------------------------------
-spec communicate_async(Msg :: #server_message{}, SessId :: session:id()) -> ok.
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
    Recipient :: pid() | undefined) -> ok.
communicate_async(_Msg, _SessId, _Recipient) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds new connection to communicator.
%% @end
%%--------------------------------------------------------------------
-spec add_connection(Comm :: pid(), Con :: pid()) -> ok.
add_connection(Comm, Con) ->
    gen_server:call(Comm, {add_connection, Con}).

%%--------------------------------------------------------------------
%% @doc
%% Removes new connection to communicator.
%% @end
%%--------------------------------------------------------------------
-spec remove_connection(Comm :: pid(), Con :: pid()) -> ok.
remove_connection(Comm, Con) ->
    gen_server:call(Comm, {remove_connection, Con}).

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
handle_call(_Request, _From, State) ->
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
handle_cast(_Request, State) ->
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
handle_info(_Info, State) ->
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
    ?warning("Event manager terminated in state ~p due to: ~p", [State, Reason]),
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