%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(connection_manager).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("timeouts.hrl").
-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").

%% API
-export([start_link/0]).
-export([
    communicate/2,
    send_sync/2, send_sync/3,
    send_async/2,
    respond/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {}).

-type state() :: #state{}.
-type message() :: #client_message{} | #server_message{}.

-define(DEFAULT_PROCESSES_CHECK_INTERVAL, timer:seconds(10)).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to peer and awaits answer.
%% @end
%%--------------------------------------------------------------------
-spec communicate(session:id(), message()) ->
    {ok, message()} | {error, term()}.
communicate(SessionId, Msg0) ->
    {ok, MsgId} = message_id:generate(self()),
    Msg1 = fill_msg_id(Msg0, MsgId),
    case send_sync_internal(SessionId, Msg1) of
        ok ->
            await_response(Msg1);
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% @equiv send_sync(SessionId, Msg, undefined).
%% @end
%%--------------------------------------------------------------------
-spec send_sync(session:id(), message()) ->
    ok | {ok, message_id:id()} | {error, Reason :: term()}.
send_sync(SessionId, Msg) ->
    send_sync(SessionId, Msg, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to peer.
%% @end
%%--------------------------------------------------------------------
-spec send_sync(session:id(), message(), Recipient :: undefined | pid()) ->
    ok | {ok, message_id:id()} | {error, Reason :: term()}.
send_sync(SessionId, Msg0, Recipient) ->
    {MsgId, Msg1} = maybe_fill_msg_id(Msg0, Recipient),
    case {send_sync_internal(SessionId, Msg1), MsgId} of
        {ok, undefined} ->
            ok;
        {ok, _} ->
            {ok, MsgId};
        {Error, _} ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Schedules message to be send to peer.
%% @end
%%--------------------------------------------------------------------
-spec send_async(session:id(), message()) -> ok.
send_async(SessionId, Msg) ->
    case session_connections:get_random_connection(SessionId) of
        {ok, Conn} ->
            connection:send_async(Conn, Msg);
        _Error ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sends response to peer.
%% @end
%%--------------------------------------------------------------------
-spec respond() -> ok.
respond() -> ok.


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
    {ok, state()} | {ok, state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([]) ->
    {ok, #state{}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call(_Request, _From, State) ->
    {reply, ok, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast(_Request, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
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
    state()) -> term().
terminate(_Reason, _State) ->
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, state(), Extra :: term()) ->
    {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec maybe_fill_msg_id(message(), Recipient :: undefined | pid()) ->
    {undefined | message_id:id(), message()}.
maybe_fill_msg_id(#client_message{message_id = MsgId} = Msg, undefined) ->
    {MsgId, Msg};
maybe_fill_msg_id(#server_message{message_id = MsgId} = Msg, undefined) ->
    {MsgId, Msg};
maybe_fill_msg_id(Msg, Recipient) ->
    {ok, MsgId} = message_id:generate(Recipient),
    {MsgId, fill_msg_id(Msg, MsgId)}.


-spec fill_msg_id(message(), message_id:id()) -> message().
fill_msg_id(#client_message{} = Msg, MsgId) ->
    Msg#client_message{message_id = MsgId};
fill_msg_id(#server_message{} = Msg, MsgId) ->
    Msg#server_message{message_id = MsgId}.


%% @private
-spec send_sync_internal(session:id(), message()) -> ok | {error, term()}.
send_sync_internal(SessionId, Msg) ->
    case session_connections:get_connections(SessionId) of
        {ok, []} ->
            {error, no_connections};
        {ok, Cons} ->
            send_in_loop(Msg, shuffle_connections(Cons));
        Error ->
            Error
    end.


%% @private
shuffle_connections(Cons) ->
    [X || {_, X} <- lists:sort([{random:uniform(), Conn} || Conn <- Cons])].


%% @private
-spec send_in_loop(message(), [pid()]) -> ok | {error, term()}.
send_in_loop(Msg, [Conn]) ->
    connection:send_sync(Conn, Msg);
send_in_loop(Msg, [Conn | Cons]) ->
    case connection:send_sync(Conn, Msg) of
        ok ->
            ok;
        {error, serialization_failed} = SerializationError ->
            SerializationError;
        {error, sending_msg_via_wrong_connection} = WrongConnError ->
            WrongConnError;
        _Error ->
            % TODO is necessary?
            timer:sleep(?SEND_RETRY_DELAY),
            send_in_loop(Msg, Cons)
    end.


%% @private
-spec await_response(message()) -> {ok, message()} | {error, timeout}.
await_response(#client_message{message_id = MsgId} = Msg) ->
    Timeout = 3 * ?DEFAULT_PROCESSES_CHECK_INTERVAL,
    receive
        #server_message{
            message_id = MsgId,
            message_body = #processing_status{code = 'IN_PROGRESS'}
        } ->
            await_response(Msg);
        #server_message{message_id = MsgId} = ServerMsg ->
            {ok, ServerMsg}
    after Timeout ->
        {error, timeout}
    end;
await_response(#server_message{message_id = MsgId}) ->
    receive
        #client_message{message_id = MsgId} = ClientMsg ->
            {ok, ClientMsg}
    % TODO VFS-4025 - how long should we wait for client answer?
    after ?DEFAULT_REQUEST_TIMEOUT ->
        {error, timeout}
    end.
