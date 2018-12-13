%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides communication API between remote client/provider
%%% and server.
%%% @end
%%%-------------------------------------------------------------------
-module(communicator).
-author("Michal Wrzeszcz").

-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("timeouts.hrl").

%%% API - convenience functions
-export([send_to_client/2, send_to_client/3, send_to_provider/2,
    send_to_provider/3, stream_to_provider/3, communicate_with_provider/2,
    communicate_with_provider/3]).
%%% API - generic function
-export([communicate/3]).

-type message() :: #client_message{} | #server_message{}.
-type generic_message() :: tuple().
%%-type options() :: map().
-type options() ::  #{wait_for_ans => boolean(), % default: false
                    repeats => non_neg_integer() | infinity | check_all_connections, % default: 2
                    error_on_empty_pool => boolean(), % default: true
                    ensure_connected => boolean(), % default: false
                    exclude => [pid()], % default: []
                    stream => {true, sequencer:stream_id()} | false, % default: false
                    ignore_send_errors => boolean(), % default: false
                    use_msg_id => boolean() | {true, pid()}}. % default: false
-type asyn_ans() :: ok | {ok | message_id:id()} | {error, Reason :: term()}.
-type sync_answer() :: ok | {ok, message()} | {error, Reason :: term()}.
-type answer() :: asyn_ans() | sync_answer().
-type ref() :: pid() | session:id().

%%%===================================================================
%%% API - convenience functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends message to client with default options.
%% @end
%%--------------------------------------------------------------------
-spec send_to_client(generic_message(), ref()) -> asyn_ans().
send_to_client(Msg, Ref) ->
    ?MODULE:send_to_client(Msg, Ref, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Sends message to client.
%% @end
%%--------------------------------------------------------------------
-spec send_to_client(generic_message(), ref(), options()) -> asyn_ans().
send_to_client(#server_message{} = Msg, Ref, Options) ->
    communicate(Msg, Ref, Options);
send_to_client(Msg, Ref, Options) ->
    send_to_client(#server_message{message_body = Msg}, Ref, Options).

%%--------------------------------------------------------------------
%% @doc
%% Sends message to provider with default options. Does not wait for answer.
%% @end
%%--------------------------------------------------------------------
-spec send_to_provider(generic_message(), ref()) -> asyn_ans().
send_to_provider(Msg, Ref) ->
    send_to_provider(Msg, Ref, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Sends message to provider. Does not wait for answer.
%% @end
%%--------------------------------------------------------------------
-spec send_to_provider(generic_message(), ref(), options()) -> asyn_ans().
send_to_provider(#client_message{} = Msg, Ref, Options) ->
    communicate(Msg, Ref, Options#{error_on_empty_pool => false,
        ensure_connected => true});
send_to_provider(Msg, Ref, Options) ->
    send_to_provider(#client_message{message_body = Msg}, Ref, Options).

%%--------------------------------------------------------------------
%% @doc
%% Sends stream message to provider.
%% @end
%%--------------------------------------------------------------------
-spec stream_to_provider(generic_message(), ref(), sequencer:stream_id()) ->
    asyn_ans().
stream_to_provider(Msg, Ref, StmId) ->
    send_to_provider(Msg, Ref, #{stream => {true, StmId}, repeats => 1}).

%%--------------------------------------------------------------------
%% @doc
%% Communicates with provider and waits for answer.
%% @end
%%--------------------------------------------------------------------
-spec communicate_with_provider(generic_message(), ref()) ->
    sync_answer().
communicate_with_provider(Msg, Ref) ->
    send_to_provider(Msg, Ref, #{wait_for_ans => true}).

%%--------------------------------------------------------------------
%% @doc
%% Communicates with provider and waits for answer.
%% @end
%%--------------------------------------------------------------------
-spec communicate_with_provider(generic_message(), ref(), pid()) ->
    sync_answer().
communicate_with_provider(Msg, Ref, Recipent) ->
    Options = case Msg of
        #client_message{message_stream = #message_stream{stream_id = StmId}}
            when is_integer(StmId) ->
            #{use_msg_id => {true, Recipent}, stream => {true, StmId}};
        _ ->
            #{use_msg_id => {true, Recipent}}
    end,
    send_to_provider(Msg, Ref, Options).

%%%===================================================================
%%% API - generic function
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Basic function that is responsible for communication with clients and other
%% providers.
%% @end
%%--------------------------------------------------------------------
-spec communicate(message(), ref(), options()) -> answer().
communicate(Msg, Ref, Options) ->
    Options2 = case maps:get(wait_for_ans, Options, false) of
        true -> Options#{use_msg_id => {true, self()}};
        _ -> Options
    end,

    case maps:get(repeats, Options2, 2) of
        check_all_connections ->
            {ok, Connections} = session_connections:get_connections(Ref),
            Connections2 = Connections -- maps:get(exclude, Options2, []),
            communicate_loop(Msg, Ref, Options2, Connections2);
        Repeats ->
            communicate_loop(Msg, Ref, Options2, Repeats)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Loop that tries to send message.
%% @end
%%--------------------------------------------------------------------
-spec communicate_loop(message(), ref(), options(),
    non_neg_integer() | infinity | [pid()]) -> answer().
communicate_loop(Msg, Ref, Options, 1) ->
    send(Msg, Ref, Options);
communicate_loop(Msg, _Ref, Options, [Con]) ->
    send(Msg, Con, Options);
communicate_loop(Msg, _Ref, Options, [Con | Cons]) ->
    send_in_loop(Msg, Con, Options, Cons);
communicate_loop(Msg, Ref, Options, Retry) ->
    send_in_loop(Msg, Ref, Options, Retry).

-spec send_in_loop(message(), ref(), options(),
    non_neg_integer() | infinity | [pid()]) -> answer().
send_in_loop(Msg, Ref, Options, Retry) ->
    case send(Msg, Ref, Options) of
        ok -> ok;
        {ok, Ans} -> {ok, Ans};
        {error, empty_connection_pool} ->
            case maps:get(error_on_empty_pool, Options, true) of
                true ->
                    {error, empty_connection_pool};
                _ ->
                    retry(Msg, Ref, Options, Retry)
            end;
        {error, _} ->
            retry(Msg, Ref, Options, Retry)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retry to send message.
%% @end
%%--------------------------------------------------------------------
-spec retry(message(), ref(), options(), non_neg_integer() | infinity | [pid()]) ->
    answer().
retry(Msg, Ref, Options, Retry) ->
    timer:sleep(?SEND_RETRY_DELAY),
    case Retry of
        Int when is_integer(Int) -> communicate_loop(Msg, Ref, Options, Int - 1);
        _ -> communicate_loop(Msg, Ref, Options, Retry)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to send message once.
%% @end
%%--------------------------------------------------------------------
-spec send(message(), ref(), options()) -> answer().
send(Msg, Ref, Options) ->
    {Msg2, ReturnMsgID} = complete_msg(Msg, Options),

    case maps:get(ensure_connected, Options, false) of
        true -> session_connections:ensure_connected(Ref);
        _ -> ok
    end,

    SendAns = case maps:get(stream, Options, false) of
        {true, StmId} -> sequencer:send_message(Msg2, StmId, Ref);
        _ -> forward_to_connection_proc(Msg2, Ref,
            maps:get(ignore_send_errors, Options, false))
    end,

    case {SendAns, maps:get(wait_for_ans, Options, false)} of
        {ok, true} ->
            receive_message(Msg2);
        {ok, _} ->
            case ReturnMsgID of
                {true, MsgId} -> {ok, MsgId};
                _ -> ok
            end;
        _ ->
            SendAns
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Forwards message to connection.
%% @end
%%--------------------------------------------------------------------
-spec forward_to_connection_proc(message(), ref(), Async :: boolean()) ->
    ok | {error, Reason :: term()}.
forward_to_connection_proc(Msg, Ref, true) when is_pid(Ref) ->
    Ref ! {send_async, Msg},
    ok;
forward_to_connection_proc(Msg, Ref, false) when is_pid(Ref) ->
    try
        Ref ! {send_sync, self(), Msg},
        receive
            {result, Resp} ->
                Resp
        after
            ?DEFAULT_REQUEST_TIMEOUT ->
                {error, timeout}
        end
    catch
        _:Reason -> {error, Reason}
    end;
forward_to_connection_proc(Msg, SessionId, Async) ->
    MsgWithProxyInfo = case Async of
        true -> Msg;
        false -> protocol_utils:fill_proxy_info(Msg, SessionId)
    end,
    case session_connections:get_random_connection(SessionId) of
        {ok, Con} -> forward_to_connection_proc(MsgWithProxyInfo, Con, Async);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Completes message with generated message ID. Returns information if message ID
%% should be used.
%% @end
%%--------------------------------------------------------------------
- spec complete_msg(message(), options()) ->
    {message(), {true, message_id:id()} | false}.
complete_msg(Msg, Options) ->
    case maps:get(use_msg_id, Options, false) of
        {true, Recipient} -> complete_msg_id(Msg, Recipient);
        true -> complete_msg_id(Msg, undefined);
        _ -> {Msg, false}
    end.

- spec complete_msg_id(message(), pid() | undefined) -> {message(), {true, message_id:id()}}.
complete_msg_id(#client_message{message_id = undefined} = Msg, Recipient) ->
    {ok, MsgId} = message_id:generate(Recipient),
    {Msg#client_message{message_id = MsgId}, {true, MsgId}};
complete_msg_id(#server_message{message_id = undefined} = Msg, Recipient) ->
    {ok, MsgId} = message_id:generate(Recipient),
    {Msg#server_message{message_id = MsgId}, {true, MsgId}};
complete_msg_id(#client_message{message_id = MsgId} = Msg, _Recipient) ->
    {Msg, {true, MsgId}};
complete_msg_id(#server_message{message_id = MsgId} = Msg, _Recipient) ->
    {Msg, {true, MsgId}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Receives reply from other provider or client
%% @end
%%--------------------------------------------------------------------
- spec receive_message(message()) -> {ok, message()} | {error, timeout}.
receive_message(#client_message{message_id = MsgId} = Msg) ->
    Timeout = 3 * async_request_manager:get_processes_check_interval(),
    receive
        #server_message{message_id = MsgId,
            message_body = #processing_status{code = 'IN_PROGRESS'}} ->
            receive_message(Msg);
        #server_message{message_id = MsgId} = ServerMsg ->
            {ok, ServerMsg}
    after
        Timeout ->
            {error, timeout}
    end;
receive_message(#server_message{message_id = MsgId}) ->
    receive
        #client_message{message_id = MsgId} = ClientMsg -> {ok, ClientMsg}
    after
    % TODO VFS-4025 - how long should we wait for client answer?
        ?DEFAULT_REQUEST_TIMEOUT ->
            {error, timeout}
    end.
