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
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").

%%% API - convenience functions
-export([send_to_client/2, send_to_client/3, send_to_provider/2,
    send_to_provider/3, stream_to_provider/3, communicate_with_provider/2,
    communicate_with_provider/3]).
%%% API - generic function
-export([communicate/3]).
-export([ensure_connected/1]).

-type message() :: #client_message{} | #server_message{}.
-type generic_message() :: tuple().
%%-type options() :: map().
-type options() ::  #{wait_for_ans => boolean(), % default: false
                    repeats => non_neg_integer() | infinity, % default: 2
                    error_on_empty_pool => boolean(), % default: true
                    ensure_connected => boolean(), % default: false
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
    send_to_provider(Msg, Ref, #{ignore_send_errors => false}).

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
stream_to_provider(#client_message{} = Msg, Ref, StmId) ->
    communicate(Msg, Ref, #{error_on_empty_pool => false,
        ensure_connected => true, stream => {true, StmId}});
stream_to_provider(Msg, Ref, StmId) ->
    send_to_provider(#client_message{message_body = Msg}, Ref, StmId).

%%--------------------------------------------------------------------
%% @doc
%% Communicates with provider and waits for answer.
%% @end
%%--------------------------------------------------------------------
-spec communicate_with_provider(generic_message(), ref()) ->
    sync_answer().
communicate_with_provider(Msg, Ref) ->
    communicate_with_provider(Msg, Ref, wait_for_ans).

%%--------------------------------------------------------------------
%% @doc
%% Communicates with provider and waits for answer.
%% @end
%%--------------------------------------------------------------------
-spec communicate_with_provider(generic_message(), ref(), wait_for_ans | pid()) ->
    sync_answer().
communicate_with_provider(#client_message{} = Msg, Ref, Recipent) ->
    Options = case Recipent of
        wait_for_ans -> #{error_on_empty_pool => false, ensure_connected => true,
            wait_for_ans => true};
        _ ->
            #{error_on_empty_pool => false, ensure_connected => true,
                use_msg_id => {true, Recipent}}
    end,
    Options2 = case Msg of
        #client_message{message_stream = #message_stream{stream_id = StmId}}
            when is_integer(StmId) -> Options#{stream => {true, StmId}};
        _ -> Options
    end,
    communicate(Msg, Ref, Options2);
communicate_with_provider(Msg, Ref, Async) ->
    communicate_with_provider(#client_message{message_body = Msg}, Ref, Async).

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
    communicate_loop(Msg, Ref, Options2, maps:get(repeats, Options2, 2)).

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
    non_neg_integer() | infinity) -> answer().
communicate_loop(Msg, Ref, Options, 1) ->
    send(Msg, Ref, Options);
communicate_loop(Msg, Ref, Options, Retry) ->
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
-spec retry(message(), ref(), options(), non_neg_integer() | infinity) ->
    answer().
retry(Msg, Ref, Options, Retry) ->
    timer:sleep(?SEND_RETRY_DELAY),
    case Retry of
        infinity -> communicate_loop(Msg, Ref, Options, Retry);
        _ -> communicate_loop(Msg, Ref, Options, Retry - 1)
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
        true -> ensure_connected(Ref);
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Ensures that there is at least one outgoing connection for given session.
%% @end
%%--------------------------------------------------------------------
-spec ensure_connected(session:id() | pid()) ->
    ok | no_return().
ensure_connected(Conn) when is_pid(Conn) ->
    ok;
ensure_connected(SessId) ->
    case session_connections:get_random_connection(SessId, true) of
        {error, _} ->
            ProviderId = case session:get(SessId) of
                {ok, #document{value = #session{proxy_via = ProxyVia}}} when is_binary(
                    ProxyVia) ->
                    ProxyVia;
                _ ->
                    session_utils:session_id_to_provider_id(SessId)
            end,

            case oneprovider:get_id() of
                ProviderId ->
                    ?warning("Provider attempted to connect to itself, skipping connection."),
                    erlang:error(connection_loop_detected);
                _ ->
                    ok
            end,

            {ok, Domain} = provider_logic:get_domain(ProviderId),
            Hosts = case provider_logic:resolve_ips(ProviderId) of
                {ok, IPs} -> [list_to_binary(inet:ntoa(IP)) || IP <- IPs];
                _ -> [Domain]
            end,
            lists:foreach(
                fun(Host) ->
                    Port = https_listener:port(),
                    critical_section:run([?MODULE, ProviderId, SessId], fun() ->
                        % check once more to prevent races
                        case session_connections:get_random_connection(SessId, true) of
                            {error, _} ->
                                outgoing_connection:start(ProviderId, SessId,
                                    Domain, Host, Port, ranch_ssl, timer:seconds(5));
                            _ ->
                                ensure_connected(SessId)
                        end
                    end)
                end, Hosts),
            ok;
        {ok, Pid} ->
            case utils:process_info(Pid, initial_call) of
                undefined ->
                    ok = session_connections:remove_connection(SessId, Pid),
                    ensure_connected(SessId);
                _ ->
                    ok
            end
    end.