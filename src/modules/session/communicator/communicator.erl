%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides communication API between remote client and server.
%%% @end
%%%-------------------------------------------------------------------
-module(communicator).
-author("Krzysztof Trzepla").

-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").

%% API
-export([send_to_client/2, send_to_client/3, communicate/3]).

%%%===================================================================
%%% API - convenience functions
%%%===================================================================

send_to_client(Msg, Ref) ->
    ?MODULE:send_to_client(Msg, Ref, #{}).

send_to_client(#server_message{} = Msg, Ref, Options) ->
    communicate(Msg, Ref, Options);
send_to_client(Msg, Ref, Options) ->
    send_to_client(#server_message{message_body = Msg}, Ref, Options).


%%%===================================================================
%%% API - generic function
%%%===================================================================

communicate(Msg, Ref, Options) ->
    Options2 = case maps:get(wait_for_ans, Options, false) of
        true -> Options#{use_msg_id => {true, self()}};
        _ -> Options
    end,
    communicate_loop(Msg, Ref, Options2, maps:get(repeats, Options2, 2)).

communicate_loop(Msg, Ref, Options, 1) ->
    send(Msg, Ref, Options);
communicate_loop(Msg, Ref, Options, Retry) ->
    case send(Msg, Ref, Options) of
        ok -> ok;
        {ok, MsgId} -> {ok, MsgId};
        {error, empty_connection_pool} ->
            case maps:get(error_on_empty_pool, Options, true) of
                true ->
                    {error, empty_connection_pool};
                _ ->
                    ensure_connected(Ref),
                    retry(Msg, Ref, Options, Retry)
            end;
        {error, _} ->
            retry(Msg, Ref, Options, Retry)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

retry(Msg, Ref, Options, Retry) ->
    timer:sleep(?SEND_RETRY_DELAY),
    case Retry of
        infinity -> communicate_loop(Msg, Ref, Options, Retry);
        _ -> communicate_loop(Msg, Ref, Options, Retry - 1)
    end.

send(Msg, Ref, Options) ->
    {Msg2, ReturnMsgID} = complete_msg(Msg, Options),

    SendAns = case maps:get(stream, Options, false) of
        {true, StmId} -> sequencer:send_message(Msg2, StmId, Ref);
        _ -> connection:send(Msg2, Ref)
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

complete_msg(Msg, Options) ->
    case maps:get(use_msg_id, Options, false) of
        {true, Recipient} -> complete_msg_id(Msg, Recipient);
        true -> complete_msg_id(Msg, undefined);
        _ -> {Msg, false}
    end.

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
%% Receives reply from other provider
%% @end
%%--------------------------------------------------------------------
%%- spec receive_message(MsgId :: #message_id{}) ->
%%    {ok, #server_message{}} | {error, timeout} | {error, Reason :: term()}.
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