%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles client and provider requests.
%%% @end
%%%-------------------------------------------------------------------
-module(connection).
-author("Tomasz Lichon").

-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("http/gui_paths.hrl").
-include("timeouts.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([send/2, send_async/2]).
-export([protocol_upgrade_request/1, verify_protocol_upgrade_response/1]).

-type ref() :: pid() | session:id().
-export_type([ref/0]).

%%--------------------------------------------------------------------
%% @doc
%% Synchronously sends a message to a client or server.
%% @end
%%--------------------------------------------------------------------
-spec send(#server_message{} | #client_message{}, ref()) ->
    ok | {error, Reason :: term()} | {exit, Reason :: term()}.
send(Msg, Ref) when is_pid(Ref) ->
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
send(Msg, SessionId) ->
    MsgWithProxyInfo = fill_proxy_info(Msg, SessionId),
    case session:get_random_connection(SessionId) of
        {ok, Con} -> send(MsgWithProxyInfo, Con);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously sends a message to a client or server.
%% @end
%%--------------------------------------------------------------------
-spec send_async(#server_message{} | #client_message{}, ref()) ->
    ok | {error, Reason :: term()}.
send_async(Msg, Ref) when is_pid(Ref) ->
    Ref ! {send_async, Msg},
    ok;
send_async(Msg, Ref) ->
    case session:get_random_connection(Ref) of
        {ok, Con} -> send_async(Msg, Con);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Fills message with info about session to which it should be proxied.
%% @end
%%--------------------------------------------------------------------
-spec fill_proxy_info(#server_message{} | #client_message{}, session:id()) ->
    #server_message{} | #client_message{}.
fill_proxy_info(Msg, SessionId) ->
    {ok, #document{value = #session{proxy_via = ProxyVia}}} = session:get(SessionId),
    case {Msg, is_binary(ProxyVia)} of
        {#server_message{proxy_session_id = undefined}, true} ->
            Msg#server_message{proxy_session_id = SessionId};
        {#client_message{proxy_session_id = undefined}, true} ->
            Msg#client_message{proxy_session_id = SessionId};
        _ ->
            Msg
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns HTTP request data used to upgrade protocol in inter-provider
%% connections.
%% @end
%%--------------------------------------------------------------------
-spec protocol_upgrade_request(Hostname :: binary()) -> binary().
protocol_upgrade_request(Hostname) -> <<
    "GET ", ?CLIENT_PROTOCOL_PATH, " HTTP/1.1\r\n"
    "Host: ", Hostname/binary, "\r\n"
    "Connection: Upgrade\r\n"
    "Upgrade: ", ?CLIENT_PROTOCOL_UPGRADE_NAME, "\r\n"
    "\r\n"
>>.

%%--------------------------------------------------------------------
%% @doc
%% Verifies given protocol upgrade response - returns true if the server
%% responded with valid answer saying that the protocol was upgraded.
%% @end
%%--------------------------------------------------------------------
-spec verify_protocol_upgrade_response(Response :: binary()) -> boolean().
verify_protocol_upgrade_response(Response) ->
    try
        Lines = binary:split(Response, <<"\r\n">>, [global, trim_all]),
        has_member_case_insensitive(<<"HTTP/1.1 101 Switching Protocols">>, Lines) andalso
            has_member_case_insensitive(<<"Connection: Upgrade">>, Lines) andalso
            has_member_case_insensitive(<<"Upgrade: ", ?CLIENT_PROTOCOL_UPGRADE_NAME>>, Lines)
    catch _:_ ->
        false
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec has_member_case_insensitive(binary(), [binary()]) -> boolean().
has_member_case_insensitive(_Bin, []) ->
    false;
has_member_case_insensitive(Bin, [First | Rest]) ->
    case string:to_lower(binary_to_list(Bin)) =:= string:to_lower(binary_to_list(First)) of
        true -> true;
        false -> has_member_case_insensitive(Bin, Rest)
    end.
