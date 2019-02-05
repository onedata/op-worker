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
-module(protocol_utils).
-author("Tomasz Lichon").

-include("http/gui_paths.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([fill_proxy_info/2]).
-export([
    protocol_upgrade_request/1,
    process_protocol_upgrade_request/1,
    verify_protocol_upgrade_response/1
]).

%%--------------------------------------------------------------------
%% @doc
%% Fills message with info about session to which it should be proxied.
%% @end
%%--------------------------------------------------------------------
-spec fill_proxy_info(#server_message{} | #client_message{}, session:id()) ->
    #server_message{} | #client_message{}.
fill_proxy_info(Msg, SessionId) ->
    {ok, #document{
        value = #session{proxy_via = ProxyVia}
    }} = session:get(SessionId),

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
%% Returns ok if request contains proper upgrade header for client protocol.
%% Otherwise informs about necessity of protocol upgrade.
%% @end
%%--------------------------------------------------------------------
-spec process_protocol_upgrade_request(cowboy_req:req()) ->
    ok | {error, update_required}.
process_protocol_upgrade_request(Req) ->
    ConnTokens = cowboy_req:parse_header(<<"connection">>, Req, []),
    case lists:member(<<"upgrade">>, ConnTokens) of
        false ->
            {error, upgrade_required};
        true ->
            case cowboy_req:parse_header(<<"upgrade">>, Req, []) of
                [<<?CLIENT_PROTOCOL_UPGRADE_NAME>>] ->
                    ok;
                _ ->
                    {error, upgrade_required}
            end
    end.

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
    case string:lowercase(Bin) =:= string:lowercase(First) of
        true -> true;
        false -> has_member_case_insensitive(Bin, Rest)
    end.
