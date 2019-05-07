%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Various connection utility function.
%%% @end
%%%-------------------------------------------------------------------
-module(connection_utils).
-author("Tomasz Lichon").

-include("http/gui_paths.hrl").
-include("global_definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([maybe_create_proxied_session/2]).
-export([
    protocol_upgrade_request/1,
    process_protocol_upgrade_request/1,
    verify_protocol_upgrade_response/1
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates proxy session if requested by peer.
%% @end
%%--------------------------------------------------------------------
-spec maybe_create_proxied_session(od_provider:id(), #client_message{}) ->
    ok | {error, term()}.
maybe_create_proxied_session(ProviderId, #client_message{
    effective_session_id = EffSessionId,
    effective_session_auth = Auth
}) when EffSessionId =/= undefined ->
    Res = session_manager:reuse_or_create_proxied_session(
        EffSessionId, ProviderId, Auth, fuse
    ),
    case Res of
        {ok, _} -> ok;
        Error -> Error
    end;
maybe_create_proxied_session(_, _) ->
    ok.


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
