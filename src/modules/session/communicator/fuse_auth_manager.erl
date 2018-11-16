%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Client authentication library.
%%% @end
%%%-------------------------------------------------------------------
-module(fuse_auth_manager).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([handle_handshake/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles client handshake request
%% @end
%%--------------------------------------------------------------------
-spec handle_handshake(#client_handshake_request{}, inet:ip_address()) ->
    {od_user:id(), session:id()} | no_return().
handle_handshake(#client_handshake_request{session_id = SessId, auth = Auth, version = Version}, IpAddress)
    when is_binary(SessId) andalso is_record(Auth, macaroon_auth) ->

    assert_client_compatibility(Version, IpAddress),
    {ok, #document{
        value = Iden = #user_identity{user_id = UserId}
    }} = user_identity:get_or_fetch(Auth),
    {ok, _} = session_manager:reuse_or_create_fuse_session(SessId, Iden, Auth, self()),
    {UserId, SessId}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Check if client is of compatible version.
%% @end
%%--------------------------------------------------------------------
-spec assert_client_compatibility(string() | binary(), inet:ip_address()) -> ok | no_return().
assert_client_compatibility(ClientVersion, IpAddress) when is_binary(ClientVersion) ->
    assert_client_compatibility(binary_to_list(ClientVersion), IpAddress);
assert_client_compatibility(ClientVersion, IpAddress) ->
    {ok, CompatibleClientVersions} = application:get_env(
        ?APP_NAME, compatible_oc_versions
    ),
    % Client sends full build version (e.g. 17.06.0-rc9-aiosufshx) so instead
    % of matching whole build version we check only prefix
    Pred = fun(Ver) -> lists:prefix(Ver, ClientVersion) end,
    case lists:any(Pred, CompatibleClientVersions) of
        true ->
            ok;
        false ->
            ?debug("Discarding connection from oneclient @ ~s because of "
            "incompatible version (~s). Version must be one of: ~p", [
                inet_parse:ntoa(IpAddress), ClientVersion, CompatibleClientVersions
            ]),
            throw(incompatible_client_version)
    end.
