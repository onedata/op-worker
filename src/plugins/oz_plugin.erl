%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements oz_plugin_behaviour in order
%%% to customize connection settings to Onezone.
%%% @end
%%%-------------------------------------------------------------------
-module(oz_plugin).
-author("Krzysztof Trzepla").

-behaviour(oz_plugin_behaviour).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/handshake_messages.hrl").

%% oz_plugin_behaviour API
-export([get_oz_url/0, get_oz_rest_port/0, get_oz_rest_api_prefix/0, get_oz_rest_endpoint/1]).
-export([get_cacerts_dir/0]).
-export([auth_to_rest_client/1]).

%%%===================================================================
%%% oz_plugin_behaviour API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Should return a Onezone URL.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_url() -> string().
get_oz_url() ->
    Domain = oneprovider:get_oz_domain(),
    "https://" ++ unicode:characters_to_list(Domain).

%%--------------------------------------------------------------------
%% @doc
%% Should return OZ REST port.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_rest_port() -> integer().
get_oz_rest_port() ->
    op_worker:get_env(oz_rest_port).

%%--------------------------------------------------------------------
%% @doc
%% @doc Should return OZ REST API prefix - for example /api/v3/onezone.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_rest_api_prefix() -> string().
get_oz_rest_api_prefix() ->
    op_worker:get_env(oz_rest_api_prefix).

%%--------------------------------------------------------------------
%% @doc
%% @doc Should return OZ REST endpoint, ended with given Path.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_rest_endpoint(string() | binary()) -> binary().
get_oz_rest_endpoint(Path) ->
    str_utils:format_bin("~ts:~B~ts~ts", [
        get_oz_url(),
        get_oz_rest_port(),
        get_oz_rest_api_prefix(),
        Path
    ]).

%%--------------------------------------------------------------------
%% @doc
%% Should return the path to CA certs directory.
%% @end
%%--------------------------------------------------------------------
-spec get_cacerts_dir() -> file:name_all().
get_cacerts_dir() ->
    op_worker:get_env(cacerts_dir).

%%--------------------------------------------------------------------
%% @doc
%% This callback is used to convert Auth term, which is transparent to ctool,
%% into one of possible authorization methods. Thanks to this, the code
%% using OZ API can always use its specific Auth terms and they are converted
%% when request is done.
%% @end
%%--------------------------------------------------------------------
-spec auth_to_rest_client(Auth :: term()) ->
    {user, token, tokens:serialized()} |
    {provider, tokens:serialized()} |
    none.
auth_to_rest_client(none) ->
    none;

auth_to_rest_client(provider) ->
    {ok, ProviderAccessToken} = provider_auth:acquire_access_token(),
    {provider, ProviderAccessToken};

auth_to_rest_client(?ROOT_SESS_ID) ->
    auth_to_rest_client(provider);

auth_to_rest_client(?GUEST_SESS_ID) ->
    none;

auth_to_rest_client(SessId) when is_binary(SessId) ->
    {ok, #document{
        value = #session{
            credentials = Credentials,
            type = Type
        }}} = session:get(SessId),
    case Type of
        provider_outgoing ->
            auth_to_rest_client(provider);
        provider_incoming ->
            auth_to_rest_client(provider);
        _ ->
            auth_to_rest_client(Credentials)
    end;

auth_to_rest_client(TokenCredentials) ->
    {user, token, auth_manager:get_access_token(TokenCredentials)}.
