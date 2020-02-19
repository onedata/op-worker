%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model manages access and identity tokens used by provider to perform
%%% operations in Onezone and prove its identity. The tokens are confined and
%%% then cached for some time for better performance.
%%%   * access token - provider's root access token is read from database and
%%%     confined with TTL each time the cache expires
%%%   * identity token - a new temporary identity token is created each time
%%%     the cache expires
%%% @end
%%%-------------------------------------------------------------------
-module(provider_auth).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([save/2, delete/0]).
-export([get_provider_id/0, is_registered/0]).
-export([clear_provider_id_cache/0]).
-export([get_access_token/0, get_identity_token/0, get_identity_token_for_consumer/1]).
-export([get_root_token_file_path/0]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type id() :: binary().
-type record() :: #provider_auth{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([id/0, record/0, doc/0]).

-define(CTX, #{
    model => ?MODULE
}).
-define(PROVIDER_ID_CACHE_KEY, provider_id_cache).

-define(PROVIDER_AUTH_KEY, <<"provider_auth">>).
-define(TOKEN_TTL, application:get_env(?APP_NAME, provider_token_ttl_sec, 900)).
% Tokens from cache with lower TTL will not be used
% (they might expire before they are consumed), a new one will be generated.
-define(MIN_TTL_FROM_CACHE, 15).

-define(NOW(), provider_logic:zone_time_seconds()).

-define(FILE_COMMENT,
    <<"This file holds the Oneprovider root token "
    "carrying its identity and full authorization. "
    "It can be used to authorize operations in Onezone's "
    "REST API on behalf of the Oneprovider when sent in the "
    "\"X-Auth-Token\" or \"Authorization: Bearer\" header. "
    "The root token is highly confidential and must be "
    "kept secret.">>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Stores provider's id and its authorization root token.
%% @end
%%--------------------------------------------------------------------
-spec save(ProviderId :: od_provider:id(), tokens:serialized()) ->
    ok.
save(ProviderId, RootToken) ->
    {ok, _} = datastore_model:save(?CTX, #document{
        key = ?PROVIDER_AUTH_KEY,
        value = #provider_auth{
            provider_id = ProviderId,
            root_token = RootToken
        }
    }),
    simple_cache:put(?PROVIDER_ID_CACHE_KEY, ProviderId),
    write_to_file(ProviderId, RootToken).


%%--------------------------------------------------------------------
%% @doc
%% Returns provider Id, or ?ERROR_UNREGISTERED_ONEPROVIDER if it is not yet
%% registered. Upon success, the ProviderId is cached in env variable to be
%% accessible quickly.
%% @end
%%--------------------------------------------------------------------
-spec get_provider_id() -> {ok, od_provider:id()} | {error, term()}.
get_provider_id() ->
    simple_cache:get(?PROVIDER_ID_CACHE_KEY, fun() ->
        case datastore_model:get(?CTX, ?PROVIDER_AUTH_KEY) of
            {error, not_found} ->
                ?ERROR_UNREGISTERED_ONEPROVIDER;
            {error, _} = Error ->
                Error;
            {ok, #document{value = #provider_auth{provider_id = Id}}} ->
                {true, Id}
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% Invalidates provider Id cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_provider_id_cache() -> ok.
clear_provider_id_cache() ->
    simple_cache:clear(?PROVIDER_ID_CACHE_KEY).


%%--------------------------------------------------------------------
%% @doc
%% Predicate saying if this provider is registered in Onezone.
%% @end
%%--------------------------------------------------------------------
-spec is_registered() -> boolean().
is_registered() ->
    case get_provider_id() of
        {ok, _ProviderId} -> true;
        ?ERROR_UNREGISTERED_ONEPROVIDER -> false;
        {error, _} = Error -> error(Error)
    end.


-spec get_access_token() -> {ok, tokens:serialized()} | {error, term()}.
get_access_token() ->
    get_token(access).


-spec get_identity_token() -> {ok, tokens:serialized()} | {error, term()}.
get_identity_token() ->
    get_token(identity).


%%--------------------------------------------------------------------
%% @doc
%% Returns identity token for this provider usable only by specified
%% consumer. The token can be used solely to verify this provider's
%% identity and carries no authorization. The token is confined with
%% TTL for security.
%% @end
%%--------------------------------------------------------------------
-spec get_identity_token_for_consumer(aai:consumer_spec()) ->
    {ok, tokens:serialized()} | {error, term()}.
get_identity_token_for_consumer(Consumer) ->
    {ok, Token} = get_identity_token(),
    {ok, tokens:confine(Token, #cv_consumer{whitelist = [Consumer]})}.


%%--------------------------------------------------------------------
%% @doc
%% Returns absolute path to file where provider root token
%% is saved.
%% @end
%%--------------------------------------------------------------------
-spec get_root_token_file_path() -> string().
get_root_token_file_path() ->
    {ok, ProviderRootMacaroonFile} = application:get_env(?APP_NAME,
        root_token_path),
    filename:absname(ProviderRootMacaroonFile).


%%--------------------------------------------------------------------
%% @doc
%% Deletes provider's identity from database.
%% Does NOT remove file storing the Oneprovider root token,
%% which is left for recovery purposes.
%% @end
%%--------------------------------------------------------------------
-spec delete() -> ok | {error, term()}.
delete() ->
    ok = datastore_model:delete(?CTX, ?PROVIDER_AUTH_KEY),
    {ok, ClusterNodes} = node_manager:get_cluster_nodes(),
    rpc:multicall(ClusterNodes, ?MODULE, clear_provider_id_cache, []),
    ok.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    3.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(V) when V < 3 ->
    % Versions 1 and 2 are the same, but upgrade is triggered to force overwrite
    % of the root token file, which has changed.
    {record, [
        {provider_id, string},
        {root_macaroon, string},
        {cached_auth_macaroon, {integer, string}},
        {cached_identity_macaroon, {integer, string}}
    ]};
get_record_struct(3) ->
    % rename the occurrences of macaroon -> token
    {record, [
        {provider_id, string},
        {root_token, string},
        {cached_auth_token, {integer, string}},
        {cached_identity_token, {integer, string}}
    ]}.


%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, ProviderAuth) ->
    % Versions 1 and 2 are the same, but upgrade is triggered to force overwrite
    % of the root token file, which has changed.
    {provider_auth, ProviderId, RootToken, _, _} = ProviderAuth,
    write_to_file(ProviderId, RootToken),
    {2, ProviderAuth};
upgrade_record(2, ProviderAuth) ->
    % rename the occurrences of macaroon -> token
    {provider_auth, ProviderId, RootToken, _, _} = ProviderAuth,
    % file format is also changed to use 'token' rather than 'macaroon'
    write_to_file(ProviderId, RootToken),
    {3, #provider_auth{provider_id = ProviderId, root_token = RootToken}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stores provider identity in a file on all nodes.
%% @end
%%--------------------------------------------------------------------
-spec write_to_file(od_provider:id(), tokens:serialized()) -> ok.
write_to_file(ProviderId, RootToken) ->
    ProviderRootTokenFile = get_root_token_file_path(),
    Map = #{<<"_comment">> => ?FILE_COMMENT,
        <<"provider_id">> => ProviderId, <<"root_token">> => RootToken},
    Formatted = json_utils:encode(Map, [pretty]),

    {ok, Nodes} = node_manager:get_cluster_nodes(),
    {Results, BadNodes} = rpc:multicall(Nodes, file, write_file,
        [ProviderRootTokenFile, Formatted]),
    case lists:filter(fun(Result) -> Result /= ok end, Results ++ BadNodes) of
        [] -> ok;
        Errors ->
            ?alert("Errors when writing provider root token to file: ~p", [Errors]),
            ok
    end.


%% @private
-spec get_token(access | identity) -> {ok, tokens:serialized()} | {error, term()}.
get_token(Type) ->
    case datastore_model:get(?CTX, ?PROVIDER_AUTH_KEY) of
        {error, not_found} ->
            ?ERROR_UNREGISTERED_ONEPROVIDER;
        {error, _} = Error ->
            Error;
        {ok, #document{value = ProviderAuth}} ->
            {ValidUntil, CachedToken} = get_cached_token(Type, ProviderAuth),
            Now = ?NOW(),
            case ValidUntil - Now > ?MIN_TTL_FROM_CACHE of
                true ->
                    {ok, CachedToken};
                false ->
                    Token = case Type of
                        access ->
                            ProviderAuth#provider_auth.root_token;
                        identity ->
                            {ok, IdentityToken} = token_logic:create_identity_token(Now + ?TOKEN_TTL),
                            IdentityToken
                    end,
                    ConfinedToken = tokens:confine(Token, caveats_for_token(Type)),
                    cache_token(Type, ConfinedToken),
                    {ok, ConfinedToken}
            end
    end.


%% @private
-spec get_cached_token(access | identity, record()) ->
    {ValidUntil :: time_utils:seconds(), tokens:serialized()}.
get_cached_token(access, ProviderAuth) ->
    ProviderAuth#provider_auth.cached_access_token;
get_cached_token(identity, ProviderAuth) ->
    ProviderAuth#provider_auth.cached_identity_token.


%% @private
-spec cache_token(access | identity, tokens:serialized()) -> ok.
cache_token(Type, Token) ->
    {ok, _} = datastore_model:update(?CTX, ?PROVIDER_AUTH_KEY, fun(ProviderAuth) ->
        CacheValue = {?NOW() + ?TOKEN_TTL, Token},
        {ok, case Type of
            access ->
                ProviderAuth#provider_auth{cached_access_token = CacheValue};
            identity ->
                ProviderAuth#provider_auth{cached_identity_token = CacheValue}
        end}
    end),
    ok.


-spec caveats_for_token(access | identity) -> [caveats:caveat()].
caveats_for_token(access) -> [
    #cv_time{valid_until = ?NOW() + ?TOKEN_TTL}
];
caveats_for_token(identity) -> [
].
