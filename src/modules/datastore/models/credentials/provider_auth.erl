%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model holds the authorization token used by provider to perform
%%% operations in onezone. The token is never used in bare form (which gives
%%% full authorization for infinite time - until it is revoked). Rather than
%%% that, the token is confined to short TTL before use.
%%% @end
%%%-------------------------------------------------------------------
-module(provider_auth).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("ctool/include/auth/onedata_macaroons.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([save/2, delete/0]).
-export([get_provider_id/0, is_registered/0]).
-export([clear_provider_id_cache/0]).
-export([get_auth_macaroon/0, get_identity_macaroon/0]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).

-type id() :: binary().
-type record() :: #provider_auth{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([id/0, record/0, doc/0]).

-define(CTX, #{
    model => ?MODULE
}).
-define(PROVIDER_ID_CACHE_KEY, provider_id_cache).

-define(PROVIDER_AUTH_KEY, <<"provider_auth">>).
-define(MACAROON_TTL, application:get_env(
    ?APP_NAME, provider_macaroon_ttl_sec, 900
)).
% Macaroons from cache with lower TTL will not be used
% (they might expire before they are consumed), a new one will be generated.
-define(MIN_TTL_FROM_CACHE, 15).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Stores provider's id and its authorization root macaroon.
%% @end
%%--------------------------------------------------------------------
-spec save(ProviderId :: od_provider:id(), Macaroon :: binary()) ->
    ok | {error, term()}.
save(ProviderId, Macaroon) ->
    {ok, _} = datastore_model:save(?CTX, #document{
        key = ?PROVIDER_AUTH_KEY,
        value = #provider_auth{
            provider_id = ProviderId,
            root_macaroon = Macaroon
        }
    }),
    application:set_env(?APP_NAME, ?PROVIDER_ID_CACHE_KEY, ProviderId),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Returns provider Id, or ?ERROR_UNREGISTERED_PROVIDER if it is not yet
%% registered. Upon success, the ProviderId is cached in env variable to be
%% accessible quickly.
%% @end
%%--------------------------------------------------------------------
-spec get_provider_id() -> od_provider:id() | {error, term()}.
get_provider_id() ->
    case application:get_env(?APP_NAME, ?PROVIDER_ID_CACHE_KEY) of
        {ok, ProviderId} ->
            ProviderId;
        _ ->
            case datastore_model:get(?CTX, ?PROVIDER_AUTH_KEY) of
                {error, not_found} ->
                    ?ERROR_UNREGISTERED_PROVIDER;
                {error, _} = Error ->
                    Error;
                {ok, #document{value = #provider_auth{provider_id = Id}}} ->
                    application:set_env(?APP_NAME, ?PROVIDER_ID_CACHE_KEY, Id),
                    Id
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Invalidates provider Id cache.
%% @end
%%--------------------------------------------------------------------
-spec clear_provider_id_cache() -> ok.
clear_provider_id_cache() ->
    application:unset_env(?APP_NAME, ?PROVIDER_ID_CACHE_KEY).


%%--------------------------------------------------------------------
%% @doc
%% Predicate saying if this provider is registered in Onezone.
%% @end
%%--------------------------------------------------------------------
-spec is_registered() -> boolean().
is_registered() ->
    case get_provider_id() of
        {error, _} -> false;
        _ProviderId -> true
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns authorization macaroon for this provider. The macaroon is confined
%% with TTL for security.
%% @end
%%--------------------------------------------------------------------
-spec get_auth_macaroon() -> {ok, Macaroon :: binary()} | {error, term()}.
get_auth_macaroon() ->
    get_macaroon(auth).


%%--------------------------------------------------------------------
%% @doc
%% Returns identity macaroon for this provider. The macaroon can be used solely
%% to verify provider's identity and carries no authorization. It can be safely
%% exposed to public view. The macaroon is confined with TTL for security.
%% @end
%%--------------------------------------------------------------------
-spec get_identity_macaroon() -> {ok, Macaroon :: binary()} | {error, term()}.
get_identity_macaroon() ->
    get_macaroon(identity).


%%--------------------------------------------------------------------
%% @doc
%% Deletes provider's identity from database.
%% @end
%%--------------------------------------------------------------------
-spec delete() -> ok | {error, term()}.
delete() ->
    ok = datastore_model:delete(?CTX, ?PROVIDER_AUTH_KEY),
    {ok, NodesIPs} = gen_server2:call({global, ?CLUSTER_MANAGER}, get_nodes),
    ClusterNodes = proplists:get_keys(NodesIPs),
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
    1.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {provider_id, string},
        {root_macaroon, string},
        {cached_auth_macaroon, {integer, string}},
        {cached_identity_macaroon, {integer, string}}
    ]}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_macaroon(Type :: auth | identity) -> {ok, Macaroon :: binary()} | {error, term()}.
get_macaroon(Type) ->
    case datastore_model:get(?CTX, ?PROVIDER_AUTH_KEY) of
        {error, not_found} ->
            ?ERROR_UNREGISTERED_PROVIDER;
        {error, _} = Error ->
            Error;
        {ok, #document{value = ProviderAuth}} ->
            {ExpirationTime, CachedMacaroon} = get_cached_macaroon(Type, ProviderAuth),
            TTL = ExpirationTime - time_utils:cluster_time_seconds(),
            case TTL > ?MIN_TTL_FROM_CACHE of
                true ->
                    {ok, CachedMacaroon};
                false ->
                    RootMacaroon = ProviderAuth#provider_auth.root_macaroon,
                    NewMacaroon = add_caveats(RootMacaroon, caveats_for_macaroon(Type)),
                    cache_macaroon(Type, NewMacaroon),
                    {ok, NewMacaroon}
            end
    end.



-spec get_cached_macaroon(Type :: auth | identity, record()) ->
    {ExpirationTime :: non_neg_integer(), Macaroon :: binary()}.
get_cached_macaroon(auth, ProviderAuth) ->
    ProviderAuth#provider_auth.cached_auth_macaroon;
get_cached_macaroon(identity, ProviderAuth) ->
    ProviderAuth#provider_auth.cached_identity_macaroon.


-spec cache_macaroon(Type :: auth | identity, Macaroon :: binary()) -> ok.
cache_macaroon(Type, Macaroon) ->
    ExpirationTime = time_utils:cluster_time_seconds() + ?MACAROON_TTL,
    {ok, _} = datastore_model:update(?CTX, ?PROVIDER_AUTH_KEY, fun(ProviderAuth) ->
        CacheValue = {ExpirationTime, Macaroon},
        {ok, case Type of
            auth ->
                ProviderAuth#provider_auth{cached_auth_macaroon = CacheValue};
            identity ->
                ProviderAuth#provider_auth{cached_identity_macaroon = CacheValue}
        end}
    end),
    ok.


-spec caveats_for_macaroon(Type :: auth | identity) -> [onedata_macaroons:caveat()].
caveats_for_macaroon(auth) -> [
    ?TIME_CAVEAT(provider_logic:zone_time_seconds(), ?MACAROON_TTL)
];
caveats_for_macaroon(identity) -> [
    ?AUTHORIZATION_NONE_CAVEAT,
    ?TIME_CAVEAT(provider_logic:zone_time_seconds(), ?MACAROON_TTL)
].


-spec add_caveats(Macaroon :: binary(), [onedata_macaroons:caveat()]) ->
    NewMacaroon :: binary().
add_caveats(MacaroonBin, Caveats) ->
    {ok, Macaroon} = onedata_macaroons:deserialize(MacaroonBin),
    NewMacaroon = lists:foldl(fun(Caveat, MacaroonAcc) ->
        onedata_macaroons:add_caveat(MacaroonAcc, Caveat)
    end, Macaroon, Caveats),
    {ok, NewMacaroonBin} = onedata_macaroons:serialize(NewMacaroon),
    NewMacaroonBin.



