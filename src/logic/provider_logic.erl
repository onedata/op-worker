%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_provider records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by Onezone.
%%% NOTE: This is the only valid way to interact with od_provider records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(provider_logic).
-author("Lukasz Opiola").

-include("modules/fslogic/fslogic_common.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("http/http_common.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/logging.hrl").

-export([get/0, get/1, get/2, get_protected_data/2]).
-export([get_as_map/0]).
-export([get_name/0, get_name/1, get_name/2]).
-export([get_spaces/0, get_spaces/1, get_spaces/2]).
-export([has_eff_user/2, has_eff_user/3]).
-export([supports_space/1, supports_space/2, supports_space/3]).
-export([map_idp_group_to_onedata/2]).
-export([get_domain/0, get_domain/1, get_domain/2]).
-export([set_domain/1, set_delegated_subdomain/1]).
-export([is_subdomain_delegated/0, get_subdomain_delegation_ips/0]).
-export([update_subdomain_delegation_ips/0]).
-export([resolve_ips/1, resolve_ips/2]).
-export([set_txt_record/2, remove_txt_record/1]).
-export([zone_time_seconds/0]).
-export([assert_zone_compatibility/0]).
-export([verify_provider_identity/1, verify_provider_identity/2]).
-export([verify_provider_nonce/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider doc of this provider.
%% @end
%%--------------------------------------------------------------------
-spec get() ->
    {ok, od_provider:doc()} | gs_protocol:error().
get() ->
    get(?ROOT_SESS_ID, oneprovider:get_id_or_undefined()).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider doc by given ProviderId using current provider's auth.
%% @end
%%--------------------------------------------------------------------
-spec get(od_provider:id()) ->
    {ok, od_provider:doc()} | gs_protocol:error().
get(ProviderId) ->
    get(?ROOT_SESS_ID, ProviderId).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider doc by given ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get(gs_client_worker:client(), od_provider:id()) ->
    {ok, od_provider:doc()} | gs_protocol:error().
get(SessionId, ProviderId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_provider, id = ProviderId, aspect = instance},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider doc restricted to protected data by given ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get_protected_data(gs_client_worker:client(), od_provider:id()) ->
    {ok, od_provider:doc()} | gs_protocol:error().
get_protected_data(SessionId, ProviderId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_provider, id = ProviderId, aspect = instance, scope = protected},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Returns current provider's data in a map.
%% Useful for RPC calls from onepanel where od_provider record is not defined.
%% @end
%%--------------------------------------------------------------------
-spec get_as_map() -> map().
get_as_map() ->
    {ok, #document{key = ProviderId, value = ProviderRecord}} = ?MODULE:get(),
    #od_provider{
        name = Name,
        subdomain_delegation = SubdomainDelegation,
        domain = Domain,
        subdomain = Subdomain,
        longitude = Longitude,
        latitude = Latitude
    } = ProviderRecord,
    #{
        id => ProviderId,
        name => Name,
        subdomain_delegation => SubdomainDelegation,
        domain => Domain,
        subdomain => Subdomain,
        longitude => Longitude,
        latitude => Latitude
    }.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider name of this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_name() -> {ok, od_provider:name()} | gs_protocol:error().
get_name() ->
    get_name(?ROOT_SESS_ID, oneprovider:get_id_or_undefined()).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider name by given ProviderId using current provider's auth.
%% @end
%%--------------------------------------------------------------------
-spec get_name(od_provider:id()) -> {ok, od_provider:name()} | gs_protocol:error().
get_name(ProviderId) ->
    get_name(?ROOT_SESS_ID, ProviderId).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider name by given ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get_name(gs_client_worker:client(), od_provider:id()) ->
    {ok, od_provider:name()} | gs_protocol:error().
get_name(SessionId, ProviderId) ->
    case get_protected_data(SessionId, ProviderId) of
        {ok, #document{value = #od_provider{name = Name}}} ->
            {ok, Name};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves spaces of this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_spaces() -> {ok, [od_space:id()]} | gs_protocol:error().
get_spaces() ->
    get_spaces(?ROOT_SESS_ID, oneprovider:get_id_or_undefined()).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves spaces of provider by given ProviderId using current provider's auth.
%% @end
%%--------------------------------------------------------------------
-spec get_spaces(od_provider:id()) -> {ok, [od_space:id()]} | gs_protocol:error().
get_spaces(ProviderId) ->
    get_spaces(?ROOT_SESS_ID, ProviderId).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves spaces of provider by given ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get_spaces(gs_client_worker:client(), od_provider:id()) ->
    {ok, [od_space:id()]} | gs_protocol:error().
get_spaces(SessionId, ProviderId) ->
    case get(SessionId, ProviderId) of
        {ok, #document{value = #od_provider{spaces = Spaces}}} ->
            {ok, maps:keys(Spaces)};
        {error, _} = Error ->
            Error
    end.


-spec has_eff_user(od_provider:doc(), od_user:id()) -> boolean().
has_eff_user(#document{value = #od_provider{eff_users = EffUsers}}, UserId) ->
    lists:member(UserId, EffUsers).


-spec has_eff_user(gs_client_worker:client(), od_provider:id(), od_user:id()) ->
    boolean().
has_eff_user(SessionId, ProviderId, UserId) ->
    case get(SessionId, ProviderId) of
        {ok, ProviderDoc = #document{}} ->
            has_eff_user(ProviderDoc, UserId);
        _ ->
            false
    end.


-spec supports_space(od_space:id()) -> boolean().
supports_space(SpaceId) ->
    supports_space(?ROOT_SESS_ID, oneprovider:get_id_or_undefined(), SpaceId).


-spec supports_space(od_provider:doc(), od_space:id()) -> boolean().
supports_space(#document{value = #od_provider{spaces = Spaces}}, SpaceId) ->
    maps:is_key(SpaceId, Spaces).


-spec supports_space(gs_client_worker:client(), od_provider:id(), od_space:id()) ->
    boolean().
supports_space(SessionId, ProviderId, SpaceId) ->
    case get(SessionId, ProviderId) of
        {ok, ProviderDoc = #document{}} ->
            supports_space(ProviderDoc, SpaceId);
        _ ->
            false
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves domain of this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_domain() ->
    {ok, od_provider:doc()} | gs_protocol:error().
get_domain() ->
    get_domain(?ROOT_SESS_ID, oneprovider:get_id_or_undefined()).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider domain by given ProviderId using current provider's auth.
%% @end
%%--------------------------------------------------------------------
-spec get_domain(od_provider:id()) ->
    {ok, od_provider:doc()} | gs_protocol:error().
get_domain(ProviderId) ->
    get_domain(?ROOT_SESS_ID, ProviderId).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider domain by given ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get_domain(gs_client_worker:client(), od_provider:id()) ->
    {ok, od_provider:domain()} | gs_protocol:error().
get_domain(SessionId, ProviderId) ->
    case get_protected_data(SessionId, ProviderId) of
        {ok, #document{value = #od_provider{domain = Domain}}} ->
            {ok, Domain};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Resolves IPs of given provider via DNS using current provider's auth.
%% @end
%%--------------------------------------------------------------------
-spec resolve_ips(od_provider:id()) -> {ok, inet:ip4_address()} | {error, term()}.
resolve_ips(ProviderId) ->
    resolve_ips(?ROOT_SESS_ID, ProviderId).


%%--------------------------------------------------------------------
%% @doc
%% Resolves IPs of given provider via DNS.
%% @end
%%--------------------------------------------------------------------
-spec resolve_ips(gs_client_worker:client(), od_provider:id()) ->
    {ok, inet:ip4_address()} | {error, term()}.
resolve_ips(SessionId, ProviderId) ->
    case get_domain(SessionId, ProviderId) of
        {ok, Domain} ->
            inet:getaddrs(binary_to_list(Domain), inet);
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks if this provider uses subdomain delegation.
%% If yes, returns the subdomain.
%% @end
%%--------------------------------------------------------------------
-spec is_subdomain_delegated() ->
    {true, binary()} | false | gs_protocol:error().
is_subdomain_delegated() ->
    case ?MODULE:get() of
        {ok, #document{value = #od_provider{
            subdomain_delegation = Delegation,
            subdomain = Subdomain}}} ->
            case Delegation of
                true ->
                    {true, Subdomain};
                false ->
                    false
            end;
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sets onezone subdomain pointing to this provider.
%% @end
%%--------------------------------------------------------------------
-spec set_delegated_subdomain(binary()) ->
    ok | {error, subdomain_exists} | gs_protocol:error().
set_delegated_subdomain(Subdomain) ->
    {ok, NodesIPs} = node_manager:get_cluster_nodes_ips(),
    {_, IPTuples} = lists:unzip(NodesIPs),
    case set_subdomain_delegation(Subdomain, IPTuples) of
        ok ->
            gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id_or_undefined()),
            ok;
        ?ERROR_BAD_VALUE_IDENTIFIER_OCCUPIED(<<"subdomain">>) ->
            {error, subdomain_exists};
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% If subdomain delegation is on, updates ips of this provider in dns state.
%% @end
%%--------------------------------------------------------------------
-spec update_subdomain_delegation_ips() -> ok | error.
update_subdomain_delegation_ips() ->
    try
        {ok, NodesIPs} = node_manager:get_cluster_nodes_ips(),
        {_, IPTuples} = lists:unzip(NodesIPs),
        case is_subdomain_delegated() of
            {true, Subdomain} ->
                ok = set_subdomain_delegation(Subdomain, IPTuples);
            false ->
                ok
        end
    catch Type:Message ->
        ?error("Error updating provider IPs: ~p:~p", [Type, Message]),
        error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves IPs of this provider as known to Onezone DNS.
%% Returns the atom 'false' if subdomain delegation is not enabled for this
%% provider.
%% @end
%%--------------------------------------------------------------------
-spec get_subdomain_delegation_ips() ->
    {true, [inet:ip4_address()]} | false | gs_protocol:error().
get_subdomain_delegation_ips() ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_provider, id = oneprovider:get_id_or_undefined(),
            aspect = domain_config}
    }),
    case Result of
        {ok, #{<<"subdomainDelegation">> := SubdomainDelegation} = Data} ->
            case SubdomainDelegation of
                true ->
                    #{<<"ipList">> := IPs} = Data,
                    {true, IPs};
                false ->
                    false
            end;
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sets provider domain that is NOT a subdomain of onezone domain.
%% @end
%%--------------------------------------------------------------------
-spec set_domain(binary()) -> ok | gs_protocol:error().
set_domain(Domain) ->
    Data = #{
        <<"subdomainDelegation">> => false,
        <<"domain">> => Domain},
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update, data = Data,
        gri = #gri{type = od_provider, id = oneprovider:get_id_or_undefined(),
            aspect = domain_config}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id_or_undefined())
    end).


%%--------------------------------------------------------------------
%% @doc
%% Sets TXT type dns record in onezone DNS.
%% @end
%%--------------------------------------------------------------------
-spec set_txt_record(Name :: binary(), Content :: binary()) -> ok | no_return().
set_txt_record(Name, Content) ->
    Data = #{<<"content">> => Content},
    ok = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create, data = Data,
        gri = #gri{type = od_provider, id = oneprovider:get_id_or_undefined(),
                   aspect = {dns_txt_record, Name}}
    }).


%%--------------------------------------------------------------------
%% @doc
%% Removes TXT type dns record in onezone DNS.
%% @end
%%--------------------------------------------------------------------
-spec remove_txt_record(Name :: binary()) -> ok | no_return().
remove_txt_record(Name) ->
    ok = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_provider, id = oneprovider:get_id_or_undefined(),
            aspect = {dns_txt_record, Name}}
    }).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Turns on subdomain delegation for this provider
%% and sets its subdomain and ips.
%% @end
%%--------------------------------------------------------------------
-spec set_subdomain_delegation(binary(), [inet:ip4_address()]) ->
    ok | gs_protocol:error().
set_subdomain_delegation(Subdomain, IPs) ->
    IPBinaries = [list_to_binary(inet:ntoa(IPTuple)) || IPTuple <- IPs],
    ProviderId = oneprovider:get_id_or_undefined(),
    Data = #{
        <<"subdomainDelegation">> => true,
        <<"subdomain">> => Subdomain,
        <<"ipList">> => IPBinaries},
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update, data = Data,
        gri = #gri{type = od_provider, id = ProviderId, aspect = domain_config}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_provider, ProviderId)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Calls OZ to learn what's the onedata id of given group from certain IdP.
%% @end
%%--------------------------------------------------------------------
-spec map_idp_group_to_onedata(Idp :: binary(), IdpGroupId :: binary()) ->
    {ok, od_group:id()} | gs_protocol:error().
map_idp_group_to_onedata(Idp, IdpGroupId) ->
    ?CREATE_RETURN_DATA(gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_provider, id = undefined, aspect = map_idp_group},
        data = #{
            <<"idp">> => Idp,
            <<"groupId">> => IdpGroupId
        }
    })).


%%--------------------------------------------------------------------
%% @doc
%% Returns current timestamp that is synchronized with the Onezone service.
%% It has the accuracy of 1 second in most cases, but this might be worse under
%% very high load. When evaluated on different providers within the same zone
%% simultaneously, it should yield times at most one second apart from each
%% other.
%% @end
%%--------------------------------------------------------------------
-spec zone_time_seconds() -> non_neg_integer().
zone_time_seconds() ->
    TimeMillis = time_utils:remote_timestamp(zone_time_bias, fun() ->
        Req = #gs_req_graph{
            operation = get,
            gri = #gri{type = od_provider, id = undefined, aspect = current_time}
        },
        case gs_client_worker:request(?ROOT_SESS_ID, Req) of
            {ok, Timestamp} ->
                {ok, Timestamp};
            Error ->
                ?warning("Cannot get zone time due to: ~p", [Error]),
                error
        end
    end),
    TimeMillis div 1000.


%%--------------------------------------------------------------------
%% @doc
%% Check compatibility of Onezone and exit if it is not compatible.
%% @end
%%--------------------------------------------------------------------
-spec assert_zone_compatibility() -> ok | no_return().
assert_zone_compatibility() ->
    URL = oneprovider:get_oz_url() ++ ?zone_version_path,
    {ok, CompatibleVersions} = application:get_env(?APP_NAME, compatible_oz_versions),
    CaCerts = oneprovider:get_ca_certs(),

    case http_client:get(URL, #{}, <<>>, [{ssl_options, [{cacerts, CaCerts}]}]) of
        {ok, 200, _RespHeaders, ResponseBody} ->
            ZoneVersion = binary_to_list(ResponseBody),
            case lists:member(ZoneVersion, CompatibleVersions) of
                true ->
                    ok;
                false ->
                    ?critical("This provider is not compatible with its Onezone "
                    "service. Onezone version: ~s. Compatible versions: ~p. "
                    "The application will be terminated.", [
                        ZoneVersion, CompatibleVersions
                    ]),
                    init:stop()
            end;
        {ok, Code, _RespHeaders, ResponseBody} ->
            ?critical("Failure while checking Onezone version. The application "
            "will be terminated. HTTP response: ~B: ~s", [
                Code, ResponseBody
            ]),
            init:stop();
        {error, Error} ->
            error(Error)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Contacts given provider and retrieves his identity macaroon, and then
%% verifies it in Onezone.
%% @end
%%--------------------------------------------------------------------
-spec verify_provider_identity(od_provider:id()) -> ok | {error, term()}.
verify_provider_identity(ProviderId) ->
    try
        {ok, Domain} = get_domain(ProviderId),
        URL = str_utils:format_bin("https://~s~s", [
            Domain, ?identity_macaroon_path
        ]),

        CaCerts = oneprovider:get_ca_certs(),
        SecureFlag = application:get_env(?APP_NAME, interprovider_connections_security, true),
        Opts = [{ssl_options, [{cacerts, CaCerts}, {secure, SecureFlag}]}],

        case http_client:get(URL, #{}, <<>>, Opts) of
            {ok, 200, _, IdentityMacaroon} ->
                verify_provider_identity(ProviderId, IdentityMacaroon);
            {ok, Code, _, _} ->
                {error, {bad_http_code, Code}};
            {error, _} = Error ->
                Error
        end
    catch _:_ ->
        ?ERROR_UNAUTHORIZED
    end.


%%--------------------------------------------------------------------
%% @doc
%% Verifies given provider in Onezone based on its identity macaroon.
%% @end
%%--------------------------------------------------------------------
-spec verify_provider_identity(od_provider:id(), IdentityMacaroon :: binary()) ->
    ok | gs_protocol:error().
verify_provider_identity(ProviderId, IdentityMacaroon) ->
    ?CREATE_RETURN_OK(gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_provider, id = undefined, aspect = verify_provider_identity},
        data = #{
            <<"providerId">> => ProviderId,
            <<"macaroon">> => IdentityMacaroon
        }
    })).


%%--------------------------------------------------------------------
%% @doc
%% Contacts given provider and verifies if given nonce is valid for the provider
%% (was generated by the provider and did not expire).
%% @end
%%--------------------------------------------------------------------
-spec verify_provider_nonce(od_provider:id(), Nonce :: binary()) -> ok | {error, term()}.
verify_provider_nonce(ProviderId, Nonce) ->
    try
        {ok, Domain} = get_domain(ProviderId),
        URL = str_utils:format_bin("https://~s~s?nonce=~s", [
            Domain, ?nonce_verify_path, Nonce
        ]),

        CaCerts = oneprovider:get_ca_certs(),
        SecureFlag = application:get_env(?APP_NAME, interprovider_connections_security, true),
        Opts = [{ssl_options, [{cacerts, CaCerts}, {secure, SecureFlag}]}],

        case http_client:get(URL, #{}, <<>>, Opts) of
            {ok, 200, _, JSON} ->
                case json_utils:decode_map(JSON) of
                    #{<<"status">> := <<"ok">>} -> ok;
                    _ -> {error, invalid_nonce}
                end;
            {ok, Code, _, _} ->
                {error, {bad_http_code, Code}};
            {error, _} = Error ->
                Error
        end
    catch _:_ ->
        ?ERROR_UNAUTHORIZED
    end.
