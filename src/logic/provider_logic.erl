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

-include("middleware/middleware.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/rtransfer/rtransfer.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("http/gui_paths.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/onedata.hrl").

-export([get/0, get/1, get/2, get_protected_data/2, get_protected_data/3]).
-export([force_fetch/0]).
-export([to_printable/1]).
-export([update/1, update/2]).
-export([get_name/0, get_name/1, get_name/2]).
-export([get_spaces/0, get_spaces/1, get_spaces/2]).
-export([get_eff_users/0]).
-export([get_storages/0, get_storages/1]).
-export([has_storage/1]).
-export([has_eff_user/1, has_eff_user/2, has_eff_user/3]).
-export([supports_space/1, supports_space/2, supports_space/3]).
-export([get_support_size/1]).
-export([map_idp_user_to_onedata/2, map_idp_group_to_onedata/2]).
-export([get_domain/0, get_domain/1, get_domain/2]).
-export([set_domain/1, set_delegated_subdomain/1]).
-export([is_subdomain_delegated/0, get_subdomain_delegation_ips/0]).
-export([update_subdomain_delegation_ips/0]).
-export([get_nodes/1, get_nodes/2]).
-export([get_rtransfer_port/1]).
-export([set_txt_record/3, remove_txt_record/1]).
-export([zone_get_offline_access_idps/0]).
-export([get_service_configuration/1, get_peer_version/1]).
-export([provider_connection_ssl_opts/1]).
-export([assert_provider_compatibility/1]).
-export([verify_provider_identity/1]).


-define(SERVICE_CONFIGURATION_CACHE_TTL_SEC, 15).
-define(PROVIDER_NODES_CACHE_TTL_SEC,
    op_worker:get_env(provider_nodes_cache_ttl_seconds, 600) % 10 minutes
).

-compile([{no_auto_import, [get/0]}]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider doc of this provider.
%% @end
%%--------------------------------------------------------------------
-spec get() ->
    {ok, od_provider:doc()} | errors:error().
get() ->
    get(?ROOT_SESS_ID, ?SELF).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider doc by given ProviderId using current provider's auth.
%% @end
%%--------------------------------------------------------------------
-spec get(od_provider:id()) ->
    {ok, od_provider:doc()} | errors:error().
get(ProviderId) ->
    get(?ROOT_SESS_ID, ProviderId).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider doc by given ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get(gs_client_worker:client(), od_provider:id()) ->
    {ok, od_provider:doc()} | errors:error().
get(SessionId, ?SELF) ->
    case oneprovider:get_id_or_undefined() of
        undefined -> ?ERROR_UNREGISTERED_ONEPROVIDER;
        ProviderId -> get(SessionId, ProviderId)
    end;
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
    {ok, od_provider:doc()} | errors:error().
get_protected_data(SessionId, ?SELF) ->
    case oneprovider:get_id_or_undefined() of
        undefined -> ?ERROR_UNREGISTERED_ONEPROVIDER;
        ProviderId -> get_protected_data(SessionId, ProviderId)
    end;
get_protected_data(SessionId, ProviderId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_provider, id = ProviderId, aspect = instance, scope = protected},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider doc restricted to protected data by given ProviderId
%% using specified AuthHint.
%% @end
%%--------------------------------------------------------------------
-spec get_protected_data(gs_client_worker:client(), od_provider:id(),
    gs_protocol:auth_hint()) -> {ok, od_provider:doc()} | errors:error().
get_protected_data(SessionId, ProviderId, AuthHint) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_provider, id = ProviderId, aspect = instance, scope = protected},
        auth_hint = AuthHint,
        subscribe = true
    }).


-spec force_fetch() -> {ok, od_provider:doc()} | errors:error().
force_fetch() ->
    gs_client_worker:force_fetch_entity(#gri{type = od_provider, id = oneprovider:get_id(), aspect = instance}).


-spec to_printable(od_provider:id()) -> string().
to_printable(ProviderId) ->
    case provider_logic:get_name(ProviderId) of
        {ok, Name} -> str_utils:format("'~ts' (~ts)", [Name, ProviderId]);
        _ -> str_utils:format("'~ts' (name unknown)", [ProviderId])
    end.


%%--------------------------------------------------------------------
%% @doc
%% Updates parameters of this provider using current provider's auth.
%% Supports updating name, latitude, longitude and adminEmail.
%% @end
%%--------------------------------------------------------------------
-spec update(Data :: #{binary() => term()}) -> ok | errors:error().
update(Data) ->
    update(?ROOT_SESS_ID, Data).

%%--------------------------------------------------------------------
%% @doc
%% Updates parameters of this provider.
%% Supports updating name, latitude, longitude and adminEmail.
%% @end
%%--------------------------------------------------------------------
-spec update(SessionId :: gs_client_worker:client(),
    Data :: #{binary() => term()}) -> ok | errors:error().
update(SessionId, Data) ->
    Result = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = update, data = Data,
        gri = #gri{type = od_provider, id = ?SELF, aspect = instance}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        provider_logic:force_fetch()
    end).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider name of this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_name() -> {ok, od_provider:name()} | errors:error().
get_name() ->
    get_name(?ROOT_SESS_ID, ?SELF).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider name by given ProviderId using current provider's auth.
%% @end
%%--------------------------------------------------------------------
-spec get_name(od_provider:id()) -> {ok, od_provider:name()} | errors:error().
get_name(ProviderId) ->
    get_name(?ROOT_SESS_ID, ProviderId).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider name by given ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get_name(gs_client_worker:client(), od_provider:id()) ->
    {ok, od_provider:name()} | errors:error().
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
-spec get_spaces() -> {ok, [od_space:id()]} | errors:error().
get_spaces() ->
    get_spaces(?ROOT_SESS_ID, ?SELF).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves spaces of provider by given ProviderId using current provider's auth.
%% @end
%%--------------------------------------------------------------------
-spec get_spaces(od_provider:id()) -> {ok, [od_space:id()]} | errors:error().
get_spaces(ProviderId) ->
    get_spaces(?ROOT_SESS_ID, ProviderId).


-spec get_eff_users() -> {ok, [od_user:id()]} | errors:error().
get_eff_users() ->
    case get() of
        {ok, #document{value = #od_provider{eff_users = EffUsers}}} -> {ok, EffUsers};
        {error, _} = Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves spaces of provider by given ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get_spaces(gs_client_worker:client(), od_provider:id()) ->
    {ok, [od_space:id()]} | errors:error().
get_spaces(SessionId, ProviderId) ->
    case get(SessionId, ProviderId) of
        {ok, #document{value = #od_provider{eff_spaces = Spaces}}} ->
            {ok, maps:keys(Spaces)};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves storage_ids of this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_storages() -> {ok, [storage:id()]} | errors:error().
get_storages() ->
    get_storages(?SELF).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves storage_ids of provider by given ProviderId using current provider's auth.
%% @end
%%--------------------------------------------------------------------
-spec get_storages(od_provider:id()) -> {ok, [storage:id()]} | errors:error().
get_storages(ProviderId) ->
    case get(?ROOT_SESS_ID, ProviderId) of
        {ok, #document{value = #od_provider{storages = Storages}}} ->
            {ok, Storages};
        {error, _} = Error ->
            Error
    end.

-spec has_storage(storage:id()) -> boolean().
has_storage(StorageId) ->
    case get_storages() of
        {ok, StorageIds} -> lists:member(StorageId, StorageIds);
        _ -> false
    end.


-spec has_eff_user(od_user:id()) -> boolean().
has_eff_user(UserId) ->
    has_eff_user(?ROOT_SESS_ID, ?SELF, UserId).

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
    supports_space(?ROOT_SESS_ID, ?SELF, SpaceId).


-spec supports_space(od_provider:doc(), od_space:id()) ->
    boolean().
supports_space(#document{value = #od_provider{eff_spaces = Spaces}}, SpaceId) ->
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


-spec get_support_size(od_space:id()) -> {ok, integer()} | errors:error().
get_support_size(SpaceId) ->
    case get(?ROOT_SESS_ID, ?SELF) of
        {ok, #document{value = #od_provider{eff_spaces = #{SpaceId := SupportSize}}}} ->
            {ok, SupportSize};
        {ok, #document{value = #od_provider{}}} ->
            ?ERROR_NOT_FOUND;
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves domain of this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_domain() ->
    {ok, od_provider:domain()} | errors:error().
get_domain() ->
    get_domain(?ROOT_SESS_ID, ?SELF).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider domain by given ProviderId using current provider's auth.
%% @end
%%--------------------------------------------------------------------
-spec get_domain(od_provider:id()) ->
    {ok, od_provider:domain()} | errors:error().
get_domain(ProviderId) ->
    get_domain(?ROOT_SESS_ID, ProviderId).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider domain by given ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get_domain(gs_client_worker:client(), od_provider:id()) ->
    {ok, od_provider:domain()} | errors:error().
get_domain(SessionId, ProviderId) ->
    case get_protected_data(SessionId, ProviderId) of
        {ok, #document{value = #od_provider{domain = Domain}}} ->
            {ok, Domain};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Resolves the node hostnames of given provider. Tries to resolve the IP
%% addresses and check if they are reachable (may not be true due to reverse proxy etc.)
%% @end
%%--------------------------------------------------------------------
-spec get_nodes(od_provider:id()) -> {ok, [binary()]} | {error, term()}.
get_nodes(ProviderId) ->
    node_cache:acquire({provider_nodes, ProviderId}, fun() ->
        % Call by ?MODULE to allow for CT testing
        case ?MODULE:get_domain(?ROOT_SESS_ID, ProviderId) of
            {ok, Domain} ->
                {ok, get_nodes(ProviderId, Domain), ?PROVIDER_NODES_CACHE_TTL_SEC};
            {error, _} = Error ->
                Error
        end
    end).

-spec get_nodes(od_provider:id(), od_provider:domain()) -> [binary()].
get_nodes(ProviderId, Domain) ->
    case inet:parse_ipv4_address(binary_to_list(Domain)) of
        {ok, _} ->
            ?warning("Provider ~ts is using an IP address instead of domain", [to_printable(ProviderId)]),
            ?info("Resolved 1 node for connection to provider ~ts: ~ts", [to_printable(ProviderId), Domain]),
            [Domain];
        {error, einval} ->
            {ok, IPsAtoms} = inet:getaddrs(binary_to_list(Domain), inet),
            IPs = [list_to_binary(inet:ntoa(IP)) || IP <- IPsAtoms],
            IpsString = str_utils:join_binary(IPs, <<", ">>),
            ProviderConfig = get_service_configuration({oneprovider, Domain, Domain}),
            ConfigMatches = fun(IP) ->
                ProviderConfig =:= (catch get_service_configuration({oneprovider, Domain, IP}))
            end,
            case lists:all(ConfigMatches, IPs) of
                true ->
                    ?info("Resolved ~B node(s) for connection to provider ~ts: ~ts", [
                        length(IPs), to_printable(ProviderId), IpsString
                    ]),
                    IPs;
                false ->
                    ?info(
                        "Falling back to domain '~ts' for connection to provider ~ts as "
                        "IP addresses failed the connectivity check (~ts)", [
                            Domain, to_printable(ProviderId), IpsString
                        ]),
                    [Domain]
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Resolves rtransfer port of given provider.
%% @end
%%--------------------------------------------------------------------
-spec get_rtransfer_port(ProviderId :: binary()) -> {ok, inet:port_number()}.
get_rtransfer_port(ProviderId) ->
    ResolvePort = fun() ->
        {ok, Domain} = provider_logic:get_domain(ProviderId),
        Port = case get_service_configuration({oneprovider, Domain, Domain}) of
            {ok, #{<<"rtransferPort">> := RtransferPort}} ->
                RtransferPort;
            _ ->
                ?info("Cannot resolve rtransfer port for provider ~ts, defaulting to ~tp",
                    [to_printable(ProviderId), ?RTRANSFER_PORT]),
                ?RTRANSFER_PORT
        end,
        {ok, Port, ?PROVIDER_NODES_CACHE_TTL_SEC}
    end,
    node_cache:acquire({rtransfer_port, ProviderId}, ResolvePort).


%%--------------------------------------------------------------------
%% @doc
%% Checks if this provider uses subdomain delegation.
%% If yes, returns the subdomain.
%% @end
%%--------------------------------------------------------------------
-spec is_subdomain_delegated() ->
    {true, binary()} | false | errors:error().
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
-spec set_delegated_subdomain(binary()) -> ok | errors:error().
set_delegated_subdomain(Subdomain) ->
    IPs = node_manager:get_cluster_ips(),
    case set_subdomain_delegation(Subdomain, IPs) of
        ok ->
            provider_logic:force_fetch(),
            ok;
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
        case is_subdomain_delegated() of
            {true, Subdomain} ->
                IPs = node_manager:get_cluster_ips(),
                ok = set_subdomain_delegation(Subdomain, IPs);
            false ->
                ok
        end
    catch Type:Message ->
        ?error("Error updating provider IPs: ~tp:~tp", [Type, Message]),
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
    {true, [inet:ip4_address()]} | false | errors:error().
get_subdomain_delegation_ips() ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_provider, id = ?SELF,
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
-spec set_domain(binary()) -> ok | errors:error().
set_domain(Domain) ->
    Data = #{
        <<"subdomainDelegation">> => false,
        <<"domain">> => Domain},
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update, data = Data,
        gri = #gri{type = od_provider, id = ?SELF,
            aspect = domain_config}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        provider_logic:force_fetch()
    end).


%%--------------------------------------------------------------------
%% @doc
%% Sets TXT type dns record in onezone DNS.
%% @end
%%--------------------------------------------------------------------
-spec set_txt_record(Name :: binary(), Content :: binary(),
    TTL :: non_neg_integer() | undefined) -> ok | no_return().
set_txt_record(Name, Content, TTL) ->
    Data = #{<<"content">> => Content},
    Data2 = case TTL of
        Number when is_integer(Number) -> Data#{<<"ttl">> => TTL};
        _ -> Data
    end,
    ok = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create, data = Data2,
        gri = #gri{type = od_provider, id = ?SELF,
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
        gri = #gri{type = od_provider, id = ?SELF,
            aspect = {dns_txt_record, Name}}
    }).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Turns on subdomain delegation for this provider
%% and sets its subdomain and ips.
%% @end
%%--------------------------------------------------------------------
-spec set_subdomain_delegation(binary(), [inet:ip4_address() | binary()]) ->
    ok | errors:error().
set_subdomain_delegation(Subdomain, IPs) ->
    IPBinaries = lists:map(fun
        (IP) when is_binary(IP) -> IP;
        (IP) when is_tuple(IP) -> list_to_binary(inet:ntoa(IP))
    end, IPs),

    Data = #{
        <<"subdomainDelegation">> => true,
        <<"subdomain">> => Subdomain,
        <<"ipList">> => IPBinaries},
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update, data = Data,
        gri = #gri{type = od_provider, id = ?SELF, aspect = domain_config}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        provider_logic:force_fetch()
    end).


%%--------------------------------------------------------------------
%% @doc
%% Calls OZ to learn what's the onedata id of given user from certain IdP.
%% @end
%%--------------------------------------------------------------------
-spec map_idp_user_to_onedata(Idp :: binary(), IdpGroupId :: binary()) ->
    {ok, od_user:id()} | errors:error().
map_idp_user_to_onedata(Idp, IdpUserId) ->
    ?CREATE_RETURN_DATA(gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_provider, id = undefined, aspect = map_idp_user},
        data = #{
            <<"idp">> => Idp,
            <<"userId">> => IdpUserId
        }
    })).


%%--------------------------------------------------------------------
%% @doc
%% Calls OZ to learn what's the onedata id of given group from certain IdP.
%% @end
%%--------------------------------------------------------------------
-spec map_idp_group_to_onedata(Idp :: binary(), IdpGroupId :: binary()) ->
    {ok, od_group:id()} | errors:error().
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
%% Returns the list of IdPs in Onezone that support offline access.
%% NOTE: The Onezone response has the following format:
%%  <<"supportedIdPs">> => [
%%      #{
%%          <<"id">> => <<"google">>,
%%          <<"offlineAccess">> => false
%%      },
%%      #{
%%          <<"id">> => <<"github">>,
%%          <<"offlineAccess">> => true
%%      }
%%  ]
%% @end
%%--------------------------------------------------------------------
-spec zone_get_offline_access_idps() -> {ok, [IdP :: binary()]} | {error, term()}.
zone_get_offline_access_idps() ->
    case get_service_configuration(onezone) of
        {ok, Map} ->
            SupportedIdPs = maps:get(<<"supportedIdPs">>, Map, []),
            {ok, lists:filtermap(fun(IdPConfig) ->
                case maps:get(<<"offlineAccess">>, IdPConfig, false) of
                    false -> false;
                    true -> {true, maps:get(<<"id">>, IdPConfig)}
                end
            end, SupportedIdPs)};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Contacts given provider and retrieves his identity token, and then
%% verifies it in Onezone.
%% @end
%%--------------------------------------------------------------------
-spec verify_provider_identity(od_provider:id()) -> ok | {error, term()}.
verify_provider_identity(TheirProviderId) ->
    try
        OurIdentityToken = ?check(provider_auth:acquire_identity_token_for_consumer(
            ?SUB(?ONEPROVIDER, TheirProviderId)
        )),
        {ok, Domain} = get_domain(TheirProviderId),
        Headers = tokens:access_token_header(OurIdentityToken),
        URL = str_utils:format_bin("https://~ts~ts", [Domain, ?IDENTITY_TOKEN_PATH]),
        SslOpts = [{ssl_options, provider_connection_ssl_opts(Domain)}],
        case http_client:get(URL, Headers, <<>>, SslOpts) of
            {ok, 200, _, TheirIdentityToken} ->
                verify_provider_identity(TheirProviderId, TheirIdentityToken);
            {ok, Code, _, _} ->
                {error, {bad_http_code, Code}};
            {error, _} = Error1 ->
                Error1
        end
    catch
        throw:{error, _} = Error2 ->
            Error2;
        Class:Reason:Stacktrace ->
            ?debug_exception("Failed to verify provider ~ts identity", [
                provider_logic:to_printable(TheirProviderId)
            ], Class, Reason, Stacktrace),
            {error, Reason}
    end.


-spec get_service_configuration(onezone | {oneprovider, Domain :: binary(), Hostname :: binary()}) ->
    {ok, json_utils:json_term()} |
    {error, {bad_response, Code :: integer(), Body :: binary()}} |
    {error, term()}.
get_service_configuration(ServiceSpec) ->
    node_cache:acquire({service_configuration, ServiceSpec}, fun() ->
        case fetch_service_configuration(ServiceSpec) of
            {ok, Configuration} ->
                {ok, Configuration, ?SERVICE_CONFIGURATION_CACHE_TTL_SEC};
            {error, _} = Error ->
                Error
        end
    end).


-spec get_peer_version(onezone | {oneprovider, Domain :: binary(), Hostname :: binary()}) ->
    {ok, PeerVersion :: binary()} |
    {error, {bad_response, Code :: integer(), Body :: binary()}} | {error, term()}.
get_peer_version(Service) ->
    case get_service_configuration(Service) of
        {ok, JsonMap} ->
            PeerVersion = maps:get(<<"version">>, JsonMap, <<"unknown">>),
            {ok, PeerVersion};
        {error, Error} ->
            {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns ssl options for connecting to a provider under given domain.
%% @end
%%--------------------------------------------------------------------
-spec provider_connection_ssl_opts(od_provider:domain()) -> [http_client:ssl_opt()].
provider_connection_ssl_opts(Domain) ->
    CaCerts = oneprovider:trusted_ca_certs(),
    SecureFlag = op_worker:get_env(interprovider_connections_security, true),
    [{cacerts, CaCerts}, {secure, SecureFlag}, {hostname, Domain}].


%%--------------------------------------------------------------------
%% @doc
%% Assert that peer provider is of compatible version.
%% @end
%%--------------------------------------------------------------------
-spec assert_provider_compatibility(Domain :: binary()) -> ok | no_return().
assert_provider_compatibility(Domain) ->
    case get_peer_version({oneprovider, Domain, Domain}) of
        {ok, RemoteOpVersion} ->
            Resolver = compatibility:build_resolver(consistent_hashing:get_all_nodes(), oneprovider:trusted_ca_certs()),
            OpVersion = op_worker:get_release_version(),
            case compatibility:check_products_compatibility(
                Resolver, ?ONEPROVIDER, RemoteOpVersion, ?ONEPROVIDER, OpVersion
            ) of
                true ->
                    ok;
                {false, RemoteCompOpVersions} ->
                    throw({
                        incompatible_peer_op_version,
                        RemoteOpVersion,
                        RemoteCompOpVersions
                    });
                {error, Error} ->
                    error(Error)
            end;
        {error, {bad_response, Code, _ResponseBody}} ->
            throw({cannot_check_peer_op_version, Code});
        {error, Error} ->
            error(Error)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec fetch_service_configuration(onezone | {oneprovider, Domain :: binary(), Hostname :: binary()}) ->
    {ok, json_utils:json_term()} |
    {error, {bad_response, Code :: integer(), Body :: binary()}} |
    {error, term()}.
fetch_service_configuration(onezone) ->
    URL = oneprovider:get_oz_url(?ZONE_CONFIGURATION_PATH),
    DeprecatedURL = oneprovider:get_oz_url(?DEPRECATED_ZONE_CONFIGURATION_PATH),
    SslOpts = [{cacerts, oneprovider:trusted_ca_certs()}],
    fetch_configuration(URL, DeprecatedURL, SslOpts);

fetch_service_configuration({oneprovider, Domain, Hostname}) ->
    URL = str_utils:format_bin("https://~ts~ts", [
        Hostname, ?PROVIDER_CONFIGURATION_PATH
    ]),
    DeprecatedURL = str_utils:format_bin("https://~ts~ts", [
        Hostname, ?DEPRECATED_PROVIDER_CONFIGURATION_PATH
    ]),
    SslOpts = provider_connection_ssl_opts(Domain),
    fetch_configuration(URL, DeprecatedURL, SslOpts).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Attempts to fetch the configuration page.
%% If the resource with default URL is not found, the older path is attempted.
%% @end
%%--------------------------------------------------------------------
-spec fetch_configuration(URL, DeprecatedURL :: URL, [http_client:ssl_opt()]) ->
    {ok, json_utils:json_term()} |
    {error, {bad_response, Code :: integer(), Body :: binary()}} |
    {error, term()}
    when URL :: binary().
fetch_configuration(URL, DeprecatedURL, SslOpts) ->
    case http_get_configuration(URL, SslOpts) of
        {error, {bad_response, 404, _}} ->
            case http_get_configuration(DeprecatedURL, SslOpts) of
                {error, Error} -> {error, Error};
                Success -> Success
            end;
        {error, Error} -> {error, Error};
        Success -> Success
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Attempts to fetch and parse the configuration page under given URL.
%% @end
%%--------------------------------------------------------------------
-spec http_get_configuration(
    URL :: string() | binary(), SslOpts :: [http_client:ssl_opt()]
) ->
    {ok, json_utils:json_term()} |
    {error, {bad_response, Code :: integer(), Body :: binary()}} |
    {error, term()}.
http_get_configuration(URL, SslOpts) ->
    case http_client:get(URL, #{}, <<>>, [{ssl_options, SslOpts}]) of
        {ok, 200, _, JsonBody} ->
            {ok, json_utils:decode(JsonBody)};
        {ok, Code, _, Body} ->
            {error, {bad_response, Code, Body}};
        {error, Error} ->
            {error, Error}
    end.


%% @private
-spec verify_provider_identity(od_provider:id(), IdentityToken :: binary()) ->
    ok | errors:error().
verify_provider_identity(ProviderId, IdentityToken) ->
    case token_logic:verify_provider_identity_token(IdentityToken) of
        {ok, ?SUB(?ONEPROVIDER, ProviderId)} ->
            ok;
        {ok, _} ->
            ?ERROR_TOKEN_SUBJECT_INVALID;
        {error, _} = Error ->
            Error
    end.
