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
-include("modules/rtransfer/rtransfer.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("http/gui_paths.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/logging.hrl").

-export([get/0, get/1, get/2, get_protected_data/2]).
-export([get_as_map/0]).
-export([to_string/1]).
-export([update/1, update/2]).
-export([get_name/0, get_name/1, get_name/2]).
-export([get_spaces/0, get_spaces/1, get_spaces/2]).
-export([has_eff_user/1, has_eff_user/2, has_eff_user/3]).
-export([support_space/2, support_space/3]).
-export([update_space_support_size/2]).
-export([supports_space/1, supports_space/2, supports_space/3]).
-export([get_support_size/1]).
-export([map_idp_group_to_onedata/2]).
-export([get_domain/0, get_domain/1, get_domain/2]).
-export([set_domain/1, set_delegated_subdomain/1]).
-export([is_subdomain_delegated/0, get_subdomain_delegation_ips/0]).
-export([update_subdomain_delegation_ips/0]).
-export([get_nodes/1, get_nodes/2]).
-export([get_rtransfer_port/1]).
-export([set_txt_record/3, remove_txt_record/1]).
-export([zone_time_seconds/0]).
-export([zone_get_offline_access_idps/0]).
-export([fetch_compatibility_config/1]).
-export([assert_zone_compatibility/0]).
-export([provider_connection_ssl_opts/1]).
-export([assert_provider_compatibility/3]).
-export([verify_provider_identity/1, verify_provider_identity/2]).
-export([verify_provider_nonce/2]).
-export([is_effective_peer/1]).
-export([start_session_with_peers/2]).
-export([terminate_session_with_peer/1]).

-define(PROVIDER_NODES_CACHE_TTL, application:get_env(?APP_NAME, provider_nodes_cache_ttl, timer:minutes(10))).


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
    get(?ROOT_SESS_ID, ?SELF).

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
get(SessionId, ?SELF) ->
    case oneprovider:get_id_or_undefined() of
        undefined -> ?ERROR_UNREGISTERED_PROVIDER;
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
    {ok, od_provider:doc()} | gs_protocol:error().
get_protected_data(SessionId, ?SELF) ->
    case oneprovider:get_id_or_undefined() of
        undefined -> ?ERROR_UNREGISTERED_PROVIDER;
        ProviderId -> get_protected_data(SessionId, ProviderId)
    end;
get_protected_data(SessionId, ProviderId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_provider, id = ProviderId, aspect = instance, scope = protected},
        subscribe = true
    }).


-spec to_string(od_provider:id()) -> string().
to_string(ProviderId) ->
    case provider_logic:get_name(ProviderId) of
        {ok, Name} -> str_utils:format("'~ts' (~s)", [Name, ProviderId]);
        _ -> str_utils:format("'~s' (name unknown)", [ProviderId])
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns current provider's data in a map.
%% Useful for RPC calls from onepanel where od_provider record is not defined.
%% @end
%%--------------------------------------------------------------------
-spec get_as_map() -> {ok, map()} | gs_protocol:error().
get_as_map() ->
    case ?MODULE:get() of
        {ok, #document{key = Id, value = Record}} ->
            {ok, #{
                id => Id,
                name => Record#od_provider.name,
                admin_email => Record#od_provider.admin_email,
                subdomain_delegation => Record#od_provider.subdomain_delegation,
                domain => Record#od_provider.domain,
                subdomain => Record#od_provider.subdomain,
                longitude => Record#od_provider.longitude,
                latitude => Record#od_provider.latitude
            }};
        Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Updates parameters of this provider using current provider's auth.
%% Supports updating name, latitude, longitude and adminEmail.
%% @end
%%--------------------------------------------------------------------
-spec update(Data :: #{binary() => term()}) -> ok | gs_protocol:error().
update(Data) ->
    update(?ROOT_SESS_ID, Data).

%%--------------------------------------------------------------------
%% @doc
%% Updates parameters of this provider.
%% Supports updating name, latitude, longitude and adminEmail.
%% @end
%%--------------------------------------------------------------------
-spec update(SessionId :: gs_client_worker:client(),
    Data :: #{binary() => term()}) -> ok | gs_protocol:error().
update(SessionId, Data) ->
    Result = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = update, data = Data,
        gri = #gri{type = od_provider, id = ?SELF, aspect = instance}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id())
    end).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider name of this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_name() -> {ok, od_provider:name()} | gs_protocol:error().
get_name() ->
    get_name(?ROOT_SESS_ID, ?SELF).


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
    get_spaces(?ROOT_SESS_ID, ?SELF).


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


%%--------------------------------------------------------------------
%% @doc
%% Supports a space based on support_space_token and support size.
%% @end
%%--------------------------------------------------------------------
-spec support_space(Token :: token:id() | macaroon:macaroon(), SupportSize :: integer()) ->
    {ok, od_space:id()} | gs_protocol:error().
support_space(Token, SupportSize) ->
    support_space(?ROOT_SESS_ID, Token, SupportSize).

-spec support_space(SessionId :: gs_client_worker:client(),
    Token :: binary() | macaroon:macaroon(), SupportSize :: integer()) ->
    {ok, od_space:id()} | gs_protocol:error().
support_space(SessionId, Token, SupportSize) ->
    Data = #{<<"token">> => Token, <<"size">> => SupportSize},
    Result = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_provider, id = ?SELF, aspect = support},
        data = Data
    }),

    ?CREATE_RETURN_ID(?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id())
    end)).


-spec supports_space(od_space:id()) -> boolean().
supports_space(SpaceId) ->
    supports_space(?ROOT_SESS_ID, ?SELF, SpaceId).


-spec supports_space(od_provider:doc(), od_space:id()) ->
    boolean().
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


-spec get_support_size(od_space:id()) -> {ok, integer()} | gs_protocol:error().
get_support_size(SpaceId) ->
    case get(?ROOT_SESS_ID, ?SELF) of
        {ok, #document{value = #od_provider{spaces = #{SpaceId := SupportSize}}}} ->
            {ok, SupportSize};
        {ok, #document{value = #od_provider{}}} ->
            ?ERROR_NOT_FOUND;
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Changes support size of this provider towards given space,
%% given that data stored on this provider is not larger than
%% the intended size.
%% @end
%%--------------------------------------------------------------------
-spec update_space_support_size(SpaceId :: od_space:id(), NewSupportSize :: integer()) ->
    ok | gs_protocol:error().
update_space_support_size(SpaceId, NewSupportSize) ->
    OccupiedSize = space_quota:current_size(SpaceId),
    update_space_support_size(SpaceId, NewSupportSize, OccupiedSize).


%% @private
-spec update_space_support_size(SpaceId :: od_space:id(), NewSupportSize :: integer(),
    CurrentOccupiedSize :: non_neg_integer()) ->
    ok | gs_protocol:error().
update_space_support_size(_SpaceId, NewSupportSize, CurrentOccupiedSize)
    when NewSupportSize < CurrentOccupiedSize ->
    ?ERROR_BAD_VALUE_TOO_LOW(<<"size">>, CurrentOccupiedSize);

update_space_support_size(SpaceId, NewSupportSize, _CurrentOccupiedSize) ->
    Data = #{<<"size">> => NewSupportSize},
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update, data = Data,
        gri = #gri{type = od_provider, id = ?SELF, aspect = {space, SpaceId}}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_space, SpaceId),
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id())
    end).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves domain of this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_domain() ->
    {ok, od_provider:domain()} | gs_protocol:error().
get_domain() ->
    get_domain(?ROOT_SESS_ID, ?SELF).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider domain by given ProviderId using current provider's auth.
%% @end
%%--------------------------------------------------------------------
-spec get_domain(od_provider:id()) ->
    {ok, od_provider:domain()} | gs_protocol:error().
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
%% Resolves the node hostnames of given provider. Tries to resolve the IP
%% addresses and check if they are reachable (may not be true due to reverse proxy etc.)
%% @end
%%--------------------------------------------------------------------
-spec get_nodes(od_provider:id()) -> {ok, [binary()]} | {error, term()}.
get_nodes(ProviderId) ->
    ResolveNodes = fun() ->
        % Call by ?MODULE to allow for CT testing
        case ?MODULE:get_domain(?ROOT_SESS_ID, ProviderId) of
            {ok, Domain} ->
                {true, get_nodes(ProviderId, Domain), ?PROVIDER_NODES_CACHE_TTL};
            {error, _} = Error ->
                Error
        end
    end,
    simple_cache:get({provider_nodes, ProviderId}, ResolveNodes).

-spec get_nodes(od_provider:id(), od_provider:domain()) -> [binary()].
get_nodes(ProviderId, Domain) ->
    case inet:parse_ipv4_address(binary_to_list(Domain)) of
        {ok, _} ->
            ?warning("Provider ~ts is using an IP address instead of domain", [to_string(ProviderId)]),
            ?info("Resolved 1 node for connection to provider ~ts: ~s", [to_string(ProviderId), Domain]),
            [Domain];
        {error, einval} ->
            {ok, IPsAtoms} = inet:getaddrs(binary_to_list(Domain), inet),
            IPs = [list_to_binary(inet:ntoa(IP)) || IP <- IPsAtoms],
            IpsString = str_utils:join_binary(IPs, <<", ">>),
            ProviderConfig = fetch_service_configuration({oneprovider, Domain, Domain}),
            ConfigMatches = fun(IP) ->
                ProviderConfig =:= (catch fetch_service_configuration({oneprovider, Domain, IP}))
            end,
            case lists:all(ConfigMatches, IPs) of
                true ->
                    ?info("Resolved ~B node(s) for connection to provider ~ts: ~s", [
                        length(IPs), to_string(ProviderId), IpsString
                    ]),
                    IPs;
                false ->
                    ?info(
                        "Falling back to domain '~ts' for connection to provider ~ts as "
                        "IP addresses failed the connectivity check (~s)", [
                            Domain, to_string(ProviderId), IpsString
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
        Port = case fetch_service_configuration({oneprovider, Domain, Domain}) of
            {ok, #{<<"rtransferPort">> := RtransferPort}} ->
                RtransferPort;
            _ ->
                ?info("Cannot resolve rtransfer port for provider ~ts, defaulting to ~p", 
                    [to_string(ProviderId), ?RTRANSFER_PORT]),
                ?RTRANSFER_PORT
        end,
        {true, Port, ?PROVIDER_NODES_CACHE_TTL}
    end,
    simple_cache:get({rtransfer_port, ProviderId}, ResolvePort).


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
    IPs = node_manager:get_cluster_ips(),
    case set_subdomain_delegation(Subdomain, IPs) of
        ok ->
            gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id()),
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
-spec set_domain(binary()) -> ok | gs_protocol:error().
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
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id())
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
    ok | gs_protocol:error().
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
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id())
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
            {ok, Timestamp1} ->
                {ok, Timestamp1};
            _ ->
                % Fallback to REST in case GS returned an error
                case oz_providers:get_zone_time(none) of
                    {ok, Timestamp2} ->
                        {ok, Timestamp2};
                    _ ->
                        % Use local time if Onezone is unreachable
                        {ok, time_utils:system_time_millis()}
                end
        end
    end),
    TimeMillis div 1000.


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
    case fetch_service_configuration(onezone) of
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
%% Contacts given provider and retrieves his identity macaroon, and then
%% verifies it in Onezone.
%% @end
%%--------------------------------------------------------------------
-spec verify_provider_identity(od_provider:id()) -> ok | {error, term()}.
verify_provider_identity(ProviderId) ->
    try
        {ok, Domain} = get_domain(ProviderId),
        URL = str_utils:format_bin("https://~s~s", [Domain, ?IDENTITY_MACAROON_PATH]),
        SslOpts = [{ssl_options, provider_connection_ssl_opts(Domain)}],
        case http_client:get(URL, #{}, <<>>, SslOpts) of
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
-spec verify_provider_nonce(od_provider:id(), Nonce :: binary()) ->
    ok | {error, term()}.
verify_provider_nonce(ProviderId, Nonce) ->
    try
        % Call by ?MODULE to allow for CT testing
        {ok, Domain} = ?MODULE:get_domain(ProviderId),
        URL = str_utils:format_bin("https://~s~s?nonce=~s", [Domain, ?NONCE_VERIFY_PATH, Nonce]),
        SslOpts = [{ssl_options, provider_connection_ssl_opts(Domain)}],
        case http_client:get(URL, #{}, <<>>, SslOpts) of
            {ok, 200, _, JSON} ->
                case json_utils:decode(JSON) of
                    #{<<"status">> := <<"ok">>} -> ok;
                    _ -> {error, invalid_nonce}
                end;
            {ok, Code, _, _} ->
                {error, {bad_http_code, Code}};
            {error, _} = Error ->
                Error
        end
    catch Type:Reason ->
        ?debug_stacktrace("Cannot verify nonce for provider ~ts - nonce was '~s' - ~p:~p", [
            to_string(ProviderId), Nonce, Type, Reason
        ]),
        ?ERROR_UNAUTHORIZED
    end.


%%--------------------------------------------------------------------
%% @doc
%% Performs request fetching compatibility information from
%% Onezone or another Oneprovider.
%% @end
%%--------------------------------------------------------------------
-spec fetch_compatibility_config(onezone | {oneprovider, Domain :: binary(), Hostname :: binary()}) ->
    {ok, PeerVersion :: binary(), CompatibleOpVersions :: [binary()]} |
    {error, {bad_response, Code :: integer(), Body :: binary()}} |
    {error, term()}.
fetch_compatibility_config(Service) ->
    case fetch_service_configuration(Service) of
        {ok, JsonMap} ->
            PeerVersion = maps:get(<<"version">>, JsonMap, <<"unknown">>),
            CompatibleOpVersions = maps:get(
                <<"compatibleOneproviderVersions">>, JsonMap, []
            ),
            {ok, PeerVersion, CompatibleOpVersions};
        {error, Error} ->
            {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Check compatibility of Onezone and exit if it is not compatible.
%% @end
%%--------------------------------------------------------------------
-spec assert_zone_compatibility() -> ok | no_return().
assert_zone_compatibility() ->
    case fetch_compatibility_config(onezone) of
        {ok, OzVersion, CompOpVersionsBin} ->
            OpVersion = oneprovider:get_version(),
            CompOzVersions = application:get_env(
                ?APP_NAME, compatible_oz_versions, []
            ),
            CompOzVersionsBin = strings_to_binaries(CompOzVersions),
            case lists:member(OzVersion, CompOzVersionsBin) orelse
                lists:member(OpVersion, CompOpVersionsBin) of
                true ->
                    ok;
                false ->
                    ?critical("This provider is not compatible with its Onezone "
                    "service.~n"
                    "Oneprovider version: ~s, supports zones: ~p~n"
                    "Onezone version: ~s, supports providers: ~p~n"
                    "The application will be terminated.", [
                        OpVersion,
                        CompOzVersions,
                        OzVersion,
                        binaries_to_strings(CompOpVersionsBin)
                    ]),
                    init:stop()
            end;
        {error, {bad_response, Code, ResponseBody}} ->
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
%% Returns ssl options for connecting to a provider under given domain.
%% @end
%%--------------------------------------------------------------------
-spec provider_connection_ssl_opts(od_provider:domain()) -> [http_client:ssl_opt()].
provider_connection_ssl_opts(Domain) ->
    CaCerts = oneprovider:trusted_ca_certs(),
    SecureFlag = application:get_env(?APP_NAME, interprovider_connections_security, true),
    [{cacerts, CaCerts}, {secure, SecureFlag}, {hostname, Domain}].


%%--------------------------------------------------------------------
%% @doc
%% Assert that peer provider is of compatible version.
%% @end
%%--------------------------------------------------------------------
-spec assert_provider_compatibility(od_provider:id(), Domain :: binary(), Hostname :: binary()) ->
    ok | no_return().
assert_provider_compatibility(ProviderId, Domain, Hostname) ->
    case fetch_compatibility_config({oneprovider, Domain, Hostname}) of
        {ok, RemoteOpVersion, RemoteCompOpVersionsBin} ->
            OpVersion = oneprovider:get_version(),
            CompOpVersions = application:get_env(
                ?APP_NAME, compatible_op_versions, []
            ),
            CompOpVersionsBin = strings_to_binaries(CompOpVersions),
            case lists:member(OpVersion, RemoteCompOpVersionsBin) orelse
                lists:member(RemoteOpVersion, CompOpVersionsBin) of
                true ->
                    ok;
                false ->
                    ?error("Discarding connection to provider ~p because of "
                    "incompatible version. ~n"
                    "Local version: ~s, supports providers: ~p~n"
                    "Remote version: ~s, supports providers: ~p~n", [
                        ProviderId,
                        OpVersion,
                        CompOpVersions,
                        RemoteOpVersion,
                        binaries_to_strings(RemoteCompOpVersionsBin)
                    ]),
                    throw(incompatible_peer_op_version)
            end;
        {error, {bad_response, Code, _ResponseBody}} ->
            ?warning("Discarding connection to provider ~p because its "
            "version cannot be determined (HTTP ~b). ~n", [
                ProviderId, Code
            ]),
            throw(cannot_check_peer_op_version);
        {error, Error} ->
            error(Error)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks if specified provider is one of this provider's effective peers.
%% @end
%%--------------------------------------------------------------------
-spec is_effective_peer(od_provider:id()) -> boolean() | gs_protocol:error().
is_effective_peer(ProviderId) ->
    case ?MODULE:get() of
        {ok, #document{value = #od_provider{eff_peers = PeerProviders}}} ->
            lists:member(ProviderId, PeerProviders);
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Reuses or creates outgoing provider session with each peer provider.
%% @end
%%--------------------------------------------------------------------
-spec start_session_with_peers(ThisProviderId :: od_provider:id(),
    [od_provider:id()]) -> ok.
start_session_with_peers(ThisProviderId, PeerProviders) ->
    lists:foreach(fun(PeerProviderId) ->
        case PeerProviderId == ThisProviderId of
            true ->
                ok;
            false ->
                SessionId = session_utils:get_provider_session_id(
                    outgoing, PeerProviderId
                ),
                session_manager:reuse_or_create_outgoing_provider_session(
                    SessionId, #user_identity{provider_id = PeerProviderId}
                )
        end
    end, PeerProviders).


%%--------------------------------------------------------------------
%% @doc
%% Terminates outgoing provider session with specified peer provider.
%% @end
%%--------------------------------------------------------------------
-spec terminate_session_with_peer(od_provider:id()) -> ok | {error, term()}.
terminate_session_with_peer(PeerProviderId) ->
    SessId = session_utils:get_provider_session_id(outgoing, PeerProviderId),
    session_manager:remove_session(SessId).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Attempts to fetch the configuration page of given service.
%% @end
%%--------------------------------------------------------------------
-spec fetch_service_configuration(onezone | {oneprovider, Domain :: binary(), Hostname :: binary()}) ->
    {ok, json_utils:json_term()} |
    {error, {bad_response, Code :: integer(), Body :: binary()}} |
    {error, term()}.
fetch_service_configuration(onezone) ->
    OzHostname = oneprovider:get_oz_domain(),
    URL = str_utils:format_bin("https://~s~s", [
        OzHostname, ?ZONE_CONFIGURATION_PATH
    ]),
    DeprecatedURL = str_utils:format_bin("https://~s~s", [
        OzHostname, ?DEPRECATED_ZONE_CONFIGURATION_PATH
    ]),
    SslOpts = [{cacerts, oneprovider:trusted_ca_certs()}],
    fetch_configuration(URL, DeprecatedURL, SslOpts);

fetch_service_configuration({oneprovider, Domain, Hostname}) ->
    URL = str_utils:format_bin("https://~s~s", [
        Hostname, ?PROVIDER_CONFIGURATION_PATH
    ]),
    DeprecatedURL = str_utils:format_bin("https://~s~s", [
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
-spec strings_to_binaries([string()]) -> [binary()].
strings_to_binaries(List) ->
    [list_to_binary(S) || S <- List].


%% @private
-spec binaries_to_strings([binary()]) -> [string()].
binaries_to_strings(List) ->
    [binary_to_list(B) || B <- List].
