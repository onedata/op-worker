%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Library module for oneprovider-wide operations.
%%% @end
%%%-------------------------------------------------------------------
-module(oneprovider).
-author("Rafal Slota").
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% ID of this provider (assigned by global registry)
-type id() :: binary().

-export_type([id/0]).

%% API
-export([get_node_hostname/0, get_node_ip/0, get_rest_endpoint/1]).
-export([get_id/1, is_local/1, is_registered/0]).
-export([get_ca_certs/0]).
-export([get_oz_domain/0, get_oz_url/0]).
-export([get_oz_login_page/0, get_oz_logout_page/0, get_oz_providers_page/0]).
-export([is_connected_to_oz/0, on_connection_to_oz/0]).

% Developer functions
-export([register_in_oz_dev/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns the hostname of the node, based on its erlang node name.
%% @end
%%--------------------------------------------------------------------
-spec get_node_hostname() -> string().
get_node_hostname() ->
    utils:get_host(node()).


%%--------------------------------------------------------------------
%% @doc
%% Returns the IP of the node, retrieved from node_manager, which has
%% acquired it by contacting OZ.
%% @end
%%--------------------------------------------------------------------
-spec get_node_ip() -> {byte(), byte(), byte(), byte()}.
get_node_ip() ->
    node_manager:get_ip_address().


%%-------------------------------------------------------------------
%% @doc
%% Returns full provider rest endpoint URL.
%% @end
%%-------------------------------------------------------------------
-spec get_rest_endpoint(string()) -> string().
get_rest_endpoint(Path) ->
    {ok, Port} = application:get_env(?APP_NAME, gui_https_port),
    Host = oneprovider:get_node_hostname(),
    str_utils:format("https://~s:~B/api/v3/oneprovider/~s", [Host, Port, Path]).


%%--------------------------------------------------------------------
%% @doc
%% Returns Provider ID for current oneprovider instance.
%% Fails with exception or returns undefined if this provider is not registered,
%% depending on the ErrorHandling option.
%% @end
%%--------------------------------------------------------------------
-spec get_id(ErrorHandling :: fail_with_undefined | fail_with_throw) ->
    undefined | od_provider:id() | no_return().
get_id(ErrorHandling) ->
    case provider_auth:get_provider_id() of
        {error, _} ->
            case ErrorHandling of
                fail_with_throw -> throw(?ERROR_UNREGISTERED_PROVIDER);
                fail_with_undefined -> undefined
            end;
        ProviderId ->
            ProviderId
    end.


%%--------------------------------------------------------------------
%% @doc
%% Predicate saying if given ProviderId is the Id of this provider.
%% @end
%%--------------------------------------------------------------------
-spec is_local(od_provider:id()) -> boolean().
is_local(ProviderId) ->
    ProviderId =:= provider_auth:get_provider_id().


%%--------------------------------------------------------------------
%% @doc
%% Returns whether this provider is registered in OneZone.
%% @end
%%--------------------------------------------------------------------
-spec is_registered() -> boolean().
is_registered() ->
    provider_auth:is_registered().


%%--------------------------------------------------------------------
%% @doc
%% Returns CA certs trusted by this provider in DER format.
%% @end
%%--------------------------------------------------------------------
-spec get_ca_certs() -> [DerCert :: binary()].
get_ca_certs() ->
    cert_utils:load_ders_in_dir(oz_plugin:get_cacerts_dir()).


%%--------------------------------------------------------------------
%% @doc
%% Returns the domain of OZ, which is specified in env.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_domain() -> string().
get_oz_domain() ->
    {ok, Hostname} = application:get_env(?APP_NAME, oz_domain),
    str_utils:to_list(Hostname).


%%--------------------------------------------------------------------
%% @doc
%% Returns the URL to OZ.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_url() -> string().
get_oz_url() ->
    "https://" ++ get_oz_domain().


%%--------------------------------------------------------------------
%% @doc
%% Returns the URL to OZ login page.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_login_page() -> string().
get_oz_login_page() ->
    {ok, Page} = application:get_env(?APP_NAME, oz_login_page),
    % Page is in format '/page_name.html'
    str_utils:format("https://~s~s", [get_oz_domain(), Page]).


%%--------------------------------------------------------------------
%% @doc
%% Returns the URL to OZ logout page.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_logout_page() -> string().
get_oz_logout_page() ->
    {ok, Page} = application:get_env(?APP_NAME, oz_logout_page),
    % Page is in format '/page_name.html'
    str_utils:format("https://~s~s", [get_oz_domain(), Page]).


%%--------------------------------------------------------------------
%% @doc
%% Returns the URL to OZ logout page.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_providers_page() -> string().
get_oz_providers_page() ->
    {ok, Page} = application:get_env(?APP_NAME, oz_providers_page),
    % Page is in format '/page_name.html'
    str_utils:format("https://~s~s", [get_oz_domain(), Page]).


%%--------------------------------------------------------------------
%% @doc
%% Predicate saying if the provider is actively connected to OneZone via
%% GraphSync channel.
%% @end
%%--------------------------------------------------------------------
-spec is_connected_to_oz() -> boolean().
is_connected_to_oz() ->
    gs_worker:is_connected().


%%--------------------------------------------------------------------
%% @doc
%% Callback called when connection to OneZone is established.
%% @end
%%--------------------------------------------------------------------
-spec on_connection_to_oz() -> ok.
on_connection_to_oz() ->
    % when connection is established onezone should be notified about
    % current provider ips.
    % cast is used as this function is called
    % in gs_client init and a call would cause a deadlock - updating
    % ips uses the graph sync connection.
    gen_server2:cast(?NODE_MANAGER_NAME, update_subdomain_delegation_ips).


%%--------------------------------------------------------------------
%% @doc
%% Registers the provider in OneZone.
%% This functionality is dedicated for test environments - in production,
%% onepanel is responsible for registering the provider.
%% @end
%%--------------------------------------------------------------------
-spec register_in_oz_dev(NodeList :: [node()], ProviderName :: binary()) ->
    {ok, ProviderId :: od_provider:id()} | {error, term()}.
register_in_oz_dev(NodeList, ProviderName) ->
    try
        % Send signing request to OZ
        IPAddresses = get_all_nodes_ips(NodeList),
        %% Use IP address of first node as provider domain - this way
        %% we don't need a DNS server to resolve provider domains in
        %% developer environment.
        Domain = <<(hd(IPAddresses))/binary>>,
        SubdomainDelegation = false,
        Parameters = [
            {<<"domain">>, Domain},
            {<<"name">>, ProviderName},
            {<<"subdomainDelegation">>, SubdomainDelegation},
            {<<"uuid">>, ProviderName}
        ],
        {ok, ProviderId, Macaroon} = oz_providers:register_with_uuid(none, Parameters),
        provider_auth:save(ProviderId, Macaroon),
        {ok, ProviderId}
    catch
        T:M ->
            ?error_stacktrace("Cannot register in OZ - ~p:~p", [T, M]),
            {error, M}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of all nodes IP addresses.
%% @end
%%--------------------------------------------------------------------
-spec get_all_nodes_ips(NodeList :: [node()]) -> [binary()].
get_all_nodes_ips(NodeList) ->
    utils:pmap(
        fun(Node) ->
            {ok, IPAddr} = rpc:call(Node, oz_providers, check_ip_address, [none]),
            IPAddr
        end, NodeList).

