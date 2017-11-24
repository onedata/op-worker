%%%-------------------------------------------------------------------
%%% @author Rafal Slota
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

%% ID of this provider (assigned by global registry)
-type id() :: binary().

-export_type([id/0]).

%% API
-export([get_node_hostname/0, get_node_ip/0]).
-export([get_provider_id/0, is_registered/0, get_rest_endpoint/1]).
-export([get_oz_domain/0, get_oz_url/0]).
-export([get_oz_login_page/0, get_oz_logout_page/0, get_oz_providers_page/0]).
-export([on_connection_to_oz/0]).

% Developer function
-export([register_in_oz_dev/3]).

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

%%-------------------------------------------------------------------
%% @doc
%% Returns full provider rest endpoint URL.
%% @end
%%-------------------------------------------------------------------
-spec get_rest_endpoint(string()) -> string().
get_rest_endpoint(Path) ->
    {ok, Port} = application:get_env(?APP_NAME, rest_port),
    Host = oneprovider:get_node_hostname(),
    str_utils:format("https://~s:~B/api/v3/oneprovider/~s", [Host, Port, Path]).

%%--------------------------------------------------------------------
%% @doc
%% Returns the IP of the node, retrieved from node_manager, which has
%% acquired it by contacting OZ.
%% @end
%%--------------------------------------------------------------------
-spec get_node_ip() -> {byte(), byte(), byte(), byte()}.
get_node_ip() ->
    node_manager:get_ip_address().


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


-spec on_connection_to_oz() -> ok.
on_connection_to_oz() ->
    % when connection is established onezone should be notified about
    % current provider ips.
    % cast is used as this function is called
    % in gs_client init and a call would cause a deadlock - updating
    % ips uses the graph sync connection.
    gen_server2:cast(?NODE_MANAGER_NAME, update_subdomain_delegation_ips),

    % Make sure provider proto listener is started
    % (it won't start until provider is registered)
    ok = provider_listener:ensure_started().


%%--------------------------------------------------------------------
%% @doc
%% Registers in OZ using config from app.src (cert locations).
%% This functionality is dedicated for test environments - in production,
%% onepanel is responsible for registering the provider.
%% @end
%%--------------------------------------------------------------------
-spec register_in_oz_dev(NodeList :: [node()], KeyFilePassword :: string(), ClientName :: binary()) ->
    {ok, ProviderID :: binary()} | {error, term()}.
register_in_oz_dev(NodeList, KeyFilePassword, ProviderName) ->
    try
        OZPKeyPath = oz_plugin:get_key_file(),
        OZPCertPath = oz_plugin:get_cert_file(),
        OZPCSRPath = oz_plugin:get_csr_file(),
        % Create a CSR
        0 = csr_creator:create_csr(KeyFilePassword, OZPKeyPath, OZPCSRPath),
        {ok, CSR} = file:read_file(OZPCSRPath),
        {ok, Key} = file:read_file(OZPKeyPath),

        % Send signing request to OZ
        IPAddresses = get_all_nodes_ips(NodeList),
        %% Use IP address of first node as provider domain - this way
        %% we don't need a DNS server to resolve provider domains in
        %% developer environment.
        Domain = <<(hd(IPAddresses))/binary>>,
        SubdomainDelegation = false,
        Parameters = [
            {<<"csr">>, CSR},
            {<<"domain">>, Domain},
            {<<"name">>, ProviderName},
            {<<"subdomainDelegation">>, SubdomainDelegation},
            {<<"uuid">>, ProviderName}
        ],
        {ok, ProviderId, Cert} = oz_providers:register_with_uuid(provider, Parameters),
        ok = file:write_file(OZPCertPath, Cert),
        OtherWorkers = NodeList -- [node()],
        ok = utils:save_file_on_hosts(OtherWorkers, OZPKeyPath, Key),
        ok = utils:save_file_on_hosts(OtherWorkers, OZPCertPath, Cert),
        % Download OZ public CA cert and save it
        {ok, CaCert} = oz_providers:get_oz_cacert(provider),
        CaCertPath = oz_plugin:get_oz_cacert_path(),
        ok = file:write_file(CaCertPath, CaCert),
        ok = utils:save_file_on_hosts(OtherWorkers, CaCertPath, CaCert),
        {ok, ProviderId}
    catch
        T:M ->
            ?error_stacktrace("Cannot register in OZ - ~p:~p", [T, M]),
            {error, M}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns Provider ID for current oneprovider instance.
%% Fails with undefined exception if the oneprovider is not registered as a provider.
%% @end
%%--------------------------------------------------------------------
-spec get_provider_id() -> ProviderId :: binary().
get_provider_id() ->
    % Cache the provider ID so that we don't decode the cert every time
    case application:get_env(?APP_NAME, provider_id) of
        {ok, ProviderId} ->
            ProviderId;
        _ ->
            try file:read_file(oz_plugin:get_cert_file()) of
                {ok, Bin} ->
                    [{_, PeerCertDer, _} | _] = public_key:pem_decode(Bin),
                    PeerCert = public_key:pkix_decode_cert(PeerCertDer, otp),
                    ProviderId = get_provider_id(PeerCert),
                    catch application:set_env(?APP_NAME, provider_id, ProviderId),
                    ProviderId;
                {error, _} ->
                    ?UNREGISTERED_PROVIDER_ID
            catch
                _:Reason ->
                    ?error_stacktrace("Unable to read certificate file due to ~p", [Reason]),
                    ?UNREGISTERED_PROVIDER_ID
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns whether this provider is registered in OneZone.
%% @end
%%--------------------------------------------------------------------
-spec is_registered() -> boolean().
is_registered() ->
    ?UNREGISTERED_PROVIDER_ID /= get_provider_id().


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns ProviderId based on provider's certificate (issued by OZ).
%% @end
%%--------------------------------------------------------------------
-spec get_provider_id(Cert :: #'OTPCertificate'{}) -> ProviderId :: binary() | no_return().
get_provider_id(#'OTPCertificate'{} = Cert) ->
    #'OTPCertificate'{tbsCertificate =
    #'OTPTBSCertificate'{subject = {rdnSequence, Attrs}}} = Cert,

    [ProviderId] = lists:filtermap(fun([Attribute]) ->
        case Attribute#'AttributeTypeAndValue'.type of
            ?'id-at-commonName' ->
                {_, Id} = Attribute#'AttributeTypeAndValue'.value,
                {true, str_utils:to_binary(Id)};
            _ -> false
        end
    end, Attrs),

    str_utils:to_binary(ProviderId).


%%--------------------------------------------------------------------
%% @doc
%% Returns a list of all nodes IP addresses.
%% @end
%%--------------------------------------------------------------------
-spec get_all_nodes_ips(NodeList :: [node()]) -> [binary()].
get_all_nodes_ips(NodeList) ->
    utils:pmap(
        fun(Node) ->
            {ok, IPAddr} = rpc:call(Node, oz_providers, check_ip_address, [provider]),
            IPAddr
        end, NodeList).





