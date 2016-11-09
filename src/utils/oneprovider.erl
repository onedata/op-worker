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
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("ctool/include/logging.hrl").

%% ID of provider that is not currently registered in Global Registry
-define(NON_GLOBAL_PROVIDER_ID, <<"non_global_provider">>).

%% ID of this provider (assigned by global registry)
-type id() :: binary().

-export_type([id/0]).

%% API
-export([get_node_hostname/0, get_node_ip/0]).
-export([get_provider_id/0, get_provider_domain/0]).
-export([get_oz_domain/0, get_oz_url/0]).
-export([get_oz_login_page/0, get_oz_logout_page/0, get_oz_providers_page/0]).
-export([register_in_oz/3, save_file/2]).

% Developer function
-export([register_in_oz_dev/3]).

%% Function for future use
%% todo: in order to use identity verification based on public keys
%% todo: use this function instead of register_in_oz[_dev] functions
-export([register_provider_in_oz/1]).

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


%%--------------------------------------------------------------------
%% @doc
%% Returns the domain of the provider, which is specified in env.
%% @end
%%--------------------------------------------------------------------
-spec get_provider_domain() -> string().
get_provider_domain() ->
    {ok, Domain} = application:get_env(?APP_NAME, provider_domain),
    str_utils:to_list(Domain).


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
%% Registers in OZ using config from app.src (cert locations).
%% @end
%%--------------------------------------------------------------------
-spec register_in_oz(NodeList :: [node()], KeyFilePassword :: string(), ClientName :: binary()) ->
    {ok, ProviderID :: binary()} | {error, term()}.
register_in_oz(NodeList, KeyFilePassword, ProviderName) ->
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
        RedirectionPoint = <<"https://", (hd(IPAddresses))/binary>>,
        Parameters = [
            {<<"urls">>, IPAddresses},
            {<<"csr">>, CSR},
            {<<"redirectionPoint">>, RedirectionPoint},
            {<<"clientName">>, ProviderName}
        ],
        {ok, ProviderId, Cert} = oz_providers:register(provider, Parameters),
        ok = file:write_file(OZPCertPath, Cert),
        OtherWorkers = NodeList -- [node()],
        save_file_on_hosts(OtherWorkers, OZPKeyPath, Key),
        save_file_on_hosts(OtherWorkers, OZPCertPath, Cert),
        {ok, ProviderId}
    catch
        T:M ->
            ?error_stacktrace("Cannot register in OZ - ~p:~p", [T, M]),
            {error, M}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Registers in OZ using config from app.src (cert locations).
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
        %% Use IP address of first node as redirection point - this way
        %% we don't need a DNS server to resolve provider domains in
        %% developer environment.
        RedirectionPoint = <<"https://", (hd(IPAddresses))/binary>>,
        Parameters = [
            {<<"urls">>, IPAddresses},
            {<<"csr">>, CSR},
            {<<"redirectionPoint">>, RedirectionPoint},
            {<<"clientName">>, ProviderName},
            {<<"uuid">>, ProviderName}
        ],
        {ok, ProviderId, Cert} = oz_providers:register_with_uuid(provider, Parameters),
        ok = file:write_file(OZPCertPath, Cert),
        OtherWorkers = NodeList -- [node()],
        save_file_on_hosts(OtherWorkers, OZPKeyPath, Key),
        save_file_on_hosts(OtherWorkers, OZPCertPath, Cert),
        {ok, ProviderId}
    catch
        T:M ->
            ?error_stacktrace("Cannot register in OZ - ~p:~p", [T, M]),
            {error, M}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Registers in OZ using config from app.src (cert locations).
%% @end
%%--------------------------------------------------------------------
-spec register_provider_in_oz(NodeList :: [node()]) ->
    {ok, ProviderID :: binary()} | {error, term()}.
register_provider_in_oz(NodeList) ->
    try
        {ok, KeyFile} = application:get_env(?APP_NAME, identity_key_file),
        {ok, CertFile} = application:get_env(?APP_NAME, identity_cert_file),
        Domain = oneprovider:get_provider_domain(),
        identity_utils:ensure_synced_cert_present(KeyFile, CertFile, Domain),
        Cert = identity_utils:read_cert(CertFile),
        PublicKey = identity_utils:get_public_key(Cert),
        ID = identity_utils:get_id(Cert),

        IPAddresses = get_all_nodes_ips(NodeList),
        RedirectionPoint = <<"https://", (hd(IPAddresses))/binary>>,

        Parameters = [
            {<<"id">>, ID},
            {<<"publicKey">>, identity_utils:encode(PublicKey)},
            {<<"urls">>, IPAddresses},
            {<<"redirectionPoint">>, RedirectionPoint}
        ],
        ok = oz_identities:register_provider(provider, Parameters),
        {ok, ID}
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
-spec get_provider_id() -> ProviderId :: binary() | no_return().
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
                    application:set_env(?APP_NAME, provider_id, ProviderId),
                    ProviderId;
                {error, _} ->
                    ?NON_GLOBAL_PROVIDER_ID
            catch
                _:Reason ->
                    ?error_stacktrace("Unable to read certificate file due to ~p", [Reason]),
                    ?NON_GLOBAL_PROVIDER_ID
            end
    end.

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
%% Saves given file on given hosts.
%% @end
%%--------------------------------------------------------------------
-spec save_file_on_hosts(Hosts :: [atom()], Path :: file:name_all(), Content :: binary()) ->
    ok | {error, [{node(), Reason :: term()}]}.
save_file_on_hosts(Hosts, Path, Content) ->
    Res = lists:foldl(
        fun(Host, Acc) ->
            case rpc:call(Host, ?MODULE, save_file, [Path, Content]) of
                ok ->
                    Acc;
                {error, Reason} ->
                    [{Host, Reason} | Acc]
            end
        end, [], Hosts),
    case Res of
        [] -> ok;
        Other -> Other
    end.


%%--------------------------------------------------------------------
%% @doc
%% Saves given file under given path.
%% @end
%%--------------------------------------------------------------------
-spec save_file(Path :: file:name_all(), Content :: binary()) -> ok | {error, term()}.
save_file(Path, Content) ->
    try
        file:make_dir(filename:dirname(Path)),
        ok = file:write_file(Path, Content),
        ok
    catch
        _:Reason ->
            ?error("Cannot save file ~p ~p", [Path, Reason]),
            {error, Reason}
    end.


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





