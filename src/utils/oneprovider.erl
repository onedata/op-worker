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
-include("cluster/worker/modules/datastore/datastore.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("ctool/include/logging.hrl").

-define(GRPKEY_ENV, grpkey_path).
-define(GRPCSR_ENV, grpcsr_path).
-define(GRPCERT_ENV, grpcert_path).


%% ID of provider that is not currently registered in Global Registry
-define(NON_GLOBAL_PROVIDER_ID, <<"non_global_provider">>).


%% ID of this provider (assigned by global registry)
-type id() :: binary().

-export_type([id/0]).

%% API
-export([get_node_hostname/0, get_node_ip/0]).
-export([get_provider_domain/0, get_gr_domain/0]).
-export([get_provider_id/0, get_globalregistry_cert/0]).
-export([register_in_gr/3, register_in_gr_dev/3, save_file/2]).

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
%% acquired it by contacting GR.
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
    gui_str:to_list(Domain).


%%--------------------------------------------------------------------
%% @doc
%% Returns the domain of GR, which is specified in env.
%% @end
%%--------------------------------------------------------------------
-spec get_gr_domain() -> string().
get_gr_domain() ->
    {ok, Hostname} = application:get_env(?APP_NAME, global_registry_domain),
    gui_str:to_list(Hostname).


%%--------------------------------------------------------------------
%% @doc
%% Registers in GR using config from app.src (cert locations).
%% @end
%%--------------------------------------------------------------------
-spec register_in_gr(NodeList :: [node()], KeyFilePassword :: string(), ClientName :: binary()) ->
    {ok, ProviderID :: binary()} | {error, term()}.
register_in_gr(NodeList, KeyFilePassword, ProviderName) ->
    try
        GRPKeyPath = gr_plugin:get_key_path(),
        GRPCertPath = gr_plugin:get_cert_path(),
        GRPCSRPath = gr_plugin:get_csr_path(),
        % Create a CSR
        0 = csr_creator:create_csr(KeyFilePassword, GRPKeyPath, GRPCSRPath),
        {ok, CSR} = file:read_file(GRPCSRPath),
        {ok, Key} = file:read_file(GRPKeyPath),
        % Send signing request to GR
        IPAddresses = get_all_nodes_ips(NodeList),
        RedirectionPoint = <<"https://", (hd(IPAddresses))/binary>>,
        Parameters = [
            {<<"urls">>, IPAddresses},
            {<<"csr">>, CSR},
            {<<"redirectionPoint">>, RedirectionPoint},
            {<<"clientName">>, ProviderName}
        ],
        {ok, ProviderId, Cert} = gr_providers:register(provider, Parameters),
        ok = file:write_file(GRPCertPath, Cert),
        OtherWorkers = NodeList -- [node()],
        save_file_on_hosts(OtherWorkers, GRPKeyPath, Key),
        save_file_on_hosts(OtherWorkers, GRPCertPath, Cert),
        {ok, ProviderId}
    catch
        T:M ->
            ?error_stacktrace("Cannot register in GlobalRegistry - ~p:~p", [T, M]),
            {error, M}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Registers in GR using config from app.src (cert locations).
%% @end
%%--------------------------------------------------------------------
-spec register_in_gr_dev(NodeList :: [node()], KeyFilePassword :: string(), ClientName :: binary()) ->
    {ok, ProviderID :: binary()} | {error, term()}.
register_in_gr_dev(NodeList, KeyFilePassword, ProviderName) ->
    try
        GRPKeyPath = gr_plugin:get_key_path(),
        GRPCertPath = gr_plugin:get_cert_path(),
        GRPCSRPath = gr_plugin:get_csr_path(),
        % Create a CSR
        0 = csr_creator:create_csr(KeyFilePassword, GRPKeyPath, GRPCSRPath),
        {ok, CSR} = file:read_file(GRPCSRPath),
        {ok, Key} = file:read_file(GRPKeyPath),
        % Send signing request to GR
        IPAddresses = get_all_nodes_ips(NodeList),
        ProviderDomain = gui_str:to_binary(oneprovider:get_provider_domain()),
        RedirectionPoint = <<"https://", ProviderDomain/binary>>,
        Parameters = [
            {<<"urls">>, IPAddresses},
            {<<"csr">>, CSR},
            {<<"redirectionPoint">>, RedirectionPoint},
            {<<"clientName">>, ProviderName},
            {<<"uuid">>, ProviderName}
        ],
        {ok, ProviderId, Cert} = gr_providers:register_with_uuid(provider, Parameters),
        ok = file:write_file(GRPCertPath, Cert),
        OtherWorkers = NodeList -- [node()],
        save_file_on_hosts(OtherWorkers, GRPKeyPath, Key),
        save_file_on_hosts(OtherWorkers, GRPCertPath, Cert),
        {ok, ProviderId}
    catch
        T:M ->
            ?error_stacktrace("Cannot register in GlobalRegistry - ~p:~p", [T, M]),
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
            try file:read_file(gr_plugin:get_cert_path()) of
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


%%--------------------------------------------------------------------
%% @doc
%% Returns GR public certificate
%% @end
%%--------------------------------------------------------------------
-spec get_globalregistry_cert() -> #'OTPCertificate'{} | no_return().
get_globalregistry_cert() ->
    % Cache the cert so that we don't decode the cert every time
    case application:get_env(?APP_NAME, globalregistry_certificate) of
        {ok, GrCert} ->
            GrCert;
        _ ->
            {ok, PemCert} = file:read_file(gr_plugin:get_cacert_path()),
            [{'Certificate', DerCert, _}] = public_key:pem_decode(PemCert),
            GrCert = #'OTPCertificate'{} = public_key:pkix_decode_cert(DerCert, otp),
            application:set_env(?APP_NAME, globalregistry_certificate, GrCert),
            GrCert
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns ProviderId based on provider's certificate (issued by globalregistry).
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
                {true, utils:ensure_binary(Id)};
            _ -> false
        end
    end, Attrs),

    utils:ensure_binary(ProviderId).


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
            {ok, IPAddr} = rpc:call(Node, gr_providers, check_ip_address, [provider]),
            IPAddr
        end, NodeList).





