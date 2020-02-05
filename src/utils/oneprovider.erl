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
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% Id of this provider (assigned by Onezone)
-type id() :: binary().
-export_type([id/0]).

%% API
-export([get_domain/0, get_node_ip/0, get_rest_endpoint/1]).
-export([get_id/0, get_id_or_undefined/0, is_self/1, is_registered/0]).
-export([get_version/0, get_build/0]).
-export([trusted_ca_certs/0]).
-export([get_oz_domain/0, get_oz_url/0, get_oz_version/0]).
-export([get_oz_login_page/0, get_oz_logout_page/0, get_oz_providers_page/0]).
-export([is_connected_to_oz/0]).
-export([terminate_oz_connection/0, force_oz_connection_start/0, restart_oz_connection/0]).
-export([on_connect_to_oz/0, on_disconnect_from_oz/0, on_deregister/0]).
-export([set_up_service_in_onezone/0]).

% Developer functions
-export([register_in_oz_dev/3]).

-define(GUI_PACKAGE_PATH, begin
    {ok, __Path} = application:get_env(?APP_NAME, gui_package_path), __Path
end).
-define(OZ_VERSION_CACHE_TTL, timer:minutes(5)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns the hostname of the node, based on its erlang node name.
%% @end
%%--------------------------------------------------------------------
-spec get_domain() -> binary().
get_domain() ->
    case provider_logic:get_domain() of
        {ok, Domain} -> Domain;
        _ -> list_to_binary(utils:get_host(node()))
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns the IP of the node, retrieved from node_manager, which has
%% acquired it by contacting OZ.
%% @end
%%--------------------------------------------------------------------
-spec get_node_ip() -> inet:ip4_address().
get_node_ip() ->
    node_manager:get_ip_address().


%%-------------------------------------------------------------------
%% @doc
%% Returns full provider rest endpoint URL.
%% @end
%%-------------------------------------------------------------------
-spec get_rest_endpoint(binary() | string()) -> string().
get_rest_endpoint(Path) ->
    Port = https_listener:port(),
    Host = get_domain(),
    str_utils:format("https://~s:~B/api/v3/oneprovider/~s", [Host, Port, Path]).


%%--------------------------------------------------------------------
%% @doc
%% Returns Provider Id for current oneprovider instance.
%% Fails with exception if this provider is not registered.
%% @end
%%--------------------------------------------------------------------
-spec get_id() -> od_provider:id() | no_return().
get_id() ->
    case provider_auth:get_provider_id() of
        {error, _} -> throw(?ERROR_UNREGISTERED_ONEPROVIDER);
        {ok, ProviderId} -> ProviderId
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns Provider Id for current oneprovider instance.
%% Returns undefined if this provider is not registered.
%% @end
%%--------------------------------------------------------------------
-spec get_id_or_undefined() -> od_provider:id() | undefined.
get_id_or_undefined() ->
    try provider_auth:get_provider_id() of
        {error, _} -> undefined;
        {ok, ProviderId} -> ProviderId
    catch
        _:_ ->
            % Can appear before cluster is initialized
            undefined
    end.


%%--------------------------------------------------------------------
%% @doc
%% Predicate saying if given ProviderId is the Id of this provider.
%% @end
%%--------------------------------------------------------------------
-spec is_self(od_provider:id()) -> boolean().
is_self(ProviderId) ->
    {ok, ProviderId} =:= provider_auth:get_provider_id().


%%--------------------------------------------------------------------
%% @doc
%% Returns whether this provider is registered in Onezone.
%% @end
%%--------------------------------------------------------------------
-spec is_registered() -> boolean().
is_registered() ->
    provider_auth:is_registered().


%%--------------------------------------------------------------------
%% @doc
%% Returns current Oneprovider version.
%% @end
%%--------------------------------------------------------------------
-spec get_version() -> binary().
get_version() ->
    {_AppId, _AppName, OpVersion} = lists:keyfind(
        ?APP_NAME, 1, application:loaded_applications()
    ),
    list_to_binary(OpVersion).


%%--------------------------------------------------------------------
%% @doc
%% Returns current Oneprovider build.
%% @end
%%--------------------------------------------------------------------
-spec get_build() -> binary().
get_build() ->
    case application:get_env(?APP_NAME, build_version, "unknown") of
        "" -> <<"unknown">>;
        Build -> list_to_binary(Build)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns CA certs trusted by this provider in DER format.
%% @end
%%--------------------------------------------------------------------
-spec trusted_ca_certs() -> [DerCert :: binary()].
trusted_ca_certs() ->
    cert_utils:load_ders_in_dir(oz_plugin:get_cacerts_dir()).


%%--------------------------------------------------------------------
%% @doc
%% Returns the domain of OZ, which is specified in env.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_domain() -> binary().
get_oz_domain() ->
    case application:get_env(?APP_NAME, oz_domain) of
        {ok, Domain} when is_binary(Domain) -> Domain;
        {ok, Domain} when is_list(Domain) -> str_utils:to_binary(Domain);
        _ -> error({missing_env_variable, oz_domain})
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns the URL to OZ.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_url() -> binary().
get_oz_url() ->
    <<"https://", (get_oz_domain())/binary>>.


%%--------------------------------------------------------------------
%% @doc
%% Returns the OZ application version.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_version() -> binary().
get_oz_version() ->
    GetOzVersion = fun() ->
        {ok, OzVersion} = provider_logic:fetch_peer_version(onezone),
        {true, OzVersion, ?OZ_VERSION_CACHE_TTL}
    end,
    {ok, OzVersion} = simple_cache:get(cached_oz_version, GetOzVersion),
    OzVersion.


%%--------------------------------------------------------------------
%% @doc
%% Returns the URL to OZ login page.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_login_page() -> binary().
get_oz_login_page() ->
    {ok, Page} = application:get_env(?APP_NAME, oz_login_page),
    % Page is in format '/page_name.html'
    str_utils:format_bin("https://~s~s", [get_oz_domain(), Page]).


%%--------------------------------------------------------------------
%% @doc
%% Returns the URL to OZ logout page.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_logout_page() -> binary().
get_oz_logout_page() ->
    {ok, Page} = application:get_env(?APP_NAME, oz_logout_page),
    % Page is in format '/page_name.html'
    str_utils:format_bin("https://~s~s", [get_oz_domain(), Page]).


%%--------------------------------------------------------------------
%% @doc
%% Returns the URL to OZ logout page.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_providers_page() -> binary().
get_oz_providers_page() ->
    {ok, Page} = application:get_env(?APP_NAME, oz_providers_page),
    % Page is in format '/page_name.html'
    str_utils:format_bin("https://~s~s", [get_oz_domain(), Page]).


%%--------------------------------------------------------------------
%% @doc
%% Predicate saying if the provider is actively connected to Onezone via
%% GraphSync channel.
%% @end
%%--------------------------------------------------------------------
-spec is_connected_to_oz() -> boolean().
is_connected_to_oz() ->
    gs_worker:is_connected().


%%--------------------------------------------------------------------
%% @doc
%% Immediately kills existing Onezone connection (if any).
%% @end
%%--------------------------------------------------------------------
-spec terminate_oz_connection() -> ok.
terminate_oz_connection() ->
    gs_worker:terminate_connection().


%%--------------------------------------------------------------------
%% @doc
%% Immediately triggers onezone connection attempt.
%% Returns boolean indicating success.
%% NOTE: used by onepanel (RPC)
%% @end
%%--------------------------------------------------------------------
-spec force_oz_connection_start() -> boolean().
force_oz_connection_start() ->
    gs_worker:force_connection_start().


%%--------------------------------------------------------------------
%% @doc
%% Immediately triggers onezone connection attempt.
%% Returns boolean indicating success.
%% @end
%%--------------------------------------------------------------------
-spec restart_oz_connection() -> ok.
restart_oz_connection() ->
    gs_worker:restart_connection().


%%--------------------------------------------------------------------
%% @doc
%% Callback called when connection to Onezone is established.
%% Should throw on any error, in such case the connection is aborted and
%% attempted again after backoff.
%% @end
%%--------------------------------------------------------------------
-spec on_connect_to_oz() -> ok.
on_connect_to_oz() ->
    set_up_service_in_onezone(),
    ok = provider_logic:update_subdomain_delegation_ips(),
    ok = main_harvesting_stream:revise_all_spaces(),
    ok = qos_bounded_cache:ensure_exists_for_all_spaces(),
    ok = fslogic_worker:init_paths_caches(all),
    ok = auth_cache:report_oz_connection_start(),
    storage_sync_worker:notify_connection_to_oz().


%%--------------------------------------------------------------------
%% @doc
%% Callback called when connection to Onezone is terminated.
%% @end
%%--------------------------------------------------------------------
-spec on_disconnect_from_oz() -> ok.
on_disconnect_from_oz() ->
    ok = auth_cache:report_oz_connection_termination().


%%--------------------------------------------------------------------
%% @doc
%% Performs cleanup upon provider deregistration.
%% @end
%%--------------------------------------------------------------------
-spec on_deregister() -> ok.
on_deregister() ->
    ?info("Provider has been deregistered"),
    provider_auth:delete(),
    storage:clear_storages(),
    % kill the connection to prevent 'unauthorized' errors due
    % to older authorization when immediately registering anew
    terminate_oz_connection().


%%--------------------------------------------------------------------
%% @doc
%% Sets up Oneprovider worker service in Onezone - updates version info
%% (release, build and GUI versions). If given GUI version is not present in
%% Onezone, the GUI package is uploaded first. If any errors occur during
%% upload, the Oneprovider continues to operate, but its GUI might not be
%% functional.
%% @end
%%--------------------------------------------------------------------
-spec set_up_service_in_onezone() -> ok.
set_up_service_in_onezone() ->
    ?info("Setting up Oneprovider worker service in Onezone"),
    Release = get_version(),
    Build = get_build(),
    {ok, GuiHash} = gui:package_hash(?GUI_PACKAGE_PATH),

    case cluster_logic:update_version_info(Release, Build, GuiHash) of
        ok ->
            ?info("Skipping GUI upload as it is already present in Onezone"),
            ?info("Oneprovider worker service successfully set up in Onezone");
        ?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"workerVersion.gui">>) ->
            ?info("Uploading GUI to Onezone (~s)", [GuiHash]),
            case cluster_logic:upload_op_worker_gui(?GUI_PACKAGE_PATH) of
                ok ->
                    ?info("GUI uploaded succesfully"),
                    ok = cluster_logic:update_version_info(Release, Build, GuiHash),
                    ?info("Oneprovider worker service successfully set up in Onezone");
                {error, _} = Error ->
                    ?alert(
                        "Oneprovider worker service could not be successfully set "
                        "up in Onezone due to an error during GUI upload: ~p",
                        [Error]
                    )
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Registers the provider in Onezone.
%% This functionality is dedicated for test environments - in production,
%% onepanel is responsible for registering the provider.
%% @end
%%--------------------------------------------------------------------
-spec register_in_oz_dev(NodeList :: [node()], ProviderName :: binary(), Token :: binary()) ->
    {ok, ProviderId :: od_provider:id()} | {error, term()}.
register_in_oz_dev(NodeList, ProviderName, Token) ->
    try
        % Send signing request to OZ
        IPAddresses = get_all_nodes_ips(NodeList),
        %% Use IP address of first node as provider domain - this way
        %% we don't need a DNS server to resolve provider domains in
        %% developer environment.
        Domain = <<(hd(IPAddresses))/binary>>,
        SubdomainDelegation = false,
        Parameters = [
            {<<"uuid">>, ProviderName},
            {<<"token">>, Token},
            {<<"name">>, ProviderName},
            {<<"adminEmail">>, <<ProviderName/binary, "@onedata.org">>},
            {<<"subdomainDelegation">>, SubdomainDelegation},
            {<<"domain">>, Domain}
        ],
        {ok, #{
            <<"providerId">> := ProviderId,
            <<"providerRootToken">> := RootToken
        }} = oz_providers:register_with_uuid(none, Parameters),
        provider_auth:save(ProviderId, RootToken),
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

