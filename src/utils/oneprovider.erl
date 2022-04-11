%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions related to the Oneprovider service (part of which is the
%%% op-worker application).
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
-export([get_domain/0, get_node_ip/0]).
-export([build_url/1, build_url/2, build_rest_url/1]).
-export([get_id/0, get_id_or_undefined/0, is_self/1, is_registered/0]).
-export([trusted_ca_certs/0]).
-export([get_oz_domain/0, replicate_oz_domain_to_node/1]).
-export([get_oz_url/0, get_oz_url/1]).
-export([get_oz_login_page/0, get_oz_logout_page/0, get_oz_providers_page/0]).
-export([set_up_service_in_onezone/0]).

% Developer functions
-export([register_in_oz_dev/3]).

-define(GUI_PACKAGE_PATH, op_worker:get_env(gui_package_path)).
-define(OZ_VERSION_CACHE_TTL, timer:minutes(5)).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_domain() -> binary().
get_domain() ->
    case provider_logic:get_domain() of
        {ok, Domain} -> Domain;
        _ -> list_to_binary(utils:get_host(node()))
    end.


-spec get_node_ip() -> inet:ip4_address().
get_node_ip() ->
    node_manager:get_ip_address().


-spec build_url(string() | binary()) -> binary().
build_url(AbsolutePath) ->
    build_url(https, AbsolutePath).

-spec build_url(wss | https, string() | binary()) -> binary().
build_url(Scheme, AbsolutePath) ->
    Port = https_listener:port(),
    Host = get_domain(),
    str_utils:format_bin("~s://~s:~B~s", [Scheme, Host, Port, AbsolutePath]).


-spec build_rest_url(binary() | [binary()]) -> binary().
build_rest_url(AbsolutePath) when is_binary(AbsolutePath) ->
    build_url(<<"/api/v3/oneprovider", AbsolutePath/binary>>);
build_rest_url(PathTokens) when is_list(PathTokens) ->
    build_rest_url(string:trim(filename:join([<<"/">> | PathTokens]), leading, [$/])).


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
%% Returns CA certs trusted by this provider in DER format.
%% @end
%%--------------------------------------------------------------------
-spec trusted_ca_certs() -> [public_key:der_encoded()].
trusted_ca_certs() ->
    cert_utils:load_ders_in_dir(oz_plugin:get_cacerts_dir()).


-spec get_oz_domain() -> binary().
get_oz_domain() ->
    str_utils:to_binary(op_worker:get_env(oz_domain)).


-spec replicate_oz_domain_to_node(node()) -> ok | no_return().
replicate_oz_domain_to_node(Node) ->
    Domain = op_worker:get_env(oz_domain),
    ok = rpc:call(Node, op_worker, set_env, [oz_domain, Domain]).


-spec get_oz_url() -> binary().
get_oz_url() ->
    <<"https://", (get_oz_domain())/binary>>.


-spec get_oz_url(Path :: binary() | string()) -> binary().
get_oz_url(Path) when is_list(Path) ->
    get_oz_url(list_to_binary(Path));
get_oz_url(<<"/", _/binary>> = Path) ->
    <<(get_oz_url())/binary, Path/binary>>;
get_oz_url(Path) ->
    get_oz_url(<<"/", Path/binary>>).


-spec get_oz_login_page() -> binary().
get_oz_login_page() ->
    get_oz_url(op_worker:get_env(oz_login_page)).


-spec get_oz_logout_page() -> binary().
get_oz_logout_page() ->
    get_oz_url(op_worker:get_env(oz_logout_page)).


-spec get_oz_providers_page() -> binary().
get_oz_providers_page() ->
    get_oz_url(op_worker:get_env(oz_providers_page)).


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
    Release = op_worker:get_release_version(),
    Build = op_worker:get_build_version(),
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
                        "up in Onezone due to an error during GUI package upload. "
                        "The Web GUI might be non-functional.~nError was: ~p",
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
        T:M:Stacktrace ->
            ?error_stacktrace("Cannot register in OZ - ~p:~p", [T, M], Stacktrace),
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
    lists_utils:pmap(fun(Node) ->
        {ok, IPAddr} = rpc:call(Node, oz_providers, check_ip_address, [none]),
        IPAddr
    end, NodeList).

