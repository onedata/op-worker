%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Plugin which extends node manager for op_worker
%%% @end
%%%-------------------------------------------------------------------
-module(node_manager_plugin).
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% node_manager_plugin_behaviour callbacks
-export([before_init/1, modules_with_args/0, listeners/0, cm_nodes/0,
    db_nodes/0, check_node_ip_address/0, app_name/0, renamed_models/0]).

-type model() :: datastore_model:model().
-type record_version() :: datastore_model:record_version().

%%%===================================================================
%%% node_manager_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:app_name/0}.
%% @end
%%--------------------------------------------------------------------
-spec app_name() -> {ok, Name :: atom()}.
app_name() ->
    {ok, op_worker}.

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:cm_nodes/0}.
%% @end
%%--------------------------------------------------------------------
-spec cm_nodes() -> {ok, Nodes :: [atom()]} | undefined.
cm_nodes() ->
    application:get_env(?APP_NAME, cm_nodes).

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:db_nodes/0}.
%% @end
%%--------------------------------------------------------------------
-spec db_nodes() -> {ok, Nodes :: [atom()]} | undefined.
db_nodes() ->
    application:get_env(?APP_NAME, db_nodes).

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:listeners/0}.
%% @end
%%--------------------------------------------------------------------
-spec listeners() -> Listeners :: [atom()].
listeners() -> node_manager:cluster_worker_listeners() ++ [
    gui_listener,
    protocol_listener,
    rest_listener,
    provider_listener
].

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:modules_with_args/0}.
%% @end
%%--------------------------------------------------------------------
-spec modules_with_args() -> Models :: [{atom(), [any()]}].
modules_with_args() -> [
    {session_manager_worker, [
        {supervisor_flags, session_manager_worker:supervisor_flags()},
        {supervisor_children_spec, session_manager_worker:supervisor_children_spec()}
    ]},
    {fslogic_worker, []},
    {dbsync_worker, [
        {supervisor_flags, dbsync_worker:supervisor_flags()}
    ]},
    {monitoring_worker, [
        {supervisor_flags, monitoring_worker:supervisor_flags()},
        {supervisor_children_spec, monitoring_worker:supervisor_children_spec()}
    ]},
    {gs_worker, [
        {supervisor_flags, gs_worker:supervisor_flags()}
    ]},
    {fslogic_deletion_worker, []},
    {space_sync_worker, []}
].

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:renamed_models/0}.
%% @end
%%--------------------------------------------------------------------
-spec renamed_models() -> #{{record_version(), model()} => model()}.
renamed_models() ->
    #{{1, open_file} => file_handles}.

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:before_init/1}.
%% @end
%%--------------------------------------------------------------------
-spec before_init(Args :: term()) -> Result :: ok | {error, Reason :: term()}.
before_init([]) ->
    try
        application:set_env(ctool, verify_oz_cert,
            application:get_env(?APP_NAME, verify_oz_cert, true)
        ),
        op_worker_sup:start_link(),
        ensure_correct_hostname(),
        ok = helpers_nif:init()
    catch
        _:Error ->
            ?error_stacktrace("Error in node_manager_plugin:before_init: ~p",
                [Error]),
            {error, cannot_start_node_manager_plugin}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:check_node_ip_address/0}.
%% @end
%%--------------------------------------------------------------------
-spec check_node_ip_address() -> IPV4Addr :: {A :: byte(), B :: byte(), C :: byte(), D :: byte()}.
check_node_ip_address() ->
    try
        {ok, IPBin} = oz_providers:check_ip_address(provider),
        {ok, IP} = inet_parse:ipv4_address(binary_to_list(IPBin)),
        IP
    catch T:M ->
        ?alert_stacktrace("Cannot check external IP of node, defaulting to 127.0.0.1 - ~p:~p", [T, M]),
        {127, 0, 0, 1}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Makes sure node hostname belongs to provider domain.
%% @end
%%--------------------------------------------------------------------
-spec ensure_correct_hostname() -> ok | no_return().
ensure_correct_hostname() ->
    Hostname = oneprovider:get_node_hostname(),
    Domain = oneprovider:get_provider_domain(),
    case string:join(tl(string:tokens(Hostname, ".")), ".") of
        Domain ->
            ok;
        _ ->
            ?error("Node hostname must be in provider domain. Check env conf. "
            "Current configuration:~nHostname: ~p~nDomain: ~p",
                [Hostname, Domain]),
            throw(wrong_hostname)
    end.
