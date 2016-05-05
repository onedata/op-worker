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

-behaviour(node_manager_plugin_behaviour).

-include("global_definitions.hrl").
-include_lib("cluster_worker/include/elements/node_manager/node_manager.hrl").
-include_lib("cluster_worker/include/elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").

%% node_manager_plugin_behaviour callbacks
-export([on_init/1, on_terminate/2, on_code_change/3,
    handle_call_extension/3, handle_cast_extension/2, handle_info_extension/2,
    modules_with_args/0, listeners/0, cm_nodes/0, db_nodes/0, check_node_ip_address/0, app_name/0]).

%%%===================================================================
%%% node_manager_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns the name of the application that bases on cluster worker.
%% @end
%%--------------------------------------------------------------------
-spec app_name() -> {ok, Name :: atom()}.
app_name() ->
    {ok, op_worker}.

%%--------------------------------------------------------------------
%% @doc
%% List cluster manager nodes to be used by node manager.
%% @end
%%--------------------------------------------------------------------
-spec cm_nodes() -> {ok, Nodes :: [atom()]} | undefined.
cm_nodes() ->
    application:get_env(?APP_NAME, cm_nodes).

%%--------------------------------------------------------------------
%% @doc
%% List db nodes to be used by node manager.
%% @end
%%--------------------------------------------------------------------
-spec db_nodes() -> {ok, Nodes :: [atom()]} | undefined.
db_nodes() ->
    application:get_env(?APP_NAME, db_nodes).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% {@link node_manager_plugin_behaviour} callback listeners/0.
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
%% @private
%% @doc
%% {@link node_manager_plugin_behaviour} callback modules_with_args/0.
%% @end
%%--------------------------------------------------------------------
-spec modules_with_args() -> Models :: [{atom(), [any()]}].
modules_with_args() -> node_manager:cluster_worker_modules() ++ [
    {session_manager_worker, [
        {supervisor_spec, session_manager_worker:supervisor_spec()},
        {supervisor_child_spec, session_manager_worker:supervisor_child_spec()}
    ]},
    {subscriptions_worker, []},
    {fslogic_worker, []},
    {dbsync_worker, []}
].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% {@link node_manager_plugin_behaviour}  callback on_init/0.
%% @end
%%--------------------------------------------------------------------
-spec on_init(Args :: term()) -> Result :: ok | {error, Reason :: term()}.
on_init([]) ->
    try
        ensure_correct_hostname(),

        %% Load NIFs
        ok = helpers_nif:init(),
        ok = luma_nif:init(),
        ok
    catch
        _:Error ->
            ?error_stacktrace("Cannot start node_manager plugin: ~p", [Error]),
            {error, cannot_start_node_manager_plugin}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call_extension(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
    Result :: {reply, Reply, NewState}
    | {reply, Reply, NewState, Timeout}
    | {reply, Reply, NewState, hibernate}
    | {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason, Reply, NewState}
    | {stop, Reason, NewState},
    Reply :: nagios_handler:healthcheck_response() | term(),
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity,
    Reason :: term().

handle_call_extension(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast_extension(Request :: term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.

handle_cast_extension(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info_extension(Info :: timeout | term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.

handle_info_extension(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec on_terminate(Reason, State :: term()) -> Any :: term() when
    Reason :: normal
    | shutdown
    | {shutdown, term()}
    | term().
on_terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec on_code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
    Result :: {ok, NewState :: term()} | {error, Reason :: term()},
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term().
on_code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks IP address of this node by asking GR. If the check cannot be performed,
%% it assumes a 127.0.0.1 address and logs an alert.
%% @end
%%--------------------------------------------------------------------
-spec check_node_ip_address() -> IPV4Addr :: {A :: byte(), B :: byte(), C :: byte(), D :: byte()}.
check_node_ip_address() ->
    try
        application:set_env(ctool, verify_oz_cert, false), % @todo VFS-1572
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
