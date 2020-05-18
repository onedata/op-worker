%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Functions used by ct test to start and manage environment 
%%% using onenv for testing.
%%% fixme move to ctool
%%% @end
%%%-------------------------------------------------------------------
-module(test_onenv_starter).
-author("Michal Stanisz").

-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/aai/caveats.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([prepare_test_environment/2, clean_environment/1]).

-define(DEFAULT_COOKIE, cluster_node).

-type path() :: string().

-spec prepare_test_environment(test_config:config(), string()) -> test_config:config().
prepare_test_environment(Config, _Suite) ->
    application:start(yamerl),
    DataDir = test_config:get_custom(Config, data_dir),
    ProjectRoot = filename:join(lists:takewhile(fun(Token) ->
        Token /= "test_distributed"
    end, filename:split(DataDir))),
    PathToSources = os:getenv("path_to_sources"),
    AbsPathToSources = filename:join([ProjectRoot, PathToSources]),
    
    ScenarioName = test_config:get_scenario(Config, "1op"),
    
    % fixme to functions
    % Set custom envs
    CustomEnvs = test_config:get_envs(Config),
    CustomConfigsPaths = add_custom_configs(AbsPathToSources, CustomEnvs),

    % Start environment
    OnenvScript = filename:join([ProjectRoot, "one-env", "onenv"]),
    ScenarioPath = filename:join([ProjectRoot, "test_distributed", "onenv_scenarios", ScenarioName ++ ".yaml"]),
    ct:pal("Starting onenv scenario ~p~n~n~p", [ScenarioName, ScenarioPath]),
    StartCmd = ["cd", "../../..", "&&", OnenvScript, "up", "--path-to-sources", PathToSources, ScenarioPath],
    OnenvStartLogs = utils:cmd(StartCmd),
    ct:pal("~s", [OnenvStartLogs]),
    
    utils:cmd([OnenvScript, "wait", "--timeout", "600"]),
    
    Status = utils:cmd([OnenvScript, "status"]),
    [StatusProplist] = yamerl:decode(Status),
    ct:pal("~s", [Status]),
    case proplists:get_value("ready", StatusProplist) of
        false -> throw(environment_not_ready);
        true -> ok
    end,
    
    PodsProplist = proplists:get_value("pods", StatusProplist),
    NodesConfig = prepare_nodes_config(PodsProplist),

    add_entries_to_etc_hosts(PodsProplist),
    connect_nodes(NodesConfig),
    
    BaseConfig = prepare_base_test_config(NodesConfig),
    test_config:set_many(BaseConfig, [
        {set_onenv_script_path, [OnenvScript]},
        [op_worker_script, script_path(AbsPathToSources, "op_worker")], 
        [cluster_manager_script, script_path(AbsPathToSources, "cluster_manager")],
        [custom_configs, CustomConfigsPaths]
    ]).


-spec clean_environment(test_config:config()) -> ok.
clean_environment(Config) ->
    OnenvScript = test_config:get_onenv_script_path(Config),
    CustomConfigsPaths = test_config:get_custom(Config, custom_configs),
    lists:foreach(fun file:delete/1, CustomConfigsPaths),
    utils:cmd([OnenvScript, "clean", "--all", "--persistent-volumes"]),
    ok.


% fixme doc
%% @private
-spec prepare_nodes_config(test_config:config()) -> test_config:config().
prepare_nodes_config(PodsProplist) ->
    % fixme add zone to pods
    % fixme variable names
    {P, N} = lists:foldl(fun({PodName, X}, {Pods, Nodes}) -> 
        H = proplists:get_value("hostname", X), 
        case proplists:get_value("service-type", X) of 
            "oneprovider" -> 
                WorkerNodes = maps:get(op_worker_nodes, Nodes, []), 
                PanelNodes = maps:get(op_panel_nodes, Nodes, []),
                CmNodes = maps:get(cm_nodes, Nodes, []), 
                WorkerNode = list_to_atom("op_worker@" ++ H),
                NewNodes = Nodes#{
                    op_worker_nodes => [WorkerNode | WorkerNodes], 
                    op_panel_nodes => [list_to_atom("onepanel@" ++ H) | PanelNodes],
                    cm_nodes => [list_to_atom("cluster_manager@" ++ H) | CmNodes]
                },
                NewPods = Pods#{WorkerNode => PodName},
                {NewPods, NewNodes};
            "onezone" ->
                WorkerNodes = maps:get(oz_worker_nodes, Nodes, []),
                PanelNodes = maps:get(oz_panel_nodes, Nodes, []),
                CmNodes = maps:get(cm_nodes, Nodes, []), 
                NewNodes = Nodes#{
                    oz_worker_nodes => [list_to_atom("oz_worker@" ++ H) | WorkerNodes],
                    oz_panel_nodes => [list_to_atom("onepanel@" ++ H) | PanelNodes],
                    cm_nodes => [list_to_atom("cluster_manager@" ++ H) | CmNodes]
                },
                {Pods, NewNodes};
            _ -> {Pods, Nodes}
        end 
    end, {#{}, #{}}, PodsProplist),
    maps:to_list(N) ++ [{pods, maps:to_list(P)}].


%% @private
-spec connect_nodes(test_config:config()) -> ok.
connect_nodes(Config) ->
    NodesTypes = [cm_nodes, op_worker_nodes, op_panel_nodes, oz_worker_nodes, oz_panel_nodes],
    erlang:set_cookie(node(), ?DEFAULT_COOKIE),
    lists:foreach(fun(NodeType) ->
        Nodes = proplists:get_value(NodeType, Config, []),
        lists:foreach(fun(Node) ->
            true = net_kernel:connect_node(Node)
        end, Nodes)
    end, NodesTypes).


% fixme use nodes_config
%% @private
-spec add_entries_to_etc_hosts(test_config:config()) -> ok.
add_entries_to_etc_hosts(PodsConfig) ->
    HostsEntries = lists:foldl(fun({_ServiceName, ServiceConfig}, Acc0) ->
        ServiceType = proplists:get_value("service-type", ServiceConfig),
        case lists:member(ServiceType, ["onezone", "oneprovider"]) of
            true ->
                Ip = proplists:get_value("ip", ServiceConfig),

                Acc1 = case proplists:get_value("domain", ServiceConfig, undefined) of
                    undefined -> Acc0;
                    Domain -> [{Domain, Ip} | Acc0]
                end,
                case proplists:get_value("hostname", ServiceConfig, undefined) of
                    undefined -> Acc1;
                    Hostname -> [{Hostname, Ip} | Acc1]
                end;
            false ->
                Acc0
        end
    end, [], PodsConfig),

    {ok, File} = file:open("/etc/hosts", [append]),

    lists:foreach(fun({DomainOrHostname, Ip}) ->
        io:fwrite(File, "~s ~s~n", [Ip, DomainOrHostname])
    end, HostsEntries),

    file:close(File).


%% @private
-spec prepare_base_test_config(test_config:config()) -> test_config:config().
prepare_base_test_config(NodesConfig) ->
    ProvidersNodes = lists:foldl(fun(Node, Acc) -> 
        ProviderId = rpc:call(Node, oneprovider, get_id, []),
        OtherNodes = maps:get(ProviderId, Acc, []),
        Acc#{ProviderId => [Node | OtherNodes]}
    end, #{}, proplists:get_value(op_worker_nodes, NodesConfig, [])),
    
    ProviderPanels = lists:foldl(fun(Node, Acc) ->
        [WorkerNode | _] = rpc:call(Node, service_op_worker, get_nodes, []),
        ProviderId = rpc:call(WorkerNode, oneprovider, get_id, []),
        OtherNodes = maps:get(ProviderId, Acc, []),
        Acc#{ProviderId => [Node | OtherNodes]}
    end, #{}, proplists:get_value(op_panel_nodes, NodesConfig, [])),
    
    PrimaryCm = maps:fold(fun(ProviderId, [PanelNode | _], Acc) ->
        {ok, Hostname} = rpc:call(PanelNode, service_cluster_manager,  get_main_host, []),
        Acc#{ProviderId => list_to_atom("cluster_manager@" ++ Hostname)}
    end, #{}, ProviderPanels),
    
    ProvidersList = maps:keys(ProvidersNodes),

    ProviderSpaces = lists:foldl(fun(ProviderId, Acc) ->
        [Node | _] = maps:get(ProviderId, ProvidersNodes),
        {ok, Spaces} = rpc:call(Node, provider_logic, get_spaces, []),
        Acc#{ProviderId => Spaces}
    end, #{}, ProvidersList),
    
    ProviderUsers = lists:foldl(fun(ProviderId, Acc) ->
        [Node | _] = maps:get(ProviderId, ProvidersNodes),
        {ok, Users} = rpc:call(Node, provider_logic, get_eff_users, []),
        Acc#{ProviderId => Users}
    end, #{}, ProvidersList),

    [OzNode | _ ] = kv_utils:get(oz_worker_nodes, NodesConfig),
    Sessions = maps:map(fun(ProviderId, Users) ->
        [Node | _] = maps:get(ProviderId, ProvidersNodes),
        lists:map(fun(UserId) ->
            {ok, SessId} = setup_user_session(UserId, OzNode, Node),
            {UserId, SessId}
        end, Users)
    end, ProviderUsers),
    
    test_config:set_many(NodesConfig, [
        [provider_nodes, ProvidersNodes],
        [providers, ProvidersList],
        [provider_panels, ProviderPanels],
        [primary_cm, PrimaryCm],
        [users, ProviderUsers], 
        [sess_id, Sessions],
        [provider_spaces, ProviderSpaces]
    ]).


%% @private
-spec setup_user_session(UserId :: binary(), OzwNode :: node(), OpwNode :: node()) -> 
    {ok, SessId :: binary()}.
setup_user_session(UserId, OzwNode, OpwNode) ->
    % fixme more time?
    TimeCaveat = #cv_time{valid_until = rpc:call(OzwNode, time_utils, cluster_time_seconds, []) + 100},
    {ok, AccessToken} =
        rpc:call(OzwNode, token_logic, create_user_temporary_token,
            [?ROOT, UserId, #{<<"caveats">> => [TimeCaveat]}]),
    {ok, SerializedAccessToken} = rpc:call(OzwNode, tokens, serialize, [AccessToken]),
    Nonce = base64:encode(crypto:strong_rand_bytes(8)),
    Identity = ?SUB(user, UserId),
    Credentials =
        rpc:call(OpwNode, auth_manager, build_token_credentials,
            [SerializedAccessToken, undefined, undefined, undefined, allow_data_access_caveats]),
    
    rpc:call(OpwNode, session_manager, reuse_or_create_fuse_session, [Nonce, Identity, Credentials]).


%% @private
-spec add_custom_configs(path(), [{Component :: atom(), proplists:proplist()}]) -> [path()].
add_custom_configs(PathToSources, CustomEnvs) ->
    lists:foldl(fun({Component, Envs}, Acc) ->
        Path = test_custom_config_path(PathToSources, atom_to_list(Component)),
        file:write_file(Path, io_lib:format("~p.", [Envs])),
        [Path | Acc]
    end, [], CustomEnvs).


%% @private
-spec script_path(path(), string()) -> path().
script_path(PathToSources, Service) ->
    SourcesRelPath = sources_rel_path(PathToSources, Service),
    filename:join([SourcesRelPath, Service, "bin", Service]).


%% @private
-spec test_custom_config_path(path(), string()) -> path().
test_custom_config_path(PathToSources, Service) ->
    SourcesRelPath = sources_rel_path(PathToSources, Service),
    filename:join([SourcesRelPath, Service, "etc", "config.d", "ct_test_custom.config"]).


%% @private
% fixme parse output to find sources path
-spec sources_rel_path(path(), string()) -> path().
sources_rel_path(PathToSources, Service) ->
    Service1 = re:replace(Service, "_", "-", [{return, list}]),
    filename:join([PathToSources, Service1, "_build", "default", "rel"]).
