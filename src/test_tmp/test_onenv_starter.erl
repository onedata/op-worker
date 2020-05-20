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

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([prepare_test_environment/2, clean_environment/1]).

-define(DEFAULT_COOKIE, cluster_node).

-type path() :: string().
-type component() :: worker | onepanel | cluster_manager.

-spec prepare_test_environment(test_config:config(), string()) -> test_config:config().
prepare_test_environment(Config0, _Suite) ->
    application:start(yamerl),
    DataDir = test_config:get_custom(Config0, data_dir),
    ProjectRoot = filename:join(lists:takewhile(fun(Token) ->
        Token /= "test_distributed"
    end, filename:split(DataDir))),
    OnenvScript = filename:join([ProjectRoot, "one-env", "onenv"]),
    PathToSources = os:getenv("path_to_sources"),
    AbsPathToSources = filename:join([ProjectRoot, PathToSources]),
    
    % Dummy first call to onenv to setup configs. 
    % Path to sources must be proivided in first onenv call that creates one-env docker
    utils:cmd([OnenvScript, "status", "--path-to-sources", AbsPathToSources]), 
    Sources = utils:cmd(["cd", ProjectRoot, "&&", OnenvScript, "find_sources"]),
    ct:pal("~nUsing sources from:~n~n~s", [Sources]),
    
    Config = test_config:set_many(Config0, [
        {set_onenv_script_path, [OnenvScript]},
        {set_project_root_path, [ProjectRoot]}
    ]),
    
    PodsProplist = start_environment(Config),
    add_entries_to_etc_hosts(PodsProplist),
    NodesConfig = prepare_nodes_config(Config, PodsProplist),
    connect_nodes(NodesConfig),
    
    test_config:set_many(NodesConfig, [
        [op_worker_script, script_path(NodesConfig, "op_worker")], 
        [cluster_manager_script, script_path(NodesConfig, "cluster_manager")]
    ]).


%% @private
-spec start_environment(test_config:config()) -> proplists:proplist().
start_environment(Config) ->
    ScenarioName = test_config:get_scenario(Config),
    OnenvScript = test_config:get_onenv_script_path(Config),
    ProjectRoot = test_config:get_project_root_path(Config),
    CustomEnvs = test_config:get_custom_envs(Config),
    
    CustomConfigsPaths = add_custom_configs(Config, CustomEnvs),
    
    ScenarioPath = filename:join([ProjectRoot, "test_distributed", "onenv_scenarios", ScenarioName ++ ".yaml"]),
    ct:pal("Starting onenv scenario ~p~n~n~p", [ScenarioName, ScenarioPath]),
    StartCmd = ["cd", ProjectRoot, "&&", OnenvScript, "up", ScenarioPath],
    OnenvStartLogs = utils:cmd(StartCmd),
    ct:pal("~s", [OnenvStartLogs]),

    lists:foreach(fun file:delete/1, CustomConfigsPaths),
    utils:cmd([OnenvScript, "wait", "--timeout", "900"]),
    
    Status = utils:cmd([OnenvScript, "status"]),
    [StatusProplist] = yamerl:decode(Status),
    ct:pal("~s", [Status]),
    case proplists:get_value("ready", StatusProplist) of
        false ->
            ok = clean_environment(Config),
            throw(environment_not_ready);
        true -> ok
    end,
    proplists:get_value("pods", StatusProplist).


-spec clean_environment(test_config:config()) -> ok.
clean_environment(Config) ->
    OnenvScript = test_config:get_onenv_script_path(Config),
    PrivDir = test_config:get_custom(Config, priv_dir),
    
    utils:cmd([OnenvScript, "export", PrivDir]),
    utils:cmd([OnenvScript, "clean", "--all", "--persistent-volumes"]),
    ok.


%% @private
-spec prepare_nodes_config(test_config:config(), proplists:proplist()) -> test_config:config().
prepare_nodes_config(Config, PodsProplist) ->
    lists:foldl(fun({PodName, X}, TmpConfig) -> 
        Hostname = proplists:get_value("hostname", X), 
        case proplists:get_value("service-type", X) of 
            "oneprovider" -> 
                NodesAndKeys = prepare_nodes(op, Hostname),
                add_nodes_to_config(NodesAndKeys, PodName, TmpConfig);
            "onezone" ->
                NodesAndKeys = prepare_nodes(oz, Hostname),
                add_nodes_to_config(NodesAndKeys, PodName, TmpConfig);
            _ -> 
                TmpConfig
        end 
    end, Config, PodsProplist).


%% @private
-spec prepare_nodes(oz | op, string()) -> [{node(), test_config:key()}].
prepare_nodes(ServiceType, Hostname) ->
    lists:map(fun(NodeType) ->
        NodeName = list_to_atom(node_name_prefix(NodeType, ServiceType) ++ "@" ++ Hostname),
        Key = service_to_config_key(NodeType, ServiceType),
        {NodeName, Key}
    end, [worker, onepanel, cluster_manager]).


%% @private
-spec add_nodes_to_config([{node(), test_config:key()}], PodName :: string(), test_config:config()) ->
    test_config:config().
add_nodes_to_config(NodesAndKeys, PodName, Config) ->
    lists:foldl(fun({Node, Key}, TmpConfig) ->
        PrevNodes = test_config:get_custom(TmpConfig, Key, []),
        PrevPods = test_config:get_custom(TmpConfig, pods, #{}),
        test_config:set_many(TmpConfig, [
            [Key, [Node | PrevNodes]],
            [pods, PrevPods#{Node => PodName}]
        ])
    end, Config, NodesAndKeys).


%% @private
-spec connect_nodes(test_config:config()) -> ok.
connect_nodes(Config) ->
    NodesTypes = [cm_nodes, op_worker_nodes, op_panel_nodes, oz_worker_nodes, oz_panel_nodes],
    erlang:set_cookie(node(), ?DEFAULT_COOKIE),
    lists:foreach(fun(NodeType) ->
        Nodes = test_config:get_custom(Config, NodeType, []),
        lists:foreach(fun(Node) ->
            true = net_kernel:connect_node(Node)
        end, Nodes)
    end, NodesTypes).


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
-spec add_custom_configs(test_config:config(), [{Component :: atom(), proplists:proplist()}]) -> [path()].
add_custom_configs(Config, CustomEnvs) ->
    lists:foldl(fun({Component, Envs}, Acc) ->
        Path = test_custom_config_path(Config, atom_to_list(Component)),
        file:write_file(Path, io_lib:format("~p.", [Envs])),
        [Path | Acc]
    end, [], CustomEnvs).


%% @private
-spec script_path(test_config:config(), test_config:service_as_list()) -> path().
script_path(Config, Service) ->
    SourcesRelPath = sources_rel_path(Config, Service),
    filename:join([SourcesRelPath, Service, "bin", Service]).


%% @private
-spec test_custom_config_path(test_config:config(), test_config:service_as_list()) -> path().
test_custom_config_path(Config, Service) ->
    SourcesRelPath = sources_rel_path(Config, Service),
    filename:join([SourcesRelPath, Service, "etc", "config.d", "ct_test_custom.config"]).


%% @private
-spec sources_rel_path(test_config:config(), test_config:service_as_list()) -> path().
sources_rel_path(Config, Service) ->
    Service1 = re:replace(Service, "_", "-", [{return, list}]),
    
    OnenvScript = test_config:get_onenv_script_path(Config),
    ProjectRoot = test_config:get_project_root_path(Config),
    SourcesYaml = utils:cmd(["cd", ProjectRoot, "&&", OnenvScript, "find_sources"]),
    [Sources] = yamerl:decode(SourcesYaml),
    
    filename:join([kv_utils:get([Service1], Sources), "_build", "default", "rel"]).


%% @private
-spec service_to_config_key(component(), op | oz) -> test_config:key().
service_to_config_key(worker, op) -> op_worker_nodes;
service_to_config_key(worker, oz) -> oz_worker_nodes;
service_to_config_key(onepanel, op) -> op_panel_nodes;
service_to_config_key(onepanel, oz) -> oz_panel_nodes;
service_to_config_key(cluster_manager, _) -> cm_nodes.


%% @private
-spec node_name_prefix(component(), op | oz) -> string().
node_name_prefix(worker, Type) -> atom_to_list(Type) ++ "_worker";
node_name_prefix(Component, _) -> atom_to_list(Component).
