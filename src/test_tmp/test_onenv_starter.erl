%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Functions used by ct test to start nodes using one_env for testing
%%% @end
%%%-------------------------------------------------------------------
-module(test_onenv_starter).
-author("Michal Stanisz").

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([prepare_test_environment/2, clean_environment/1]).

-define(DEFAULT_COOKIE, cluster_node).

prepare_test_environment(Config, Suite) ->
    application:start(yamerl),
    DataDir = ?config(data_dir, Config),
    CtTestRoot = filename:join(DataDir, ".."),
    ProjectRoot = filename:join(lists:takewhile(fun(Token) ->
        Token /= "test_distributed"
    end, filename:split(DataDir))),
    
    OnenvDir = filename:join([ProjectRoot, "one-env"]),
    OnenvScript = filename:join([OnenvDir, "onenv"]),
    OnenvConfig = filename:join([OnenvDir, "test_env_config.yaml"]),
    OnenvStartLogs = utils:cmd(["cd", "../../..", "&&", OnenvScript, "up", OnenvConfig]),
    ct:pal("~s", [OnenvStartLogs]),
    
    utils:cmd([OnenvScript, "wait"]),
    
    Status = utils:cmd([OnenvScript, "status"]),
    [StatusProplist] = yamerl:decode(Status),
    ct:pal("~s", [Status]),
    
    PodsProplist = proplists:get_value("pods", StatusProplist),
    NodesConfig = prepare_nodes_config(PodsProplist),

    [{PodName, _} | _] = PodsProplist,
    set_up_dns(OnenvScript, PodName),
    
    ping_nodes(NodesConfig),
    
    A = prepare_base_config(NodesConfig),
    A ++ [{onenv_script, OnenvScript}].

clean_environment(Config) ->
%%    ct:pal("clean environment: ~p", [Config]),
    ok.


prepare_nodes_config(PodsProplist) ->
    {P, N} = lists:foldl(fun({PodName, X}, {Pods, Nodes}) -> 
        H = proplists:get_value("hostname", X), 
        case proplists:get_value("service-type", X) of 
            "oneprovider" -> 
                WorkerNodes = maps:get(op_worker_nodes, Nodes, []), 
                PanelNodes = maps:get(op_panel_nodes, Nodes, []), 
                WorkerNode = list_to_atom("op_worker@" ++ H),
                NewNodes = Nodes#{
                    op_worker_nodes => [WorkerNode | WorkerNodes], 
                    op_panel_nodes => [list_to_atom("onepanel@" ++ H) | PanelNodes]
                },
                NewPods = Pods#{WorkerNode => PodName},
                {NewPods, NewNodes};
            "onezone" ->
                WorkerNodes = maps:get(oz_worker_nodes, Nodes, []),
                PanelNodes = maps:get(oz_panel_nodes, Nodes, []), 
                NewNodes = Nodes#{
                    oz_worker_nodes => [list_to_atom("oz_worker@" ++ H) | WorkerNodes], 
                    oz_panel_nodes => [list_to_atom("onepanel@" ++ H) | PanelNodes]
                },
                {Pods, NewNodes};
            _ -> {Pods, Nodes}
        end 
    end, {#{}, #{}}, PodsProplist),
    maps:to_list(N) ++ [{pods, maps:to_list(P)}].


ping_nodes(Config) ->
    NodesTypes = [op_worker_nodes, op_panel_nodes, oz_worker_nodes, oz_panel_nodes],
    erlang:set_cookie(node(), ?DEFAULT_COOKIE),
    lists:foreach(fun(NodeType) ->
        Nodes = proplists:get_value(NodeType, Config, []),
        lists:foreach(fun(Node) ->
            true == net_kernel:hidden_connect_node(Node)
        end, Nodes)
    end, NodesTypes).

set_up_dns(OnenvScript, PodName) ->
    ResolvConfEntry = utils:cmd([OnenvScript, "exec2", PodName, "head", "-n", "1", "/etc/resolv.conf"]),
    [] = os:cmd("echo \"" ++ ResolvConfEntry ++ "\" > /etc/resolv.conf"),
    ok.
    
prepare_base_config(NodesConfig) ->
    ProvidersNodes = lists:foldl(fun(Node, Acc) -> 
        ProviderId = rpc:call(Node, oneprovider, get_id, []),
        OtherNodes = maps:get(ProviderId, Acc, []),
        Acc#{ProviderId => [Node | OtherNodes]}
    end, #{}, proplists:get_value(op_worker_nodes, NodesConfig, [])),
    
    ProvidersList = maps:keys(ProvidersNodes),
    
    ProviderSpaces = lists:foldl(fun(ProviderId, Acc) ->
        [Node | _] = maps:get(ProviderId, ProvidersNodes),
        {ok, Spaces} = rpc:call(Node, provider_logic, get_spaces, []),
        Acc#{ProviderId => Spaces}
    end, #{}, ProvidersList),
    
    Sessions = lists:foldl(fun(ProviderId, Acc) ->
        [Node | _] = maps:get(ProviderId, ProvidersNodes),
        {ok, L} = rpc:call(Node, session, list, []),
        ProviderSessions = lists:filtermap(fun({_, SessId, SessionDoc, _, _,_,_,_,_,_}) ->
            case element(4, SessionDoc) of
                fuse -> 
                    {_,_,_,UserId} = element(5, SessionDoc),
                    {true, {UserId, SessId}};
                _ -> false
            end
        end, L),
        Acc#{ProviderId => ProviderSessions}
    end, #{}, ProvidersList),
    
    ProviderUsers = maps:fold(fun(ProviderId, UserSessionMapping, Acc) ->
        Users = lists:usort(lists:map(fun({UserId, _SessionId}) -> UserId end, UserSessionMapping)),
        Acc#{ProviderId => Users}
    end, #{}, Sessions),
    
    NodesConfig ++ 
        [{provider_nodes, maps:to_list(ProvidersNodes)}] ++ 
        [{providers, ProvidersList}] ++ 
        [{users, maps:to_list(ProviderUsers)}] ++ 
        [{sess_id, maps:to_list(Sessions)}] ++
        [{provider_spaces, maps:to_list(ProviderSpaces)}].
    
