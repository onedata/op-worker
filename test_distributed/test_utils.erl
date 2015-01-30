%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module contains functions useful for distributed tests
%%% e.g. function that sets environment before start, function that
%%% synchronizes virtual machines etc.
%%% @end
%%%--------------------------------------------------------------------
-module(test_utils).

-include("registered_names.hrl").
-include_lib("ctool/include/global_registry/gr_openid.hrl").
-include("test_utils.hrl").
-include("modules_and_args.hrl").

-define(GEN_SERV_CALL_TIMEOUT, 100000).

%% API
-export([ct_mock/4, wait_for_cluster_cast/0, wait_for_cluster_cast/1, wait_for_nodes_registration/1, wait_for_cluster_init/0,
    wait_for_cluster_init/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Evaluates meck:new(Module, [passthrough]) and meck:expect(Module, Method, Fun) on all
%% cluster nodes from given test Config.
%% For return value spac please see rpc:multicall/4.
%% Config shall be a proplist with at least {nodes, Nodes :: list()} entry.
%% @end
%%--------------------------------------------------------------------
-spec ct_mock(Config :: list(), Module :: atom(), Method :: atom(), Fun :: [term()]) ->
    {[term()], [term()]}.
ct_mock(Config, Module, Method, Fun) ->
    NodesUp = ?config(nodes, Config),
    {_, []} = rpc:multicall(NodesUp, meck, new, [Module, [passthrough, non_strict, unstick, no_link]]),
    {_, []} = rpc:multicall(NodesUp, meck, expect, [Module, Method, Fun]).

%%--------------------------------------------------------------------
%% @doc
%% Wait until cluster processes last cast.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_cluster_cast() -> ok | no_return().
wait_for_cluster_cast() ->
    wait_for_cluster_cast({global, ?CCM}).

%%--------------------------------------------------------------------
%% @doc
%% Wait until cluster processes last cast.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_cluster_cast(GenServ :: term()) -> ok | no_return().
wait_for_cluster_cast(GenServ) ->
    timer:sleep(100),
    Ans = try
        gen_server:call(GenServ, check, ?GEN_SERV_CALL_TIMEOUT)
          catch
              E1:E2 ->
                  {exception, E1, E2}
          end,
    ?assertEqual(ok, Ans).

%%--------------------------------------------------------------------
%% @doc
%% Wait until all nodes will be registered.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_nodes_registration(NodesNum :: integer()) -> ok | no_return().
wait_for_nodes_registration(NodesNum) ->
    wait_for_nodes_registration(NodesNum, 20).

%%--------------------------------------------------------------------
%% @doc
%% Wait until all nodes will be registered.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_nodes_registration(NodesNum :: integer(), TriesNum :: integer()) -> ok | no_return().
wait_for_nodes_registration(NodesNum, 0) ->
    ?assertEqual(NodesNum, check_nodes()),
    ok;
wait_for_nodes_registration(NodesNum, TriesNum) ->
    case check_nodes() of
        NodesNum -> ok;
        _ ->
            timer:sleep(500),
            wait_for_nodes_registration(NodesNum, TriesNum - 1)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get number of registered nodes.
%% @end
%%--------------------------------------------------------------------
-spec check_nodes() -> Ans when
    Ans :: integer() | {exception, E1, E2},
    E1 :: term(),
    E2 :: term().
check_nodes() ->
    try
        length(gen_server:call({global, ?CCM}, get_nodes, ?GEN_SERV_CALL_TIMEOUT))
    catch
        E1:E2 ->
            {exception, E1, E2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Check if cluster is initialized properly.
%% @end
%%--------------------------------------------------------------------
-spec check_init(ModulesNum :: integer()) -> Ans when
    Ans :: boolean() | {exception, E1, E2},
    E1 :: term(),
    E2 :: term().
check_init(ModulesNum) ->
    try
        {WList, StateNum} = gen_server:call({global, ?CCM}, get_workers, 2000),
        case length(WList) >= ModulesNum of
            true ->
                Nodes = gen_server:call({global, ?CCM}, get_nodes, 1000),
                {_, CStateNum} = gen_server:call({global, ?CCM}, get_callbacks, 1000),
                CheckNode = fun(Node, TmpAns) ->
                    StateNum2 = gen_server:call({?DISPATCHER_NAME, Node}, get_state_num, 1000),
                    {_, CStateNum2} = gen_server:call({?DISPATCHER_NAME, Node}, get_callbacks, 1000),
                    case (StateNum == StateNum2) and (CStateNum == CStateNum2) of
                        true -> TmpAns;
                        false -> [{wrong_state_nums, Node, StateNum, StateNum2, CStateNum, CStateNum2} | TmpAns]
                    end
                end,
                lists:foldl(CheckNode, true, Nodes);
            false ->
                {to_few_modules, WList}
        end
    catch
        E1:E2 ->
            {exception, E1, E2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Wait until cluster is initialized properly.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_cluster_init() -> Ans when
    Ans :: true | no_return().
wait_for_cluster_init() ->
    wait_for_cluster_init(0).

%%--------------------------------------------------------------------
%% @doc
%% Wait until cluster is initialized properly.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_cluster_init(ModulesNum :: integer()) -> Ans when
    Ans :: true | no_return().
wait_for_cluster_init(ModulesNum) ->
    wait_for_cluster_init(ModulesNum + length(?MODULES_WITH_ARGS), 50, []).

%%--------------------------------------------------------------------
%% @doc
%% Wait until cluster is initialized properly.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_cluster_init(ModulesNum :: integer(), TriesNum :: integer(), Errors :: list()) -> Ans when
    Ans :: true | no_return().
wait_for_cluster_init(ModulesNum, 0, Errors) ->
    case check_init(ModulesNum) of
        true -> true;
        E -> ?assert([E | Errors])
    end;
wait_for_cluster_init(ModulesNum, TriesNum, Errors) ->
    case check_init(ModulesNum) of
        true -> true;
        E ->
            timer:sleep(1000),
            wait_for_cluster_init(ModulesNum, TriesNum - 1, [E | Errors])
    end.
