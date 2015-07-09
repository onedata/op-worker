#!/usr/bin/env escript
%% -*- erlang -*-
%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This escript communicates with providers and GR to initialize an
%%% environment with providers, users, spaces and groups. It takes one
%%% argument - a JSON literal with the following structure:
%%%
%%% {
%%%     'gr_node': 'gr1@gr1.1436272392.dev.docker',
%%%     'gr_cookie': 'cookie0',
%%%     'providers': {
%%%         'p1': {
%%%             'nodes': [
%%%                 'worker1_p1@worker1_p1.1436272392.dev.docker'
%%%             ],
%%%             'cookie': 'cookie1'
%%%         },
%%%         'p2': {
%%%             'nodes': [
%%%                 'worker1_p2@worker1_p2.1436279125.dev.docker',
%%%                 'worker2_p2@worker2_p2.1436279125.dev.docker'
%%%             ],
%%%             'cookie': 'cookie1'
%%%         }
%%%     },
%%%     'users': {
%%%         'u1': {
%%%             'default_space': 's1'
%%%         },
%%%         'u2': {
%%%             'default_space': 's2'
%%%         },
%%%         'u3': {
%%%             'default_space': 's1'
%%%         }
%%%     },
%%%     'groups': {
%%%         'g1': {
%%%             'users': [
%%%                 'u1',
%%%                 'u3'
%%%             ]
%%%         },
%%%         'g2': {
%%%             'users': [
%%%                 'u2'
%%%             ]
%%%         }
%%%     },
%%%     'spaces': {
%%%         's1': {
%%%             'users': [
%%%                 'u1',
%%%                 'u3'
%%%             ],
%%%             'groups': [
%%%                 'g1'
%%%             ],
%%%             'providers': [
%%%                 {
%%%                     'provider': 'p1',
%%%                     'supported_size': 1000000000
%%%                 },
%%%                 {
%%%                     'provider': 'p2',
%%%                     'supported_size': 1000000000
%%%                 }
%%%             ]
%%%         },
%%%         's2': {
%%%             'users': [
%%%                 'u2'
%%%             ],
%%%             'groups': [
%%%                 'g2'
%%%             ],
%%%             'providers': [
%%%                 {
%%%                     'provider': 'p1',
%%%                     'supported_size': 1000000000
%%%                 }
%%%             ]
%%%         }
%%%     }
%%% }
%%% @end
%%%-------------------------------------------------------------------
-module(env_configurator).

-define(SCRIPT_NODE_HOSNTAME,
    begin
        Hostname = os:cmd("hostname -f") -- "\n",
        list_to_atom(lists:concat(["env_configurator_", os:getpid(), "@", Hostname]))
    end).
-define(DEFAULT_KEY_FILE_PASSWD, "").
-define(EXIT_FAILURE_CODE, 1).


% Prints a single variable
-define(dump(_Arg), io:format(user, "[DUMP] ~s: ~p~n~n", [??_Arg, _Arg])).

%% API
-export([main/1]).

main([InputJson]) ->
    try
        helpers_init(),
        start_distribution(),
        Input = mochijson2:decode(InputJson, [{format, proplist}]),
        GRNode = bin_to_atom(proplists:get_value(<<"gr_node">>, Input)),
        GRCookie = bin_to_atom(proplists:get_value(<<"gr_cookie">>, Input)),
        Providers = proplists:get_value(<<"providers">>, Input),
        Users = proplists:get_value(<<"users">>, Input),
        Groups = proplists:get_value(<<"groups">>, Input),
        Spaces = proplists:get_value(<<"spaces">>, Input),
        lists:foreach(
            fun({Provider, Props}) ->
                ProviderWorkersBin = proplists:get_value(<<"nodes">>, Props),
                ProviderWorkers = [bin_to_atom(P) || P <- ProviderWorkersBin],
                Cookie = bin_to_atom(proplists:get_value(<<"cookie">>, Props)),
                {ok, Provider} = call_node(hd(ProviderWorkers), Cookie, oneprovider, register_in_gr_dev,
                    [ProviderWorkers, ?DEFAULT_KEY_FILE_PASSWD, Provider])
            end, Providers),
        call_node(GRNode, GRCookie, dev_utils, set_up_test_entities, [Users, Groups, Spaces]),
        io:format("Global configuration applied sucessfully!~n"),
        ok
    catch
        T:M ->
            io:format("Error in ~s - ~p:~p~n", [escript:script_name(), T, M])
    end;

main(_) ->
    io:format("Usage: ~s <input_json>~n", [escript:script_name()]).


%%%===================================================================
%%% Helper functions
%%%===================================================================

start_distribution() ->
    {ok, _Pid} = net_kernel:start([?SCRIPT_NODE_HOSNTAME, longnames]).


call_node(Node, Cookie, Module, Function, Args) ->
    erlang:set_cookie(node(), Cookie),
    case {net_kernel:connect_node(Node), net_adm:ping(Node)} of
        {true, pong} ->
            ok;
        {_, pang} ->
            io:format("Node ~p not responding to pings.~n", [Node]),
            init:stop(0)
    end,
    rpc:call(Node, Module, Function, Args).


%%--------------------------------------------------------------------
%% @doc
%% Loads helper modules.
%% @end
%%--------------------------------------------------------------------
-spec helpers_init() -> ok.
helpers_init() ->
    true = code:add_path(filename:join(get_escript_dir(), "ebin")).


%%--------------------------------------------------------------------
%% @doc
%% Get path of current escript dir.
%% @end
%%--------------------------------------------------------------------
-spec get_escript_dir() -> string().
get_escript_dir() ->
    filename:dirname(escript:script_name()).


%%--------------------------------------------------------------------
%% @doc
%% Get path of current escript dir.
%% @end
%%--------------------------------------------------------------------
-spec bin_to_atom(Bin :: binary()) -> atom().
bin_to_atom(Bin) ->
    list_to_atom(binary_to_list(Bin)).


