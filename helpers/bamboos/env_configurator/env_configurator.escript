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
%%% This escript communicates with providers and zone to initialize an
%%% environment with providers, users, spaces and groups. It takes one
%%% argument - a JSON literal, that is dynamically generated by env.up script.
%%% It has the following structure:
%%%
%%% {
%%%     'oz_worker_node': 'gr1@gr1.1436272392.dev.docker',
%%%     'oz_cookie': 'cookie0',
%%%     'provider_domains': {
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
%%%             'providers': {
%%%                 'p1': {
%%%                     'storage': '/mnt/st1',
%%%                     'supported_size': 1000000000
%%%                 },
%%%                 'p2': {
%%%                     'storage': '/mnt/st2',
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
%%%             'providers': {
%%%                 'p1': {
%%%                     'storage': '/mnt/st1',
%%%                     'supported_size': 1000000000
%%%                 }
%%%             }
%%%         }
%%%     }
%%% }
%%% @end
%%%-------------------------------------------------------------------
-module(env_configurator).

% Hostname of the node started within this escript
-define(SCRIPT_NODE_HOSNTAME,
    begin
        Hostname = os:cmd("hostname -f") -- "\n",
        list_to_atom(lists:concat(["env_configurator_", os:getpid(), "@", Hostname]))
    end).
% Password for keyfiles created for providers
-define(DEFAULT_KEY_FILE_PASSWD, "").


%% API
-export([main/1]).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Main script function.
%% @end
%%--------------------------------------------------------------------
-spec main([list()]) -> ok.
main([InputJson, RegisterInOz, SetUpEntities]) ->
    try
        helpers_init(),
        {ok, _} = start_distribution(),
        Input = mochijson2:decode(InputJson, [{format, proplist}]),
        OZNode = bin_to_atom(proplists:get_value(<<"oz_node">>, Input, <<"">>)),
        OZCookie = bin_to_atom(proplists:get_value(<<"oz_cookie">>, Input, <<"">>)),
        Providers = proplists:get_value(<<"provider_domains">>, Input, <<"">>),
        Users = proplists:get_value(<<"users">>, Input, <<"">>),
        Groups = proplists:get_value(<<"groups">>, Input, <<"">>),
        Spaces = proplists:get_value(<<"spaces">>, Input, <<"">>),
        lists:foreach(
            fun({Provider, Props}) ->
                ProviderWorkersBin = proplists:get_value(<<"nodes">>, Props),
                ProviderWorkers = [bin_to_atom(P) || P <- ProviderWorkersBin],
                Cookie = bin_to_atom(proplists:get_value(<<"cookie">>, Props)),
                case list_to_atom(string:to_lower(RegisterInOz)) of
                    true ->
                        register_in_onezone(ProviderWorkers, Cookie, Provider);
                    _ -> ok
                end,
                create_space_storage_mapping(hd(ProviderWorkers), Cookie, Spaces, Provider)
            end, Providers),
        case list_to_atom(string:to_lower(SetUpEntities)) of
            true ->
                case call_node(OZNode, OZCookie, dev_utils, set_up_test_entities,
                    [Users, Groups, Spaces]) of
                    ok ->
                        ok;
                    Other ->
                        io:format("dev_utils:set_up_test_entities returned: ~p~n",
                            [Other]),
                        throw(error)
                end,
                io:format("Global configuration applied sucessfully!~n");
            _ -> ok
        end,
        halt(0)
    catch
        T:M ->
            io:format("Error in ~s - ~p:~p~n~p~n", [escript:script_name(), T, M, erlang:get_stacktrace()]),
            halt(1)
    end;

main(_) ->
    io:format("Usage: ~s <input_json> <register_in_oz> <set_up_test_entities>~n",
        [escript:script_name()]),
    halt(0).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Start the net kernel with long node name.
%% @end
%%--------------------------------------------------------------------
-spec start_distribution() -> {ok, pid()}.
start_distribution() ->
    {ok, _Pid} = net_kernel:start([?SCRIPT_NODE_HOSNTAME, longnames]).


%%--------------------------------------------------------------------
%% @doc
%% Calls an erlang node, given the cookie that it uses.
%% @end
%%--------------------------------------------------------------------
-spec call_node(Node :: node(), Cookie :: atom(), Module :: atom(), Function :: function(), Args :: [term()]) -> term().
call_node(Node, Cookie, Module, Function, Args) ->
    erlang:set_cookie(node(), Cookie),
    true = net_kernel:hidden_connect_node(Node),
    rpc:call(Node, Module, Function, Args).


%%--------------------------------------------------------------------
%% @doc
%% Loads helper modules.
%% @end
%%--------------------------------------------------------------------
-spec helpers_init() -> ok.
helpers_init() ->
    SrcDir = filename:join(get_escript_dir(), "src"),
    {ok, Modules} = file:list_dir(SrcDir),
    lists:foreach(fun(Module) ->
        compile_and_load_module(filename:join(SrcDir, Module))
    end, Modules).


%%--------------------------------------------------------------------
%% @doc
%% Compiles and loads module into erlang VM.
%% @end
%%--------------------------------------------------------------------
-spec compile_and_load_module(File :: string()) -> ok.
compile_and_load_module(File) ->
    {ok, ModuleName, Binary} = compile:file(File, [report, binary]),
    {module, _} = code:load_binary(ModuleName, File, Binary),
    ok.


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


%%--------------------------------------------------------------------
%% @doc
%% Registers provider in OZ.
%% @end
%%--------------------------------------------------------------------
-spec register_in_onezone(Workers :: [node()], Cookie :: atom(),
    Provider :: binary()) -> ok.
register_in_onezone(Workers, Cookie, Provider) ->
    {ok, Provider} = call_node(hd(Workers), Cookie, oneprovider,
        register_in_oz_dev, [Workers, ?DEFAULT_KEY_FILE_PASSWD, Provider]),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Creates space storage mapping in provider database.
%% @end
%%--------------------------------------------------------------------
-spec create_space_storage_mapping(Worker :: node(), Cookie :: atom(),
    Spaces :: proplists:proplist(), ProviderDomain :: binary()) -> ok.
create_space_storage_mapping(Worker, Cookie, Spaces, ProviderDomain) ->
    lists:foreach(fun({SpaceId, Props}) ->
        SpaceProviders = proplists:get_value(<<"providers">>, Props),
        case proplists:get_value(ProviderDomain, SpaceProviders) of
            undefined ->
                ok;
            ProviderSupportInfo ->
                StorageName = proplists:get_value(<<"storage">>, ProviderSupportInfo),
                {ok, Storage} = call_node(Worker, Cookie, storage, select, [StorageName]),
                StorageId = call_node(Worker, Cookie, storage, get_id, [Storage]),
                {ok, _} = call_node(Worker, Cookie, space_storage, add, [SpaceId, StorageId])
        end
    end, Spaces).
