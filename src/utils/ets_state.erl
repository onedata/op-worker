%%%-------------------------------------------------------------------
%%% @author michal
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Mar 2019 11:36
%%%-------------------------------------------------------------------
-module(ets_state).
-author("michal").

%% API
-export([init/1, terminate/1, save/4, get/3, delete/3]).


init(Name) ->
    ets:new(Name, [set, public, named_table]).

terminate(Name) ->
    true = ets:delete(Name),
    ok.

save(Name, Entity, Key, Value) ->
    Nodes = consistent_hasing:get_all_nodes(),
    lists:foreach(fun(Node) ->
        true = rpc:call(Node, ets, insert, [Name, {{Entity, Key}, Value}])
    end, Nodes),
    ok.

get(Name, Entity, Key) ->
    case ets:lookup(Name, {Entity, Key}) of
        [{_, Value}] -> {ok, Value};
        _ -> error
    end.

delete(Name, Entity, Key) ->
    Nodes = consistent_hasing:get_all_nodes(),
    lists:foreach(fun(Node) ->
        true = rpc:call(Node, ets, delete, [Name, {{Entity, Key}}])
    end, Nodes),
    ok.