%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(cluster_manager_utils).
-author("Tomasz Lichon").

-include_lib("ctool/include/logging.hrl").

%% API
-export([node_to_ip/1, aggregate_over_first_element/1, average/1]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolve ipv4 address of node.
%% @end
%%--------------------------------------------------------------------
-spec node_to_ip(Node :: atom()) -> inet:ip4_address() | unknownaddress.
node_to_ip(Node) ->
    StrNode = atom_to_list(Node),
    AddressWith@ = lists:dropwhile(fun(Char) -> Char =/= $@ end, StrNode),
    Address = lists:dropwhile(fun(Char) -> Char =:= $@ end, AddressWith@),
    case inet:getaddr(Address, inet) of
        {ok, Ip} -> Ip;
        {error, Error} ->
            ?error("Cannot resolve ip address for node ~p, error: ~p", [Node, Error]),
            unknownaddress
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% aggregate list over first element of tuple
%% @end
%%--------------------------------------------------------------------
-spec aggregate_over_first_element(List :: [{K,V}]) -> [{K,[V]}].
aggregate_over_first_element(List) ->
    lists:reverse(
        lists:foldl(fun({Key, Value}, []) -> [{Key, [Value]}];
            ({Key, Value}, [{Key, AccValues} | Tail]) -> [{Key, [Value | AccValues]} | Tail];
            ({Key, Value}, Acc) -> [{Key, [Value]} | Acc]
        end, [], lists:keysort(1, List))).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calculates average of listed numbers
%% @end
%%--------------------------------------------------------------------
-spec average(List :: list()) -> float().
average(List) ->
    lists:foldl(fun(N, Acc) -> N + Acc end, 0, List) / length(List).

%%%===================================================================
%%% Internal functions
%%%===================================================================