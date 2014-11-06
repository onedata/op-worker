%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module allows for management of RTransfer file blocks.
%% @end
%% ===================================================================
-module(rtheap).

%% API
-export([push/1, fetch/0, test/0]).

-on_load(init/0).

-record(rt_block, {file_id = "", offset = 0, size = 0, priority = 0}).

%% ====================================================================
%% API functions
%% ====================================================================

%% init/0
%% ====================================================================
%% @doc Initializes NIF library.
%% @end
-spec init() -> ok | no_return().
%% ====================================================================
init() ->
    ok = erlang:load_nif("c_lib/rtheap_drv", 0).


%% push/1
%% ====================================================================
%% @doc Pushes block on RTransfer heap.
%% @end
-spec push(#rt_block{}) -> ok | no_return().
%% ====================================================================
push(_Block) ->
    throw("NIF library not loaded.").


%% fetch/0
%% ====================================================================
%% @doc Fetches block from RTransfer heap.
%% @end
-spec fetch() -> #rt_block{}.
%% ====================================================================
fetch() ->
    throw("NIF library not loaded.").


test() ->
    PushAns = push(#rt_block{file_id = "file", offset = 1, size = 2, priority = 3}),
    io:format("push ans: ~p~n", [PushAns]),
    BlockAns = #rt_block{} = fetch(),
    io:format("fetch ans: ~p~n", [BlockAns]).