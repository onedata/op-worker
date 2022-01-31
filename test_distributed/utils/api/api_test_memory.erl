%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions used for cache/memory manipulation in API tests.
%%% @end
%%%-------------------------------------------------------------------
-module(api_test_memory).
-author("Bartosz Walkowicz").


-export([init/0, set/3, get/2, get/3]).

-opaque mem_ref() :: integer().

-export_type([mem_ref/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec init() -> mem_ref().
init() ->
    erlang:unique_integer([positive]).


-spec set(mem_ref(), Key :: term(), Value :: term()) -> ok.
set(MemRef, Key, Value) ->
    node_cache:put({MemRef, Key}, Value).


-spec get(mem_ref(), Key :: term()) -> Value :: term() | no_return().
get(MemRef, Key) ->     
    node_cache:get({MemRef, Key}).


-spec get(mem_ref(), Key :: term(), Default :: term()) ->
    Value :: term() | no_return().
get(MemRef, Key, Default) ->
    node_cache:get({MemRef, Key}, Default).
