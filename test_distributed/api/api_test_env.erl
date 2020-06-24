%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions used for environment manipulation in API tests.
%%% @end
%%%-------------------------------------------------------------------
-module(api_test_env).
-author("Bartosz Walkowicz").


-export([init/0, set/3, get/2, get/3]).

-opaque env_ref() :: integer().

-export_type([env_ref/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec init() -> env_ref().
init() ->
    erlang:unique_integer([positive]).


-spec set(env_ref(), Key :: term(), Value :: term()) -> ok.
set(EnvRef, Key, Value) ->
    simple_cache:put({EnvRef, Key}, Value).


-spec get(env_ref(), Key :: term()) -> Value :: term() | no_return().
get(EnvRef, Key) ->
    case simple_cache:get({EnvRef, Key}) of
        {ok, Value} -> Value;
        {error, _} = Error -> throw(Error)
    end.


-spec get(env_ref(), Key :: term(), Default :: term()) ->
    Value :: term() | no_return().
get(EnvRef, Key, Default) ->
    case simple_cache:get({EnvRef, Key}) of
        {ok, Value} -> Value;
        {error, not_found} -> Default;
        {error, _} = Error -> throw(Error)
    end.
