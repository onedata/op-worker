%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: @todo: write me!
%% @end
%% ===================================================================
-module(dbsync_state).
-author("Rafal Slota").


-include("oneprovider_modules/dao/dao.hrl").
-include_lib("ctool/include/logging.hrl").

-define(dbsync_state, dbsync_state).

%% API
-export([get/1, set/2, call/1, state_loop/1, state_loop/0]).


set(Key, Value) ->
    ets:insert(?dbsync_state, {Key, Value}).

get(Key) ->
    case ets:lookup(?dbsync_state, Key) of
        [{_, Value}] -> Value;
        _ -> undefined
    end.


state_loop() ->
    state_loop(#dbsync_state{}).
state_loop(State) ->
    NewState =
        receive
            {From, Fun} when is_function(Fun) ->
                {Response, NState} =
                    try Fun(State) of
                        {Response1, #dbsync_state{} = NState1} ->
                            {Response1, NState1};
                        OnlyResp -> {OnlyResp, State}
                    catch
                        Type:Error -> {{error, {Type, Error}}, State}
                    end,
                From ! {self(), Response},
                NState;
            {From, Module, Method, Args} ->
                {Response, NState} =
                    try apply(Module, Method, Args ++ [State]) of
                        {Response1, #dbsync_state{} = NState1} ->
                            {Response1, NState1};
                        OnlyResp -> {OnlyResp, State}
                    catch
                        Type:Error ->
                            Stack = erlang:get_stacktrace(),
                            ?dump_all([Type, Error, Stack]),
                            {{error, {Type, Error, Stack}}, State}
                    end,
                From ! {self(), Response},
                NState
        after 10000 ->
            State
        end,
    ?MODULE:state_loop(NewState).

call(Fun) when is_function(Fun) ->
    ?dbsync_state ! {self(), Fun},
    sync_call_get_response();
call(Method) when is_atom(Method) ->
    call(Method, []).
call(Method, Args) ->
    call(dbsync_state, Method, Args).
call(Module, Method, Args) when is_atom(Module), is_atom(Method), is_list(Args) ->
    ?dbsync_state ! {self(), Module, Method, Args},
    sync_call_get_response().

%% Internal sync_call use only !
sync_call_get_response() ->
    StPid = whereis(?dbsync_state),
    receive
        {StPid, Response} -> Response
    after 10000 ->
        {error, sync_timeout}
    end.