%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Temporary state module for DBSync
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_temp).
-author("Rafal Slota").

-export([put_value/2, put_value/3, update_value/2, get_value/1, clear_value/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Puts given Value in datastore worker's temporary state
%% @end
%%--------------------------------------------------------------------
-spec put_value(Key :: term(), Value :: term()) -> ok.
put_value(Key, Value) ->
    put_value(Key, Value, 0).


%%--------------------------------------------------------------------
%% @doc
%% Puts given Value in datastore worker's temporary state
%% @end
%%--------------------------------------------------------------------
-spec put_value(Key :: term(), Value :: term(), Timeout :: non_neg_integer()) -> ok.
put_value(Key, Value, 0) ->
    worker_host:state_put(dbsync_worker, Key, Value);
put_value(Key, Value, Timeout) ->
    timer:send_after(Timeout, whereis(dbsync_worker), {timer, {clear_temp, Key}}),
    worker_host:state_put(dbsync_worker, Key, Value).


%%--------------------------------------------------------------------
%% @doc
%% Clears given Key in datastore worker's temporary state
%% @end
%%--------------------------------------------------------------------
-spec clear_value(Key :: term()) -> ok.
clear_value(Key) ->
    worker_host:state_put(dbsync_worker, Key, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Gets Value from datastore worker's temporary state
%% @end
%%--------------------------------------------------------------------
-spec get_value(Key :: term()) -> Value :: term().
get_value(Key) ->
    worker_host:state_get(dbsync_worker, Key).


%%--------------------------------------------------------------------
%% @doc
%% Updates Value from datastore worker's temporary state
%% @end
%%--------------------------------------------------------------------
-spec update_value(Key :: term(), UpdateFun :: fun((OldValue :: term()) -> NewValue :: term())) -> ok.
update_value(Key, UpdateFun) ->
    DBSyncWorker = whereis(dbsync_worker),
    case self() of
        DBSyncWorker ->
            NewValue = UpdateFun(get_value(Key)),
            put_value(Key, NewValue);
        _ ->
            worker_host:state_update(dbsync_worker, Key, UpdateFun)
    end.

