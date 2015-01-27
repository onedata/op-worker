%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%--------------------------------------------------------------------
-module(datastore_worker).
-author("Rafal Slota").

-behaviour(worker_plugin_behaviour).

-include("registered_names.hrl").
-include("workers/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

-define(PERSISTENCE_DRIVER, riak_datastore_driver).
-define(LOCAL_CACHE_DRIVER, local_cache_driver).
-define(DISTRIBUTED_CACHE_DRIVER, distributed_cache_driver).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/2, cleanup/0]).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
      Result :: ok | {error, Error},
      Error :: term().
init(_Args) ->
    ets:new(datastore_state, [named_table, public, set]),
    Buckets = lists:foldl(
      fun(Model, Acc) ->
          #model_config{name = RecordName, bucket = Bucket} = ModelConfig = Model:model_init(),
          state_put(RecordName, ModelConfig),
          maps:put(Bucket, [ModelConfig | maps:get(Bucket, Acc, [])], Acc)
      end, #{}, ?MODELS),

    lists:foreach(
        fun(Bucket) ->
            ?info("Initializing bucket ~p", [Bucket]),
            ok = ?PERSISTENCE_DRIVER:init_bucket(Bucket),
            ok = ?LOCAL_CACHE_DRIVER:init_bucket(Bucket),
            ok = ?DISTRIBUTED_CACHE_DRIVER:init_bucket(Bucket)
        end, maps:to_list(Buckets)),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1. <br/>
%% @end
%%--------------------------------------------------------------------
-spec handle(ProtocolVersion :: term(), Request) -> Result when
      Request :: ping | healthcheck | get_version,
      Result :: ok | {ok, Response} | {error, Error} | pong | Version,
      Response :: term(),
      Version :: term(),
      Error :: term().
handle(_ProtocolVersion, ping) ->
    pong;

handle(_ProtocolVersion, healthcheck) ->
    ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

handle(_ProtocolVersion, _Msg) ->
    ?warning("datastore worker unknown message: ~p", [_Msg]).


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
      Result :: ok | {error, Error},
      Error :: timeout | term().
%% ====================================================================
cleanup() ->
    ok.


state_put(Key, Value) ->
    ets:insert(datastore_state, {Key, Value}).

state_get(Key) ->
    case ets:lookup(datastore_state, Key) of
        [{Key, Value}] -> Value;
        _ -> undefinded
    end.
