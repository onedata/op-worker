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
-define(LOCAL_CACHE_DRIVER, ets_cache_driver).
-define(DISTRIBUTED_CACHE_DRIVER, mnesia_cache_driver).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/2, cleanup/0]).
-export([state_get/1, state_put/2]).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, #{term() => term()}} | {error, Error},
    Error :: term().
init(_Args) ->
    RiakNodes =
        case application:get_env(?APP_NAME, riak_nodes) of
            {ok, Nodes} ->
                lists:map(
                    fun(NodeString) ->
                        [HostName, Port] = string:tokens(NodeString, ":"),
                        {list_to_binary(HostName), list_to_integer(Port)}
                    end, Nodes);
            _ ->
                []
        end,

    {State, Buckets} = lists:foldl(
      fun(Model, Acc) ->
          #model_config{name = RecordName, bucket = Bucket} = ModelConfig = Model:model_init(),
          {
              maps:put(RecordName, ModelConfig),
              maps:put(Bucket, [ModelConfig | maps:get(Bucket, Acc, [])], Acc)
          }
      end, {#{}, #{}}, ?MODELS),

    lists:foreach(
        fun(Bucket) ->
            ?info("Initializing bucket ~p", [Bucket]),
            ok = ?PERSISTENCE_DRIVER:init_bucket(Bucket),
            ok = ?LOCAL_CACHE_DRIVER:init_bucket(Bucket),
            ok = ?DISTRIBUTED_CACHE_DRIVER:init_bucket(Bucket)
        end, maps:to_list(Buckets)),
    {ok, State#{riak_nodes => RiakNodes}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1. <br/>
%% @end
%%--------------------------------------------------------------------
-spec handle(Request, State :: term()) -> Result when
    Request :: ping | healthcheck |
    {update_state, list(), list()} |
    {get_worker, atom()} |
    get_nodes,
    Result :: ok | {ok, Response} | {error, Error} | pong,
    Response :: [inet:ip4_address()],
    Error :: term().
handle(_ProtocolVersion, ping) ->
    pong;
handle(_ProtocolVersion, healthcheck) ->
    ok;
handle(_ProtocolVersion, {driver_call, Module, Method, Args}) ->
    erlang:apply(Module, Method, Args);
handle(_ProtocolVersion, _Msg) ->
    ?warning("datastore worker unknown message: ~p", [_Msg]).


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok.
cleanup() ->
    ok.


state_put(Key, Value) ->
    gen_server:call(?MODULE, {updatePlugInState,
        fun(State) ->
            maps:put(Key, Value, State)
        end}).

state_get(Key) ->
    maps:get(Key, gen_server:call(?MODULE, getPlugInState)).
