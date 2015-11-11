%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc datastore worker's implementation
%%% @end
%%%--------------------------------------------------------------------
-module(datastore_worker).
-author("Rafal Slota").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("cluster/worker/modules/datastore/datastore_engine.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).
-export([state_get/1, state_put/2]).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->

    %% Get Riak nodes
    DBNodes =
        case application:get_env(?APP_NAME, db_nodes) of
            {ok, Nodes} ->
                lists:map(
                    fun(NodeString) ->
                        [HostName, Port] = string:tokens(atom_to_list(NodeString), ":"),
                        {list_to_binary(HostName), list_to_integer(Port)}
                    end, Nodes);
            _ ->
                []
        end,

    State = lists:foldl(
        fun(Model, StateAcc) ->
            #model_config{name = RecordName} = ModelConfig = Model:model_init(),
            maps:put(RecordName, ModelConfig, StateAcc)
        end, #{}, datastore_config:models()),

    {ok, State#{db_nodes => DBNodes}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck |
    {driver_call, Module :: atom(), Method :: atom(), Args :: [term()]},
    Result :: nagios_handler:healthcheck_response() | ok | pong | {ok, Response} |
    {error, Reason},
    Response :: term(),
    Reason :: term().
handle(ping) ->
    pong;

handle(healthcheck) ->
    State = worker_host:state_to_map(?MODULE),
    PersistenceModule = datastore:driver_to_module(?PERSISTENCE_DRIVER),
    LocalCacheModule = datastore:driver_to_module(?LOCAL_CACHE_DRIVER),
    GlobalCacheModule = datastore:driver_to_module(?DISTRIBUTED_CACHE_DRIVER),
    HC = #{
        datastore_state_init => datastore:healthcheck(),
        ?PERSISTENCE_DRIVER => catch PersistenceModule:healthcheck(State),
        ?LOCAL_CACHE_DRIVER => catch LocalCacheModule:healthcheck(State),
        ?DISTRIBUTED_CACHE_DRIVER => catch GlobalCacheModule:healthcheck(State)
    },

    maps:fold(
        fun
            (_, ok, AccIn) ->
                AccIn;
            (K, {error, Reason}, _AccIn) when is_atom(K) ->
                ?error("Driver ~p healthcheck error: ~p", [K, Reason]),
                {error, K};
            (K, Reason, _AccIn) ->
                ?error("Unknown status of driver ~p, healthcheck error: ~p", [K, Reason]),
                {error, unknown_driver_status}
        end, ok, HC);

%% Proxy call to given datastore driver
handle({driver_call, Module, Method, Args}) ->
    try erlang:apply(Module, Method, Args) of
        ok -> ok;
        {ok, Response} -> {ok, Response};
        {error, Reason} -> {error, Reason}
    catch
        _:Reason ->
            ?error_stacktrace("datastore request failed due to ~p", [Reason]),
            {error, Reason}
    end;

%% Unknown request
handle(_Request) ->
    ?log_bad_request(_Request).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok.
cleanup() ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Puts given Value in datastore worker's state
%% @end
%%--------------------------------------------------------------------
-spec state_put(Key :: term(), Value :: term()) -> ok.
state_put(Key, Value) ->
    worker_host:state_put(?MODULE, Key, Value).

%%--------------------------------------------------------------------
%% @doc
%% Puts Value from datastore worker's state
%% @end
%%--------------------------------------------------------------------
-spec state_get(Key :: term()) -> Value :: term().
state_get(Key) ->
    worker_host:state_get(?MODULE, Key).
