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
-spec init(Args :: term()) ->
    {ok, #{term() => term()}} | {error, Reason :: term()}.
init(_Args) ->

    %% Get Riak nodes
    RiakNodes =
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
        end, #{}, ?MODELS),

    {ok, State#{riak_nodes => RiakNodes}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request, State :: term()) -> Result when
    Request :: ping | healthcheck |
    {driver_call, Module :: atom(), Method :: atom(), Args :: [term()]},
    Result :: nagios_handler:healthcheck_reponse() | ok | pong | {ok, Response} |
    {error, Reason},
    Response :: term(),
    Reason :: term().
handle(ping, _State) ->
    pong;

handle(healthcheck, _State) ->
    ok;
%% TODO @Rafal
%% healthcheck rzuca blad (bo Ryjaka nie ma), natomiast powinien zwracac healthcheck_reponse()
%%
%%     HC = #{
%%         ?PERSISTENCE_DRIVER => ?PERSISTENCE_DRIVER:healthcheck(State),
%%         ?LOCAL_CACHE_DRIVER => ?LOCAL_CACHE_DRIVER:healthcheck(State),
%%         ?DISTRIBUTED_CACHE_DRIVER => ?DISTRIBUTED_CACHE_DRIVER:healthcheck(State)
%%     },
%%
%%     maps:fold(
%%         fun
%%             (_, ok, AccIn) ->
%%                 AccIn;
%%             (K, {error, Reason}, _AccIn) ->
%%                 ?error("Driver ~p healthckeck error: ~p", [K, Reason]),
%%                 {error, {driver_failure, {K, Reason}}}
%%         end, ok, HC);

%% Proxy call to given datastore driver
handle({driver_call, Module, Method, Args}, _State) ->
    case erlang:apply(Module, Method, Args) of
        ok -> ok;
        {ok, Response} -> {ok, Response};
        {error, Reason} -> {error, Reason}
    end;

%% Unknown request
handle(_Request, _) ->
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
    gen_server:call(?MODULE, {updatePlugInState,
        fun(State) ->
            maps:put(Key, Value, State)
        end}).

%%--------------------------------------------------------------------
%% @doc
%% Puts Value from datastore worker's state
%% @end
%%--------------------------------------------------------------------
-spec state_get(Key :: term()) -> Value :: term().
state_get(Key) ->
    maps:get(Key, gen_server:call(?MODULE, get_plugin_state)).
