%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module manages VeilFS DBMS hosts
%% @end
%% ===================================================================
-module(dao_hosts).

-behaviour(supervisor).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, [[]]}, transient, 5000, Type, [I]}).
-define(DAO_DB_HOSTS_REFRESH_INTERVAL, 60*60*1000). % 1h


%% API
-export([start_link/0, start_link/1]).

%% supervisor callbacks
-export([init/1]).
%% ===================================================================
%% API functions
%% ===================================================================

%% start_link/0
%% ====================================================================
%% @doc Starts application supervisor
-spec start_link() -> Result when
    Result ::  {ok, pid()}
    | ignore
    | {error, Error},
    Error :: {already_started, pid()}
    | {shutdown, term()}
    | term().
%% ====================================================================
start_link() ->
    supervisor:start_link({local, dao_hosts_sup},?MODULE, []).

%% start_link/1
%% ====================================================================
%% @doc Starts the hosts store
-spec start_link(Args) -> Result when
    Args :: term(),
    Result ::  {ok,Pid}
    | ignore
    | {error,Error},
    Pid :: pid(),
    Error :: {already_started,Pid} | term().
%% ====================================================================
start_link(_Args) ->
    Pid = spawn_link(fun() -> init() end),
    register(db_host_store_proc, Pid),
    {ok, Pid}.


%% ===================================================================
%% Behaviour callback functions
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/supervisor.html#Module:init-1">supervisor:init/1</a>
-spec init(Args :: term()) -> Result when
    Result :: {ok, {SupervisionPolicy, [ChildSpec]}} | ignore,
    SupervisionPolicy :: {RestartStrategy, MaxR :: non_neg_integer(), MaxT :: pos_integer()},
    RestartStrategy :: one_for_all
    | one_for_one
    | rest_for_one
    | simple_one_for_one,
    ChildSpec :: {Id :: term(), StartFunc, RestartPolicy, Type :: worker | supervisor, Modules},
    StartFunc :: {M :: module(), F :: atom(), A :: [term()] | undefined},
    RestartPolicy :: permanent
    | transient
    | temporary,
    Modules :: [module()] | dynamic.
%% ====================================================================
init([]) ->
    {ok, {{one_for_one, 5, 10}, [?CHILD(?MODULE, worker)]}}.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% init/0
%% ====================================================================
%% @doc Initializes the hosts store
-spec init() -> Result when
    Result ::  ok | {error, Error},
    Error :: term().
%% ====================================================================
init() ->
    ets:new(db_host_store, [named_table, protected, bag]),
    store_loop({0, 0, 0}).


%% store_loop/1
%% ====================================================================
%% @doc Host store main loop
-spec store_loop(State) -> NewState when
    State :: erlang:timestamp(),
    NewState :: erlang:timestamp().
%% ====================================================================
store_loop(State) ->
    receive
        {From, force_update} ->
            From ! {self(), ok};
        {From, shutdown} ->
            exit(normal)
    after ?DAO_DB_HOSTS_REFRESH_INTERVAL ->
        ok
    end,
    store_loop(update_hosts(State, timer:now_diff(State, erlang:now()))).


%% update_hosts/1
%% ====================================================================
%% @doc Update host list when it's old
-spec update_hosts(State, TimeDiff) -> NewState when
    State :: erlang:timestamp(),
    TimeDiff :: integer(),
    NewState :: erlang:timestamp().
%% ====================================================================
update_hosts(_State, TimeDiff) when TimeDiff > ?DAO_DB_HOSTS_REFRESH_INTERVAL ->
    ets:insert(db_host_store, 'bc@RoXeon-Laptop.lan'),
    erlang:now();
update_hosts(State, _TimeDiff) ->
    State.
