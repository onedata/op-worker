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

-include_lib("veil_modules/dao/common.hrl").

-ifdef(TEST).
-compile([export_all]).
-endif.

%% Config
-define(DAO_DB_HOSTS_REFRESH_INTERVAL, 60*60*1000). % 1h


%% API
-export([start_link/0, start_link/1, insert/1, call/2, call/3]).

%% supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

%% start_link/0
%% ====================================================================
%% @doc Starts application supervisor for host store master process
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
%% @doc Starts the hosts store master process (called by supervisor)
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

%% insert/1
%% ====================================================================
%% @doc Inserts db host name into store (host pool)
-spec insert(Host :: atom()) -> ok | {error, timeout}.
%% ====================================================================
insert(Host) ->
    Pid = whereis(db_host_store_proc),
    db_host_store_proc ! {self(), insert_host, Host},
    receive
        {Pid, ok} -> ok
    after 100 ->
        {error, timeout}
    end.

%% call/2
%% ====================================================================
%% @doc Calls fabric:Method with Args on random db host. Random host will be assigned to the calling process
%% and will be used with subsequent calls
%% @end
-spec call(Method :: atom(), Args :: [term()]) -> term().
%% ====================================================================
call(Method, Args) ->
    call(fabric, Method, Args).

%% call/2
%% ====================================================================
%% @doc Same as call/2, but with custom Module
-spec call(Module :: atom(), Method :: atom(), Args :: [Arg :: term()]) -> term().
%% ====================================================================
call(Module, Method, Args) ->
    rpc:call(get_host(), Module, Method, Args).

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
        {From, insert_host, Host} ->
            ets:insert(db_host_store, {host, Host}),
            From ! {self(), ok};
        {_From, shutdown} ->
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
    ets:insert(db_host_store, {host, 'bc@RoXeon-Laptop.lan'}),
    erlang:now();
update_hosts(State, _TimeDiff) ->
    State.

%% get_random/0
%% ====================================================================
%% @doc Returns first random and alive DB node from host store
-spec get_random() -> DbNode when
    DbNode :: atom() | {error, no_db_host_found}.
%% ====================================================================
get_random() ->
    ?SEED,
    get_random([X || {host, X} <- ets:lookup(db_host_store, host)]).

%% get_random/1
%% ====================================================================
%% @doc Returns first random and alive DB node from Hosts list. Should not be called directly, use get_random/1 instead
-spec get_random([Host :: atom()]) -> DbNode when
    DbNode :: atom() | {error, no_db_host_found}.
%% ====================================================================
get_random([]) ->
    {error, no_db_host_found};
get_random(Hosts) when is_list(Hosts) ->
    Host = lists:nth(?RND(length(Hosts)), Hosts),
    check_host(Host, Hosts, net_adm:ping(Host)).

%% check_host/3
%% ====================================================================
%% @doc Finds fist alive DB node from Hosts list. It's support method therefore should not be called directly
-spec check_host(Host :: atom(), Hosts :: [atom()], PingResult) -> DbNode when
    DbNode :: atom() | {error, no_db_host_found},
    PingResult :: ping | pong.
%% ====================================================================
check_host(Host, _Hosts, pong) ->
    Host;
check_host(Host, Hosts, pang) ->
    get_random(lists:filter(fun(X) -> X =/= Host end, Hosts)).


%% get_host/0
%% ====================================================================
%% @doc Returns DB node assigned to current process or randomize one if none (i.e this is first get_host/0 call)
-spec get_host() -> DbNode when
    DbNode :: atom() | {error, no_db_host_found}.
%% ====================================================================
get_host() ->
    case get(db_host) of
        undefined ->
            put(db_host, get_random());
            _ ->    ok
    end,
    get(db_host).
