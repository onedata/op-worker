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
-define(RPC_MAX_RETRIES, 5).
-define(DEFAULT_BAN_TIME, 5*60*1000). % 5min


%% API
-export([start_link/0, start_link/1, insert/1, delete/1, ban/1, ban/2, reactivate/1, call/2, call/3]).

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
    store_exec({insert_host, Host}).

%% delete/1
%% ====================================================================
%% @doc Deletes db host name from store (host pool)
-spec delete(Host :: atom()) -> ok | {error, timeout}.
%% ====================================================================
delete(Host) ->
    put(db_host, undefined),
    store_exec({delete_host, Host}).

%% ban/1
%% ====================================================================
%% @doc Bans db host name (lowers its priority while selecting host in get_host/0)
%% Ban will be cleaned after DEFAULT_BAN_TIME ms or while store refresh
%% @end
-spec ban(Host :: atom()) -> ok | {error, timeout}.
%% ====================================================================
ban(Host) ->
    ban(Host, ?DEFAULT_BAN_TIME).

%% ban/2
%% ====================================================================
%% @doc Bans db host name (lowers its priority while selecting host in get_host/0)
%% Ban will be cleaned after BanTime ms or while store refresh
%% If given Host is already banned, nothing happens and will return 'ok'
%% If given Host wasn't inserted, returns {error, no_host}
%% @end
-spec ban(Host :: atom(), BanTime :: integer()) -> ok | {error, no_host} | {error, timeout}.
%% ====================================================================
ban(Host, BanTime) ->
    Res = store_exec({ban_host, Host}),
    put(db_host, undefined),
    {ok, _} = timer:apply_after(BanTime, ?MODULE, reactivate, [Host]),
    Res.

%% reactivate/2
%% ====================================================================
%% @doc Reactivate banned db host name
%% If given Host wasn't banned, nothing happens and will return 'ok'
%% If given Host wasn't inserted, returns {error, no_host}
%% @end
-spec reactivate(Host :: atom()) -> ok | {error, no_host} | {error, timeout}.
%% ====================================================================
reactivate(Host) ->
    store_exec({reactivate_host, Host}).

%% call/2
%% ====================================================================
%% @doc Calls fabric:Method with Args on random db host. Random host will be assigned to the calling process
%% and will be used with subsequent calls
%% @end
-spec call(Method :: atom(), Args :: [term()]) -> term() | {error, rpc_retry_limit_exceeded}.
%% ====================================================================
call(Method, Args) ->
    call(fabric, Method, Args).

%% call/3
%% ====================================================================
%% @doc Same as call/2, but with custom Module
-spec call(Module :: atom(), Method :: atom(), Args :: [Arg :: term()]) -> term() | {error, rpc_retry_limit_exceeded}.
%% ====================================================================
call(Module, Method, Args) ->
    call(Module, Method, Args, 0).

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
        {From, {insert_host, Host}} ->
            ets:insert(db_host_store, {host, Host}),
            From ! {self(), ok};
        {From, {delete_host, Host}} ->
            ets:delete_object(db_host_store, {host, Host}),
            ets:delete_object(db_host_store, {banned_host, Host}),
            From ! {self(), ok};
        {From, {ban_host, Host}} ->
            case registered(Host) of
                false ->
                    From ! {self(), {error, no_host}};
                true ->
                    ets:delete_object(db_host_store, {host, Host}),
                    ets:insert(db_host_store, {banned_host, Host}),
                    From ! {self(), ok}
            end;
        {From, {reactivate_host, Host}} ->
            case registered(Host) of
                false ->
                    From ! {self(), {error, no_host}};
                true ->
                    ets:delete_object(db_host_store, {banned_host, Host}),
                    ets:insert(db_host_store, {host, Host}),
                    From ! {self(), ok}
            end;
        {_From, shutdown} ->
            exit(normal)
    after ?DAO_DB_HOSTS_REFRESH_INTERVAL ->
        ok
    end,
    store_loop(update_hosts(State, timer:now_diff(State, erlang:now()))).


% registered/1
%% ====================================================================
%% @doc Checks if Host was inserted to store
-spec registered(Host :: atom()) -> true | false.
%% ====================================================================
registered(Host) ->
    case ets:match_object(db_host_store, {'_', Host}) of
        [] -> false;
        _ -> true
    end.


%% update_hosts/1
%% ====================================================================
%% @doc Update host list when it's old
-spec update_hosts(State, TimeDiff) -> NewState when
    State :: erlang:timestamp(),
    TimeDiff :: integer(),
    NewState :: erlang:timestamp().
%% ====================================================================
update_hosts(_State, TimeDiff) when TimeDiff > ?DAO_DB_HOSTS_REFRESH_INTERVAL ->
    ets:delete_all_objects(db_host_store),
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
    get_random([X || {host, X} <- ets:lookup(db_host_store, host)], [X || {banned_host, X} <- ets:lookup(db_host_store, banned_host)]).


%% get_random/1
%% ====================================================================
%% @doc Returns first random and alive DB node from Hosts list. Should not be called directly, use get_random/1 instead
-spec get_random([Host :: atom()], [BannedHost :: atom()]) -> DbNode when
    DbNode :: atom() | {error, no_db_host_found}.
%% ====================================================================
get_random([], []) ->
    {error, no_db_host_found};
get_random([], Banned) ->
    lists:nth(?RND(length(Banned)), Banned);
get_random(Hosts, _Banned) when is_list(Hosts) ->
    lists:nth(?RND(length(Hosts)), Hosts).


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


%% store_exec/1
%% ====================================================================
%% @doc Sends Msg to host store master process and waits for replay
-spec store_exec(Msg :: term()) -> ok | {error, Err} | {error, timeout} when
    Err :: term().
%% ====================================================================
store_exec(Msg) ->
    Pid = whereis(db_host_store_proc),
    db_host_store_proc ! {self(), Msg},
    receive
        {Pid, ok} -> ok;
        {Pid, {error, Err}} -> {error, Err}
    after 100 ->
        {error, timeout}
    end.


%% call/4
%% ====================================================================
%% @doc Internal version of call/3. Do not call directly
-spec call(Module :: atom(), Method :: atom(), Args :: [Arg :: term()], Attempt :: integer()) -> term() | {error, rpc_retry_limit_exceeded}.
%% ====================================================================
call(Module, Method, Args, Attempt) when Attempt < ?RPC_MAX_RETRIES ->
    Host = get_host(),
    case rpc:call(Host, Module, Method, Args) of
        {badrpc, nodedown} ->
            ban(Host),
            call(Module, Method, Args, Attempt+1);
        {badrpc, Error} ->
            {error, Error};
        Other ->
            Other
    end;
call(_Module, _Method, _Args, _Attempt) ->
    {error, rpc_retry_limit_exceeded}.