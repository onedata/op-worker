%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module manages hosts and connections to VeilFS DB
%% @end
%% ===================================================================
-module(dao_hosts).

-include_lib("veil_modules/dao/common.hrl").
-include_lib("veil_modules/dao/dao_hosts.hrl").

-ifdef(TEST).
-compile([export_all]).
-endif.

%% API
-export([insert/1, delete/1, ban/1, ban/2, reactivate/1, call/2, call/3, store_exec/2]).

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
    lager:warning([{mod, ?MODULE}], "Host: ~p is being banned. Reason of that is probably it wasn't answering or it was answering to slow. It'll be reactivated after: ~ps", [Host, BanTime/1000]),
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
-spec call(Module :: atom(), Method :: atom(), Args :: [Arg :: term()]) -> term() | Error when
    Error :: {error, rpc_retry_limit_exceeded} | {error, term()}.
%% ====================================================================
call(Module, Method, Args) ->
    case get_host() of
        H when is_atom(H) ->
            call(Module, Method, Args, 0);
        {error, Err} -> {error, Err}
    end.

%% store_exec/2
%% ====================================================================
%% @doc Executes Msg. Caller must ensure that this method won't be used concurrently.
%% Currently this method is used as part of internal module implementation, although it has to be exported
%% because it's called by gen_server (which ensures it's sequential call).
%% @end
-spec store_exec(sequential, Msg :: term()) -> ok | {error, Error :: term()}.
%% ====================================================================
store_exec(sequential, {insert_host, Host}) ->
    ets:insert(db_host_store, {host, Host}),
    ok;
store_exec(sequential, {delete_host, Host}) ->
    ets:delete_object(db_host_store, {host, Host}),
    ets:delete_object(db_host_store, {banned_host, Host}),
    ok;
store_exec(sequential, {ban_host, Host}) ->
    case registered(Host) of
        false ->
            {error, no_host};
        true ->
            ets:delete_object(db_host_store, {host, Host}),
            ets:insert(db_host_store, {banned_host, Host}),
            ok
    end;
store_exec(sequential, {reactivate_host, Host}) ->
    case registered(Host) of
        false ->
            {error, no_host};
        true ->
            ets:delete_object(db_host_store, {banned_host, Host}),
            ets:insert(db_host_store, {host, Host}),
            ok
    end;
store_exec(sequential, _Unknown) ->
    lager:error([{mod, ?MODULE}], "Unknown host store command: ~p", [_Unknown]),
    {error, unknown_command}.


%% ===================================================================
%% Internal functions
%% ===================================================================


%% registered/1
%% ====================================================================
%% @doc Checks if Host was inserted to store
-spec registered(Host :: atom()) -> true | false.
%% ====================================================================
registered(Host) ->
    case ets:match_object(db_host_store, {'_', Host}) of
        [] -> false;
        _ -> true
    end.


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
            Host = get_random(),
            if is_atom(Host) -> put(db_host, Host); true -> ok end,
            Host;
        R -> R
    end.


%% store_exec/1
%% ====================================================================
%% @doc Sequentially exec Msg
-spec store_exec(Msg :: term()) -> ok | {error, Err} | {error, timeout} when
    Err :: term().
%% ====================================================================
store_exec(Msg) ->
    PPid = self(),
    Pid = spawn(fun() -> receive Resp -> PPid ! {self(), Resp} after 1000 -> exited end end),
    gen_server:cast(dao, {sequential_synch, get(protocol_version), {hosts, store_exec, [sequential, Msg]}, non, {proc, Pid}}),
    receive
        {Pid, Response} -> Response
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
    case ping(Host, ?NODE_DOWN_TIMEOUT) of
        pang -> ban(Host), call(Module, Method, Args, Attempt + 1);
        pong ->
            case rpc:call(Host, Module, Method, Args) of
                {badrpc, nodedown} ->
                    ban(Host),
                    call(Module, Method, Args, Attempt + 1);
                {badrpc, Error} ->
                    {error, Error};
                Other ->
                    reactivate(Host),
                    Other
            end
    end;
call(_Module, _Method, _Args, _Attempt) ->
    lager:error([{mod, ?MODULE}], "DBMS call filed after: ~p attempts. Current host store state: ~p", [_Attempt, ets:lookup(db_host_store, host) ++ ets:lookup(db_host_store, banned_host)]),
    {error, rpc_retry_limit_exceeded}.


%% ping/2
%% ====================================================================
%% @doc Same as net_adm:ping/1, but with Timeout
-spec ping(Host :: atom(), Timeout :: non_neg_integer()) -> pong | pang.
%% ====================================================================
ping(Host, Timeout) ->
    Me = self(),
    Pid = spawn(fun() -> Me ! {self(), {ping_res, net_adm:ping(Host)}} end),
    receive
        {Pid, {ping_res, Res}} -> Res
    after Timeout ->
        pang
    end.
