%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour callbacks
%% DAO API functions are implemented in DAO sub-modules like: dao_cluster, dao_vfs
%% All DAO API functions should not be called directly. Call dao:handle(_, {Module, MethodName, ListOfArgs) instead, when
%% Module :: atom() is module suffix (prefix is 'dao_'), MethodName :: atom() is the method name
%% and ListOfArgs :: [term()] is list of argument for the method.
%% See handle/2 for more details.
%% @end
%% ===================================================================
-module(dao).
-behaviour(worker_plugin_behaviour).

-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/couch_db.hrl").

-import(dao_helper, [name/1]).

-ifdef(TEST).
-compile([export_all]).
-endif.

%% worker_plugin_behaviour callbacks
-export([init/1, handle/2, cleanUp/0]).

%% ===================================================================
%% Behaviour callback functions
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc worker_plugin_behaviour callback init/1
-spec init(Args :: term()) -> Result when
    Result :: ok | {error, Error},
    Error :: term().
%% ====================================================================
init(_Args) ->
    case dao_hosts:start_link() of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok;
        ignore -> {error, supervisor_ignore};
        {error, _Err} = Ret -> Ret
    end.

%% handle/1
%% ====================================================================
%% @doc worker_plugin_behaviour callback handle/1
%% All {Module, Method, Args} requests (second argument), executes Method with Args in 'dao_Module' module.
%% E.g calling dao:handle(_, {vfs, some_method, [some_arg]}) will call dao_vfs:some_method(some_arg)
%% You can omit Module atom in order to use default module which is dao_cluster.
%% E.g calling dao:handle(_, {some_method, [some_arg]}) will call dao_cluster:some_method(some_arg)
%% Additionally all exceptions from called API method will be caught and converted into {error, Exception} tuple
%% E.g. calling handle(_, {save_record, [Id, Rec]}) will execute dao_cluster:save_record(Id, Rec) and normalize return value
%% @end
-spec handle(ProtocolVersion :: term(), Request) -> Result when
    Request :: {Method, Args} | {Mod :: tuple(), Method, Args},
    Method :: atom(),
    Args :: list(),
    Result :: ok | {ok, Response} | {error, Error},
    Response :: term(),
    Error :: term().
%% ====================================================================
handle(_ProtocolVersion, {Target, Method, Args}) when is_atom(Target), is_atom(Method), is_list(Args) ->
    Module = list_to_atom("dao_" ++ atom_to_list(Target)),
    try apply(Module, Method, Args) of
        {error, Err} -> {error, Err};
        {ok, Response} -> {ok, Response};
        ok -> ok;
        Other -> {error, Other}
    catch
        error:{badmatch, {error, Err}} -> {error, Err};
        _:Error -> {error, Error}
    end;
handle(ProtocolVersion, {Method, Args}) when is_atom(Method), is_list(Args) ->
    handle(ProtocolVersion, {cluster, Method, Args});
handle(_ProtocolVersion, _Request) ->
    {error, wrong_args}.

%% cleanUp/0
%% ====================================================================
%% @doc worker_plugin_behaviour callback cleanUp/0
-spec cleanUp() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
%% ====================================================================
cleanUp() ->
    Pid = whereis(db_host_store_proc),
    monitor(process, Pid),
    Pid ! {self(), shutdown},
    receive {'DOWN', _Ref, process, Pid, normal} -> ok after 1000 -> {error, timeout} end.

%% ===================================================================
%% API functions
%% ===================================================================


%% ===================================================================
%% Internal functions
%% ===================================================================


