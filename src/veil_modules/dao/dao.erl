%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module gives high level API for VeilFS database
%% @end
%% ===================================================================
-module(dao).
-behaviour(worker_plugin_behaviour).

-ifdef(TEST).
-compile([export_all]).
-endif.

%% API
-export([]).

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
        {error, _Err}=Ret -> Ret
    end.

%% init/1
%% ====================================================================
%% @doc worker_plugin_behaviour callback init/1
-spec handle(ProtocolVersion :: term(), Request) -> Result when
    Request :: {Method, Args},
    Method :: atom(),
    Args :: list(),
    Result :: {ok, Response} | {error, Error},
    Response :: term(),
    Error :: term().
%% ====================================================================
handle(_ProtocolVersion, {Method, Args}) when is_atom(Method), is_list(Args) ->
    try apply(dao_helper, Method, Args) of
        Response -> {ok, Response}
    catch
        Type:Error -> {error, {Type, Error}}
    end;
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

