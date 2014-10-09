%%%-------------------------------------------------------------------
%%% @author RoXeon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Oct 2014 15:31
%%%-------------------------------------------------------------------
-module(dbsync_lib).
-author("RoXeon").

%% API
-export([]).
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

-define(DBSYNC_REQUEST_TIMEOUT, 5000).

%% apply/4
%% ====================================================================
%% @doc Same as apply/5 but with default Timeout
%% @end
-spec apply(Module :: module(), Method :: atom() | {synch, atom()} | {asynch, atom()},
    Args :: [term()], ProtocolVersion :: number()) -> any() | {error, worker_not_found}.
%% ====================================================================
apply(Module, Method, Args, ProtocolVersion) ->
    apply(Module, Method, Args, ProtocolVersion, ?DBSYNC_REQUEST_TIMEOUT).

%% apply/5
%% ====================================================================
%% @doc Behaves similar to erlang:apply/3 but works only with DAO worker<br/>.
%% Method calls are made through random gen_server. <br/>
%% Method should be tuple {synch, Method} or {asynch, Method}<br/>
%% but if its simple atom(), {synch, Method} is assumed<br/>
%% Timeout argument defines how long should this method wait for response
%% @end
-spec apply(Module :: module(), Method :: atom() | {synch, atom()} | {asynch, atom()},
    Args :: [term()], ProtocolVersion :: number(), Timeout :: pos_integer()) -> any() | {error, worker_not_found} | {error, timeout}.
%% ====================================================================
apply(Module, {asynch, Method}, Args, ProtocolVersion, _Timeout) ->
    try gen_server:call(?Dispatcher_Name, {dao_worker, ProtocolVersion, {Module, Method, Args}}) of
        ok ->
            ok;
        worker_not_found ->
            {error, worker_not_found}
    catch
        Type:Error ->
            ?error("Cannot make a call to request_dispatcher on node ~p Reason: ~p", [node(), {Type, Error}]),
            {error, {Type, Error}}
    end;
apply(Module, {synch, Method}, Args, ProtocolVersion, Timeout) ->
    case get(msgID) of
        ID when is_integer(ID) ->
            put(msgID, ID + 1);
        _ -> put(msgID, 0)
    end,
    MsgID = get(msgID),
    try gen_server:call(?Dispatcher_Name, {dao_worker, ProtocolVersion, self(), MsgID, {Module, Method, Args}}) of
        ok ->
            receive
                {worker_answer, MsgID, Resp} -> Resp
            after Timeout ->
                ?warning("Cannot receive answer - timeout"),
                {error, timeout}
            end;
        worker_not_found ->
            {error, worker_not_found}
    catch
        Type:Error ->
            ?error("Cannot make a call to request_dispatcher on node ~p Reason: ~p", [node(), {Type, Error}]),
            {error, {Type, Error}}
    end;
apply(Module, Method, Args, ProtocolVersion, Timeout) ->
    apply(Module, {synch, Method}, Args, ProtocolVersion, Timeout).