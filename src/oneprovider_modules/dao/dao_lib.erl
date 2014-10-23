%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: DAO helper/utility functional methods. Those can be used in other modules
%% bypassing worker_host and gen_server.
%% @end
%% ===================================================================
-module(dao_lib).

-include("oneprovider_modules/dao/dao.hrl").
-include("registered_names.hrl").
-include_lib("dao/include/common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([wrap_record/1, strip_wrappers/1, apply/4, apply/5]).

%% ===================================================================
%% API functions
%% ===================================================================

%% wrap_record/1
%% ====================================================================
%% @doc Wraps given erlang record with #db_document{} wrapper. <br/>
%% See {@link dao_records:save_record/1} and {@link dao_records:get_record/1} for more details about #db_document{} wrapper.<br/>
%% @end
-spec wrap_record(Record :: tuple()) -> #db_document{}.
%% ====================================================================
wrap_record(Record) when is_tuple(Record) ->
    #db_document{record = Record}.


%% strip_wrappers/1
%% ====================================================================
%% @doc Strips #db_document{} wrapper. Argument can be either an #db_document{} or list of #db_document{} <br/>
%% Alternatively arguments can be passed as {ok, Arg} tuple. Its convenient because most DAO methods formats return value formatted that way<br/>
%% If the argument cannot be converted (e.g. error tuple is passed), this method returns it unchanged. <br/>
%% This method is designed for use as wrapper for "get_*"-like DAO methods. E.g. dao_lib:strip_wrappers(dao_vfs:get_file({absolute_path, "/foo/bar"}))
%% See {@link dao_records:save_record/1} and {@link dao_records:get_record/1} for more details about #db_document{} wrapper.<br/>
%% @end
-spec strip_wrappers(DbDocOrList :: #db_document{} | [#db_document{}]) -> tuple() | [tuple()].
%% ====================================================================
strip_wrappers({ok, List}) when is_list(List) ->
    {ok, strip_wrappers(List)};
strip_wrappers(List) when is_list(List) ->
    [X || #db_document{record = X} <- List];
strip_wrappers({ok, #db_document{} = Doc}) ->
    {ok, strip_wrappers(Doc)};
strip_wrappers(#db_document{record = Record}) when is_tuple(Record) ->
    Record;
strip_wrappers(Other) ->
    Other.


%% apply/4
%% ====================================================================
%% @doc Same as apply/5 but with default Timeout
%% @end
-spec apply(Module :: module(), Method :: atom() | {synch, atom()} | {asynch, atom()},
    Args :: [term()], ProtocolVersion :: number()) -> any() | {error, worker_not_found}.
%% ====================================================================
apply(Module, Method, Args, ProtocolVersion) ->
    apply(Module, Method, Args, ProtocolVersion, ?DAO_REQUEST_TIMEOUT).

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

%% ===================================================================
%% Internal functions
%% ===================================================================

