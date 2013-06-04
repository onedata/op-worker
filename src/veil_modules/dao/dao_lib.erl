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

-include_lib("veil_modules/dao/dao.hrl").

%% API
-export([wrap_record/1, strip_wrappers/1, apply/4, apply/5]).

%% ===================================================================
%% API functions
%% ===================================================================

%% wrap_record/1
%% ====================================================================
%% @doc Wraps given erlang record with #veil_document{} wrapper. <br/>
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% @end
-spec wrap_record(Record :: tuple()) -> #veil_document{}.
%% ====================================================================
wrap_record(Record) when is_tuple(Record) ->
    #veil_document{record = Record}.


%% strip_wrappers/1
%% ====================================================================
%% @doc Strips #veil_document{} wrapper. Argument can be either an #veil_document{} or list of #veil_document{} <br/>
%% Alternatively arguments can be passed as {ok, Arg} tuple. Its convenient because most DAO methods formats return value formatted that way<br/>
%% If the argument cannot be converted (e.g. error tuple is passed), this method returns it unchanged. <br/>
%% This method is designed for use as wrapper for "get_*"-like DAO methods. E.g. dao_lib:strip_wrappers(dao_vfs:get_file({absolute_path, "/foo/bar"}))
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% @end
-spec strip_wrappers(VeilDocOrList :: #veil_document{} | [#veil_document{}]) -> tuple() | [tuple()].
%% ====================================================================
strip_wrappers({ok, List}) when is_list(List) ->
    {ok, strip_wrappers(List)};
strip_wrappers(List) when is_list(List) ->
    [X || #veil_document{record = X} <- List];
strip_wrappers({ok, #veil_document{} = Doc}) ->
    {ok, strip_wrappers(Doc)};
strip_wrappers(#veil_document{record = Record}) when is_tuple(Record) ->
    Record;
strip_wrappers(Other) ->
    Other.

%% apply/4
%% ====================================================================
%% @doc Same as apply/5 but with default Timeout
%% @end
-spec apply(Module :: module(), Method :: atom() | {synch, atom()} | {asynch, atom()},
            Args :: [term()], ProtocolVersion :: number()) -> any() | {error, no_active_workers_found}.
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
            Args :: [term()], ProtocolVersion :: number(), Timeout :: pos_integer()) -> any() | {error, no_active_workers_found}.
%% ====================================================================
apply(Module, Method, Args, ProtocolVersion, Timeout) ->
    Workers = gen_server:call(request_dispatcher, {get_workers, dao}),
    apply(Module, Method, Args, ProtocolVersion, Timeout, Workers).

%% ===================================================================
%% Internal functions
%% ===================================================================

%% apply/6
%% ====================================================================
%% @doc Internal version of apply/5
%% @end
-spec apply(Module :: module(), Method :: atom() | {synch, atom()} | {asynch, atom()},
    Args :: [term()], ProtocolVersion :: number(), Timeout :: pos_integer(), [Worker :: atom()]) -> any() | {error, no_active_workers_found}.
%% ====================================================================
apply(_Module, _Method, _Args, _ProtocolVersion, _Timeout, []) ->
    {error, no_active_workers_found};
apply(Module, {asynch, Method}, Args, ProtocolVersion, Timeout, Workers) ->
    {Head, [ Worker | Tail ]} = lists:split(crypto:rand_uniform(0, length(Workers)), Workers), %% Complexity O(length(Workers))
    try gen_server:cast({dao, Worker}, {asynch, ProtocolVersion, {Module, Method, Args}}) of
        _ -> ok
    catch
        _:_ -> apply(Module, {asynch, Method}, Args, ProtocolVersion, Timeout, lists:append(Head, Tail)) %% Complexity O(length(Workers))
    end;
apply(Module, {synch, Method}, Args, ProtocolVersion, Timeout, Workers) ->
    PPid = self(),
    {Head, [ Worker | Tail ]} = lists:split(crypto:rand_uniform(0, length(Workers)), Workers),
    Pid = spawn(fun() -> receive Response -> PPid ! {self(), Response} after Timeout -> exit end end),
    try gen_server:cast({dao, Worker}, {synch, ProtocolVersion, {Module, Method, Args}, dao_lib_call, {proc, Pid}}) of
        _ ->
            receive
                {Pid, Resp} -> Resp
            after Timeout ->
                {error, timeout}
            end
    catch
        _ -> apply(Module, {synch, Method}, Args, ProtocolVersion, Timeout, lists:append(Head, Tail))
    end;
apply(Module, Method, Args, ProtocolVersion, Timeout, Workers) ->
    apply(Module, {synch, Method}, Args, ProtocolVersion, Timeout, Workers).
