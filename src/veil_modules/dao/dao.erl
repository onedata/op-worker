%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour callbacks and contains utility API methods
%% DAO API functions are implemented in DAO sub-modules like: dao_cluster, dao_vfs
%% All DAO API functions should not be called directly. Call dao:handle(_, {Module, MethodName, ListOfArgs) instead, when
%% Module :: atom() is module suffix (prefix is 'dao_'), MethodName :: atom() is the method name
%% and ListOfArgs :: [term()] is list of argument for the method.
%% If you want to call utility methods from this module - use Module = utils
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

%% API
-export([save_record/2, get_record/2, remove_record/2]).

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
%% All {Module, Method, Args} requests (second argument), executes Method with Args in 'dao_Module' module, but with one exception:
%% If Module = utils, then dao module will be used.
%% E.g calling dao:handle(_, {vfs, some_method, [some_arg]}) will call dao_vfs:some_method(some_arg)
%% but calling dao:handle(_, {utils, some_method, [some_arg]}) will call dao:some_method(some_arg)
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
    Module =
        case Target of
            utils -> dao;
            T -> list_to_atom("dao_" ++ atom_to_list(T))
        end,
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

%% save_record/2
%% ====================================================================
%% @doc Saves record Rec to DB with ID = Id. Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
-spec save_record(Id :: atom(), Rec :: tuple()) ->
    ok |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
save_record(Id, Rec) when is_tuple(Rec), is_atom(Id) ->
    Size = tuple_size(Rec),
    RecName = atom_to_list(element(1, Rec)),
    Fields =
        case ?dao_record_info(element(1, Rec)) of
            {RecSize, Flds} when RecSize =:= Size, is_list(Flds) -> Flds;
            {error, E} -> throw(E);
            _ -> throw(invalid_record)
        end,
    dao_helper:ensure_db_exists(?SYSTEM_DB_NAME),
    DocName = ?RECORD_INSTANCES_DOC_PREFIX ++ RecName,
    RecData =
        case dao_helper:open_doc(?SYSTEM_DB_NAME, DocName) of
            {ok, Doc} -> Doc;
            {error, {not_found, _}} ->
                NewDoc = dao_json:mk_field(dao_json:mk_doc(DocName), "instances", []),
                {ok, _Rev} = dao_helper:insert_doc(?SYSTEM_DB_NAME, NewDoc),
                {ok, Doc} = dao_helper:open_doc(?SYSTEM_DB_NAME, DocName),
                Doc;
            {error, E1} -> throw(E1)
        end,
    Instances = [X || X <- dao_json:get_field(RecData, "instances"), is_binary(dao_json:get_field(X, "_ID_")), dao_json:mk_str(dao_json:get_field(X, "_ID_")) =/= dao_json:mk_str(Id)],
    [_ | FValues] = [dao_json:mk_bin(X) || X <- tuple_to_list(Rec)],
    NewInstance = dao_json:mk_fields(dao_json:mk_obj(), ["_ID_" | Fields], [dao_json:mk_str(Id) | FValues]),
    NewInstances = [NewInstance | Instances],
    NewDoc1 = dao_json:mk_field(RecData, "instances", NewInstances),
    {ok, _Rev1} = dao_helper:insert_doc(?SYSTEM_DB_NAME, NewDoc1),
    {ok, saved}.

%% get_record/2
%% ====================================================================
%% @doc Retrieves record with ID = Id from DB.
%% If second argument is an atom - record name - every field that was added
%% after last record save, will get value 'undefined'.
%% If second argument is an record instance, every field that was added
%% after last record save, will get same value as in given record instance
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% @end
-spec get_record(Id :: atom(), RecordNameOrRecordTemplate :: atom() | tuple()) ->
    Record :: tuple() |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
get_record(Id, NewRecord) when is_atom(NewRecord) ->
    {RecSize, _Fields} =
        case ?dao_record_info(NewRecord) of
            {_, Flds} = R when is_list(Flds) -> R;
            {error, E} -> throw(E);
            _ -> throw(invalid_record)
        end,
    EmptyRecord = [NewRecord | [undefined || _ <- lists:seq(1, RecSize - 1)]],
    get_record(Id, list_to_tuple(EmptyRecord));
get_record(Id, EmptyRecord) when is_tuple(EmptyRecord) ->
    RecName = atom_to_list(element(1, EmptyRecord)),
    {_RecSize, Fields} =
        case ?dao_record_info(element(1, EmptyRecord)) of
            {_, Flds} = R when is_list(Flds) -> R;
            {error, E} -> throw(E);
            _ -> throw(invalid_record)
        end,
    SFields = [atom_to_list(X) || X <- Fields],
    dao_helper:ensure_db_exists(?SYSTEM_DB_NAME),
    DocName = ?RECORD_INSTANCES_DOC_PREFIX ++ RecName,
    RecData =
        case dao_helper:open_doc(?SYSTEM_DB_NAME, DocName) of
            {ok, Doc} -> Doc;
            {error, {not_found, _}} -> throw(record_data_not_found);
            {error, E1} -> throw(E1)
        end,
    Instance = [X || X <- dao_json:get_field(RecData, "instances"), is_binary(dao_json:get_field(X, "_ID_")), dao_json:mk_str(dao_json:get_field(X, "_ID_")) =:= dao_json:mk_str(Id)],
    [_ | SavedFields] =
        case Instance of
            [] -> throw(record_data_not_found);
            [Inst] -> dao_json:get_fields(Inst);
            _ -> throw(invalid_data)
        end,
    lists:foldl(fun({Name, Value}, Acc) ->
        case string:str(SFields, [Name]) of 0 -> Acc; Poz -> setelement(1 + Poz, Acc, binary_to_term(Value)) end end, EmptyRecord, SavedFields).


%% remove_record/2
%% ====================================================================
%% @doc Removes record with given Id an RecordName from DB
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% @end
-spec remove_record(Id :: atom(), RecordName :: atom()) ->
    ok |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
remove_record(Id, RecName) when is_atom(RecName) ->
    dao_helper:ensure_db_exists(?SYSTEM_DB_NAME),
    DocName = ?RECORD_INSTANCES_DOC_PREFIX ++ atom_to_list(RecName),
    case dao_helper:open_doc(?SYSTEM_DB_NAME, DocName) of
        {ok, Doc} ->
            Instances = [X || X <- dao_json:get_field(Doc, "instances"), is_binary(dao_json:get_field(X, "_ID_")), dao_json:mk_str(dao_json:get_field(X, "_ID_")) =/= dao_json:mk_str(Id)],
            NewDoc1 = dao_json:mk_field(Doc, "instances", Instances),
            {ok, _Rev} = dao_helper:insert_doc(?SYSTEM_DB_NAME, NewDoc1),
            ok;
        {error, {not_found, _}} -> ok;
        {error, E1} -> throw(E1)
    end.


%% ===================================================================
%% Internal functions
%% ===================================================================


