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

-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/couch_db.hrl").

-import(dao_helper, [name/1]).

-ifdef(TEST).
-compile([export_all]).
-endif.

%% API
-export([save_record/2, get_record/2, remove_record/2]).

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
handle(_ProtocolVersion, {Target, Method, Args}) when
      is_atom(Method), is_list(Args), Target == dao;
      is_atom(Method), is_list(Args), Target == helper;
      is_atom(Method), is_list(Args), Target == hosts ->
    Module =
        case Target of
            dao -> dao;
            helper -> dao_helper;
            hosts -> dao_hosts
        end,
    try apply(Module, Method, Args) of
        {error, Err} -> {error, Err};
        {ok, Response} -> {ok, Response}
    catch
        error:{badmatch, {error, Err}} -> {error, Err};
        _:Error -> {error, Error}
    end;
handle(ProtocolVersion, {Method, Args}) when is_atom(Method), is_list(Args) ->
    handle(ProtocolVersion, {dao, Method, Args});
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
%% @doc Saves record Rec to db with ID = Id. Should not be used directly, use handle/2 instead.
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
    ensure_db_exists(?SYSTEM_DB_NAME),
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
    ensure_db_exists(?SYSTEM_DB_NAME),
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
%% @end
-spec remove_record(Id :: atom(), RecordName :: atom()) ->
			   ok |
			   no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
remove_record(Id, RecName) when is_atom(RecName) ->
    ensure_db_exists(?SYSTEM_DB_NAME),
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


%% ensure_db_exists/1
%% ====================================================================
%% @doc Creates DbName if not exists
-spec ensure_db_exists(DbName :: string()) ->
			      ok |
			      no_return(). % erlang:error({badmatch, any()})
%% ====================================================================
ensure_db_exists(DbName) ->
    ok = dao_helper:create_db(DbName).
