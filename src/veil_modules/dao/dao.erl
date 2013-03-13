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
-export([save_record/2]).

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
                NewDoc = #doc{id = name(DocName), body = {[{<<"instances">>, []}]}},
                {ok, _Rev} = dao_helper:insert_doc(?SYSTEM_DB_NAME, NewDoc),
                {ok, Doc} = dao_helper:open_doc(?SYSTEM_DB_NAME, DocName),
                Doc;
            {error, E1} -> throw(E1)
        end,
    {[{<<"instances">>, Instances} | DocFields]} = RecData#doc.body, % <<"instances">> field has to be first field in document !
    [_ | FValues] = tuple_to_list(Rec),
    RecFields = [{list_to_binary(atom_to_list(X)), term_to_binary(Y)} || {X, Y} <- lists:zip(Fields, FValues)],
    Instance = {[{<<"ID">>, list_to_binary(atom_to_list(Id))}, {<<"fields">>, {RecFields}}]},
    NewInstances = [{X} || {X} <- Instances, list_to_binary(atom_to_list(Id)) =/= element(2, lists:nth(1, X))],
    NewDoc1 = RecData#doc{body = {[{<<"instances">>, [Instance | NewInstances]} | DocFields]}},
    {ok, _Rev1} = dao_helper:insert_doc(?SYSTEM_DB_NAME, NewDoc1),
    ok.

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
