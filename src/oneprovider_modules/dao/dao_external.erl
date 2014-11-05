%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module provides application specific functions needed by dao
%% library to work with database
%% @end
%% ===================================================================
-module(dao_external).
-author("Tomasz Lichon").

-include("registered_names.hrl").
-include_lib("oneprovider_modules/dao/dao.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([set_db/1, get_db/0, record_info/1, is_valid_record/1, sequential_synch_call/3, view_def_location/0, on_doc_save/4, on_doc_get/3, on_view_query/3]).

on_doc_save(DbName, #db_document{} = Doc, NewRew, Opts) ->
    %% ?info("on doc_save ==============================> ~p ~p ~p ~p", [DbName, Doc, NewRew, Opts]),
    case lists:member(replicated_changes, Opts) of
        false ->
            ok = gen_server:call(?Dispatcher_Name, {dbsync, 1, {{event, doc_saved}, {DbName, Doc, NewRew, Opts}}});
        true ->
            ok
    end;
on_doc_save(DbName, Unkn, NewRew, Opts) ->
    ?info("Unknown doc_save ========================> ~p ~p ~p ~p", [DbName, Unkn, NewRew, Opts]),
    ok.

on_doc_get(DbName, DocUUID, DocRev) ->
    case DocUUID of
        <<"">> -> ok;
        "" -> ok;
        _ ->
            %% ?info("on doc_get ========================> ~p ~p ~p", [DbName, DocUUID, DocRev]),
            ok = gen_server:call(?Dispatcher_Name, {dbsync, 1, {{event, doc_requested}, {DbName, DocUUID, DocRev}}})
    end.

on_view_query(ViewInfo, QueryArgs, UUIDs) ->
    %% ?info("on view_query ========================> ~p ~p ~p", [ViewInfo, QueryArgs, UUIDs]),
    ok = gen_server:call(?Dispatcher_Name, {dbsync, 1, {{event, view_queried}, {ViewInfo, QueryArgs, UUIDs}}}).

%% set_db/1
%% ====================================================================
%% @doc Sets current working database name
%% @end
-spec set_db(DbName :: string()) -> ok.
%% ====================================================================
set_db(DbName) ->
    put(current_db, DbName).

%% get_db/0
%% ====================================================================
%% @doc Gets current working database name
%% @end
-spec get_db() -> DbName :: string().
%% ====================================================================
get_db() ->
    case get(current_db) of
        DbName when is_list(DbName) ->
            DbName;
        _ ->
            ?DEFAULT_DB
    end.

%% is_valid_record/1
%% ====================================================================
%% @doc Checks if given record/record name is supported and existing record
%% @end
-spec is_valid_record(Record :: atom() | string() | tuple()) -> boolean().
%% ====================================================================
is_valid_record(Record) when is_list(Record) ->
  try
    is_valid_record(list_to_existing_atom(Record))
  catch
    _:_ ->
      ?error("Checking record that should not exist: ~p", [Record]),
      false
  end;
is_valid_record(Record) when is_atom(Record) ->
    case ?dao_record_info(Record) of
        {_Size, _Fields, _} -> true;    %% When checking only name of record, we omit size check
        _ -> false
    end;
is_valid_record(Record) when not is_tuple(Record); not is_atom(element(1, Record)) ->
    false;
is_valid_record(Record) ->
    case ?dao_record_info(element(1, Record)) of
        {Size, Fields, _} when is_list(Fields), tuple_size(Record) =:= Size ->
            true;
        _ -> false
    end.

%% record_info/1
%% ====================================================================
%% @doc Returns info about given record
%% @end
-spec record_info(Record :: atom() | string() | tuple()) -> boolean().
%% ====================================================================
record_info(Record) ->
    ?dao_record_info(Record).

%% sequential_synch_call/3
%% ====================================================================
%% @doc Synchronizes sequentially multiple calls to given dao function
-spec sequential_synch_call(Module :: atom(), Function ::atom(), Args :: list()) -> Result :: term().
%% ====================================================================
sequential_synch_call(Module,Function,Args) ->
    PPid = self(),
    Pid = spawn(fun() -> receive Resp -> PPid ! {self(), Resp} after 1000 -> exited end end),
    gen_server:cast(dao_worker, {sequential_synch, get(protocol_version), {Module, Function, [sequential, Args]}, {proc, Pid}}),
    receive
        {Pid, Response} -> Response
    after 1000 ->
        {error, timeout}
    end.

%% view_def_location/0
%% ====================================================================
%% @doc Returns location of database views definitions
-spec view_def_location() -> Location :: string().
%% ====================================================================
view_def_location() ->
    {ok, Location} = application:get_env(oneprovider_node, view_def_location),
    atom_to_list(Location).
