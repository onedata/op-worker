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

-include_lib("veil_modules/dao/dao.hrl").
%% API
-export([set_db/1, get_db/0, record_info/1, is_valid_record/1, sequential_synch_call/3]).

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
    is_valid_record(list_to_atom(Record));
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
