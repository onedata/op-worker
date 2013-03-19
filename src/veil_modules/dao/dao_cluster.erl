%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module gives high level DB API which contain veil cluster specific utility methods.
%% All DAO API functions should not be called directly. Call dao:handle(_, {cluster, MethodName, ListOfArgs) instead.
%% See dao:handle/2 for more details.
%% @end
%% ===================================================================
-module(dao_cluster).

-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/couch_db.hrl").

%% API
-export([save_record/2, get_record/2, remove_record/2]).

-ifdef(TEST).
-compile([export_all]).
-endif.

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
    
