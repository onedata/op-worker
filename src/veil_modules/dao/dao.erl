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
-export([init/1, handle/2, cleanup/0]).

%% API
-export([save_record/3, save_record/2, save_record/1, get_record/1, remove_record/1]).

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

%% cleanup/0
%% ====================================================================
%% @doc worker_plugin_behaviour callback cleanup/0
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
%% ====================================================================
cleanup() ->
    Pid = whereis(db_host_store_proc),
    monitor(process, Pid),
    Pid ! {self(), shutdown},
    receive {'DOWN', _Ref, process, Pid, normal} -> ok after 1000 -> {error, timeout} end.

%% ===================================================================
%% API functions
%% ===================================================================


%% save_record/2
%% ====================================================================
%% @doc Same as save_record/3 but with Mode = insert
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% @end
-spec save_record(Rec :: tuple(), Id :: atom() | string()) ->
    {ok, DocId :: string()} |
    {error, conflict} |
    no_return(). % erlang:error(any()) | throw(any())
save_record(Rec, Id) ->
    save_record(Rec, Id, insert).

%% save_record/3
%% ====================================================================
%% @doc Saves record Rec to DB as document with UUID = Id.
%% If Mode == update then the document with given UUID will be overridden
%% By default however saving to existing document will fail with {error, conflict}
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% @end
-spec save_record(Rec :: tuple(), Id :: atom() | string(), Mode :: update | insert) ->
    {ok, DocId :: string()} |
    {error, conflict} |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
save_record(Rec, Id, Mode) when is_tuple(Rec), is_atom(Id) ->
    save_record(Rec, atom_to_list(Id), Mode);
save_record(Rec, Id, Mode) when is_tuple(Rec), is_list(Id)->
    Valid = is_valid_record(Rec),
    if
        Valid -> ok;
        true -> throw(unsupported_record)
    end,
    dao_helper:ensure_db_exists(?SYSTEM_DB_NAME),
    Revs =
        if
            Mode =:= update ->
                case dao_helper:open_doc(?SYSTEM_DB_NAME, Id) of
                    {ok, #doc{revs = RevDef}} -> RevDef;
                    _ -> #doc{revs = RevDef} = #doc{}, RevDef
                end;
            true ->
                #doc{revs = RevDef} = #doc{},
                RevDef
        end,
    case dao_helper:insert_doc(?SYSTEM_DB_NAME, #doc{id = dao_helper:name(Id), revs = Revs, body = term_to_doc(Rec)}) of
        {ok, _} -> {ok, Id};
        {error, Err} -> Err
    end.


%% save_record/1
%% ====================================================================
%% @doc Saves record Rec to DB as document with random UUID. Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
-spec save_record(Rec :: tuple()) ->
    {ok, DocId :: string()} |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
save_record(Rec) when is_tuple(Rec) ->
    save_record(Rec, dao_helper:gen_uuid(), insert).


%% get_record/1
%% ====================================================================
%% @doc Retrieves record with UUID = Id from DB.
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% @end
-spec get_record(Id :: atom() | string()) ->
    Record :: tuple() |
    {error, Error :: term()} |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
get_record(Id) when is_atom(Id) ->
    get_record(atom_to_list(Id));
get_record(Id) when is_list(Id) ->
    dao_helper:ensure_db_exists(?SYSTEM_DB_NAME),
    case dao_helper:open_doc(?SYSTEM_DB_NAME, Id) of
        {ok, #doc{body = Body}} ->
            try doc_to_term(Body) of
                Term -> Term
            catch
                _:Err -> {error, {invalid_document, Err}}
            end;
            {error, Error} -> Error
    end.


%% remove_record/1
%% ====================================================================
%% @doc Removes record with given UUID from DB
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% @end
-spec remove_record(Id :: atom()) ->
    ok |
    {error, Error :: term()}.
%% ====================================================================
remove_record(Id) when is_atom(Id) ->
    remove_record(atom_to_list(Id));
remove_record(Id) when is_list(Id) ->
    dao_helper:ensure_db_exists(?SYSTEM_DB_NAME),
    dao_helper:delete_doc(?SYSTEM_DB_NAME, Id).


%% ===================================================================
%% Internal functions
%% ===================================================================

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
        {_Size, _Fields, _} -> true;
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



%% term_to_doc/1
%% ====================================================================
%% @doc Converts given erlang term() into valid BigCouch document body. Given term should be a record
%% All erlang data types are allowed, although using binary() is not recommended (because JSON will treat it like a string and will fail to read it)
%% @end
-spec term_to_doc(Field :: term()) -> term().
%% ====================================================================
term_to_doc(Field) when is_number(Field) ->
    Field;
term_to_doc(Field) when is_boolean(Field); Field =:= null ->
    Field;
term_to_doc(Field) when is_binary(Field) ->
    <<<<?RECORD_FIELD_BINARY_PREFIX>>/binary, Field/binary>>;
term_to_doc(Field) when is_list(Field) ->
    case io_lib:printable_list(Field) of
        true -> dao_helper:name(Field);
        false -> [term_to_doc(X) || X <- Field]
    end;
term_to_doc(Field) when is_atom(Field) ->
    term_to_doc(?RECORD_FIELD_ATOM_PREFIX ++ atom_to_list(Field));
term_to_doc(Field) when is_tuple(Field) ->
    IsRec = is_valid_record(Field),

    {InitObj, LField, RecName} =
        case IsRec of
            true ->
                [RecName1 | Res] = tuple_to_list(Field),
                {dao_json:mk_field(dao_json:mk_obj(), ?RECORD_META_FIELD_NAME, dao_json:mk_str(atom_to_list(RecName1))), Res, RecName1};
            false ->
                {dao_json:mk_obj(), tuple_to_list(Field), none}
        end,
    FoldFun = fun(Elem, {Poz, AccIn}) ->
            case IsRec of
                true ->
                    {_, Fields, _} = ?dao_record_info(RecName),
                    {Poz + 1, dao_json:mk_field(AccIn, atom_to_list(lists:nth(Poz, Fields)), term_to_doc(Elem))};
                false ->
                    {Poz + 1, dao_json:mk_field(AccIn, ?RECORD_TUPLE_FIELD_NAME_PREFIX ++ integer_to_list(Poz), term_to_doc(Elem))}
            end
        end,
    {_, {Ret}} = lists:foldl(FoldFun, {1, InitObj}, LField),
    {lists:reverse(Ret)}.


%% doc_to_term/1
%% ====================================================================
%% @doc Converts given valid BigCouch document body into erlang term().
%% If document contains saved record which is a valid record (see is_valid_record/1),
%% then structure of the returned record will be updated
%% @end
-spec doc_to_term(Field :: term()) -> term().
%% ====================================================================
doc_to_term(Field) when is_number(Field); is_atom(Field) ->
    Field;
doc_to_term(Field) when is_binary(Field) ->
    SField = binary_to_list(Field),
    BinPref = string:str(SField, ?RECORD_FIELD_BINARY_PREFIX),
    AtomPref = string:str(SField, ?RECORD_FIELD_ATOM_PREFIX),
    if
        BinPref == 1 -> list_to_binary(string:sub_string(SField, length(?RECORD_FIELD_BINARY_PREFIX) + 1));
        AtomPref == 1 -> list_to_atom(string:sub_string(SField, length(?RECORD_FIELD_ATOM_PREFIX) + 1));
        true -> SField
    end;
doc_to_term(Field) when is_list(Field) ->
    [doc_to_term(X) || X <- Field];
doc_to_term({Fields}) when is_list(Fields) ->
    Fields1 = [{binary_to_list(X), Y} || {X, Y} <- Fields],
    {IsRec, FieldsTmp, RecName} =
        case lists:keyfind(?RECORD_META_FIELD_NAME, 1, Fields1) of
            {_, RecName1} ->
                {case is_valid_record(binary_to_list(RecName1)) of true -> true; _ -> partial end,
                    lists:keydelete(?RECORD_META_FIELD_NAME, 1, Fields1), list_to_atom(binary_to_list(RecName1))};
            _ -> {false, [{list_to_integer(lists:filter(fun(E) -> (E >= $0) andalso (E =< $9) end, Num)), Data} || {Num, Data} <- Fields1], none}
        end,
    Fields2 = lists:sort(fun({A, _}, {B, _}) -> A < B end, FieldsTmp),
    case IsRec of
        false ->
            list_to_tuple([doc_to_term(Data) || {_, Data} <- Fields2]);
        partial ->
            list_to_tuple([RecName | [doc_to_term(Data) || {_, Data} <- Fields2]]);
        true ->
            {_, FNames, InitRec} = ?dao_record_info(RecName),
            FoldFun = fun(Elem, {Poz, AccIn}) ->
                    case lists:keyfind(atom_to_list(Elem), 1, Fields2) of
                        {_, Data} ->
                            {Poz + 1, setelement(Poz, AccIn, doc_to_term(Data))};
                        _ ->
                            {Poz + 1, AccIn}
                    end
                end,
            {_, Ret} = lists:foldl(FoldFun, {2, InitRec}, FNames),
            Ret
    end;
doc_to_term(_) ->
    throw(invalid_document).