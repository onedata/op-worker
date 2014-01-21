%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements {@link worker_plugin_behaviour} callbacks and contains utility API methods. <br/>
%% DAO API functions are implemented in DAO sub-modules like: {@link dao_cluster}, {@link dao_vfs}. <br/>
%% All DAO API functions Should not be used directly, use {@link dao:handle/2} instead.
%% Module :: atom() is module suffix (prefix is 'dao_'), MethodName :: atom() is the method name
%% and ListOfArgs :: [term()] is list of argument for the method. <br/>
%% If you want to call utility methods from this module - use Module = utils
%% See {@link handle/2} for more details.
%% @end
%% ===================================================================
-module(dao).
-behaviour(worker_plugin_behaviour).

-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/couch_db.hrl").
-include_lib("veil_modules/dao/dao_types.hrl").
-include_lib("veil_modules/dao/dao_vfs.hrl").
-include_lib("veil_modules/fslogic/fslogic.hrl").
-include_lib("logging.hrl").

-import(dao_helper, [name/1]).

-define(init_storage_after_seconds,1).

-ifdef(TEST).
-compile([export_all]).
-endif.

%% worker_plugin_behaviour callbacks
-export([init/1, handle/2, cleanup/0]).

%% API
-export([save_record/1, get_record/1, remove_record/1, list_records/2, load_view_def/2, set_db/1]).
-export([doc_to_term/1,init_storage/0]).

%% ===================================================================
%% Behaviour callback functions
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1
-spec init(Args :: term()) -> Result when
    Result :: ok | {error, Error},
    Error :: term().
%% ====================================================================
init({Args, {init_status, undefined}}) ->
    ets:new(db_host_store, [named_table, public, bag, {read_concurrency, true}]),
    init({Args, {init_status, table_initialized}});
init({_Args, {init_status, table_initialized}}) -> %% Final stage of initialization. ETS table was initialized
    case application:get_env(veil_cluster_node, db_nodes) of
        {ok, Nodes} when is_list(Nodes) ->
            [dao_hosts:insert(Node) || Node <- Nodes, is_atom(Node)],
            catch setup_views(?DATABASE_DESIGN_STRUCTURE);
        _ ->
            lager:warning("There are no DB hosts given in application env variable.")
    end,

    ProcFun = fun(ProtocolVersion, {Target, Method, Args}) ->
      handle(ProtocolVersion, {Target, Method, Args})
    end,

    MapFun = fun({_, _, [File, _]}) ->
      lists:foldl(fun(Char, Sum) -> 10 * Sum + Char end, 0, File)
    end,

    SubProcList = worker_host:generate_sub_proc_list(id_generation, 6, 10, ProcFun, MapFun),

    RequestMap = fun
      ({T, M, _}) ->
        case {T, M} of
          {dao_vfs, save_new_file} -> id_generation;
          _ -> non
        end;
      (_) -> non
    end,

    DispMapFun = fun
      ({T2, M2, [File, _]}) ->
        case {T2, M2} of
          {dao_vfs, save_new_file} ->
            lists:foldl(fun(Char, Sum) -> 2 * Sum + Char end, 0, File);
          _ -> non
        end;
      (_) -> non
    end,

		erlang:send_after(?init_storage_after_seconds * 1000, self(), {timer, {asynch, 1, {utils,init_storage,[]}}}),

    #initial_host_description{request_map = RequestMap, dispatcher_request_map = DispMapFun, sub_procs = SubProcList, plug_in_state = ok};
init({Args, {init_status, _TableInfo}}) ->
    init({Args, {init_status, table_initialized}});
init(Args) ->
    %% Init Cache-ETS. Ignore the fact that other DAO worker could have created this table. In this case, this call will
    %% fail, but table is present anyway, so everyone is happy.
    case ets:info(dao_cache) of
        undefined   -> ets:new(dao_cache, [named_table, public, ordered_set, {read_concurrency, true}]);
        [_ | _]     -> ok
    end,

    %% Start linked process that will maintain cache status
    Interval =
        case application:get_env(veil_cluster_node, dao_cache_loop_time) of
            {ok, Interval1} -> Interval1;
            _               -> 30*60 %% Hardcoded 30min, just in case
        end,
    spawn_link(fun() -> cache_guard(Interval * 1000) end),

    init({Args, {init_status, ets:info(db_host_store)}}).

%% handle/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1. <br/>
%% All {Module, Method, Args} requests (second argument), executes Method with Args in {@type dao_Module} module, but with one exception:
%% If Module = utils, then dao module will be used. <br/>
%% E.g calling dao:handle(_, {vfs, some_method, [some_arg]}) will call dao_vfs:some_method(some_arg) <br/>
%% but calling dao:handle(_, {utils, some_method, [some_arg]}) will call dao:some_method(some_arg) <br/>
%% You can omit Module atom in order to use default module which is dao_cluster. <br/>
%% E.g calling dao:handle(_, {some_method, [some_arg]}) will call dao_cluster:some_method(some_arg) <br/>
%% Additionally all exceptions from called API method will be caught and converted into {error, Exception} tuple. <br/>
%% E.g. calling handle(_, {save_record, [Id, Rec]}) will execute dao_cluster:save_record(Id, Rec) and normalize return value.
%% @end
-spec handle(ProtocolVersion :: term(), Request) -> Result when
    Request :: {Method, Args} | {Mod :: atom(), Method, Args} | ping | get_version,
    Method :: atom(),
    Args :: list(),
    Result :: ok | {ok, Response} | {error, Error} | pong | Version,
    Response :: term(),
    Version :: term(),
    Error :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(ProtocolVersion, {Target, Method, Args}) when is_atom(Target), is_atom(Method), is_list(Args) ->
    put(protocol_version, ProtocolVersion), %% Some sub-modules may need it to communicate with DAO' gen_server
    Module =
        case atom_to_list(Target) of
            "utils" -> dao;
            [$d, $a, $o, $_ | T] -> list_to_atom("dao_" ++ T);
            T -> list_to_atom("dao_" ++ T)
        end,
    try apply(Module, Method, Args) of
        {error, Err} ->
            lager:error("Handling ~p:~p with args ~p returned error: ~p", [Module, Method, Args, Err]),
            {error, Err};
        {ok, Response} -> {ok, Response};
        ok -> ok;
        Other ->
            lager:error("Handling ~p:~p with args ~p returned unknown response: ~p", [Module, Method, Args, Other]),
            {error, Other}
    catch
        error:{badmatch, {error, Err}} -> {error, Err};
        _Type:Error ->
%%             lager:error("Handling ~p:~p with args ~p interrupted by exception: ~p:~p ~n ~p", [Module, Method, Args, Type, Error, erlang:get_stacktrace()]),
            {error, Error}
    end;
handle(ProtocolVersion, {Method, Args}) when is_atom(Method), is_list(Args) ->
    handle(ProtocolVersion, {cluster, Method, Args});
handle(_ProtocolVersion, _Request) ->
    lager:error("Unknown request ~p (protocol ver.: ~p)", [_Request, _ProtocolVersion]),
    {error, wrong_args}.

%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
%% ====================================================================
cleanup() ->
    ok.

%% ===================================================================
%% API functions
%% ===================================================================


%% save_record/1
%% ====================================================================
%% @doc Saves record to DB. Argument has to be either Record :: term() which will be saved<br/>
%% with random UUID as completely new document or #veil_document record. If #veil_document record is passed <br/>
%% caller may set UUID and revision info in order to update this record in DB.<br/>
%% If you got #veil_document{} via {@link dao:get_record/1}, uuid and rev_info are in place and you shouldn't touch them<br/>
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec save_record(term() | #veil_document{uuid :: string(), rev_info :: term(), record :: term(), force_update :: boolean()}) ->
    {ok, DocId :: string()} |
    {error, conflict} |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
save_record(#veil_document{uuid = "", record = Rec} = Doc) when is_tuple(Rec) ->
    save_record(Doc#veil_document{uuid = dao_helper:gen_uuid()});
save_record(#veil_document{uuid = Id, record = Rec} = Doc) when is_tuple(Rec), is_atom(Id) ->
    save_record(Doc#veil_document{uuid = atom_to_list(Id)});
save_record(#veil_document{uuid = Id, rev_info = RevInfo, record = Rec, force_update = IsForced}) when is_tuple(Rec), is_list(Id)->
    Valid = is_valid_record(Rec),
    if
        Valid -> ok;
        true ->
            lager:error("Cannot save record: ~p because it's not supported", [Rec]),
            throw(unsupported_record)
    end,
    Revs =
        if
            IsForced -> %% If Mode == update, we need to open existing doc in order to get revs
                case dao_helper:open_doc(get_db(), Id) of
                    {ok, #doc{revs = RevDef}} -> RevDef;
                    _ -> #doc{revs = RevDef} = #doc{}, RevDef
                end;
            RevInfo =/= 0 ->
                RevInfo;
            true ->
                #doc{revs = RevDef} = #doc{},
                RevDef
        end,
    case dao_helper:insert_doc(get_db(), #doc{id = dao_helper:name(Id), revs = Revs, body = term_to_doc(Rec)}) of
        {ok, _} -> {ok, Id};
        {error, Err} -> {error, Err}
    end;
save_record(Rec) when is_tuple(Rec) ->
    save_record(#veil_document{record = Rec}).


%% get_record/1
%% ====================================================================
%% @doc Retrieves record with UUID = Id from DB. Returns whole #veil_document record containing UUID, Revision Info and
%% demanded record inside. #veil_document{}.uuid and #veil_document{}.rev_info should not be ever changed. <br/>
%% You can strip wrappers if you do not need them using API functions of dao_lib module.
%% See #veil_document{} structure for more info.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec get_record(Id :: atom() | string()) ->
    {ok,#veil_document{record :: tuple()}} |
    {error, Error :: term()} |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
get_record(Id) when is_atom(Id) ->
    get_record(atom_to_list(Id));
get_record(Id) when is_list(Id) ->
    case dao_helper:open_doc(get_db(), Id) of
        {ok, #doc{body = Body, revs = RevInfo}} ->
            try {doc_to_term(Body), RevInfo} of
                {Term, RInfo} -> {ok, #veil_document{uuid = Id, rev_info = RInfo, record = Term}}
            catch
                _:Err -> {error, {invalid_document, Err}}
            end;
        {error, Error} -> Error
    end.


%% remove_record/1
%% ====================================================================
%% @doc Removes record with given UUID from DB
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec remove_record(Id :: atom() | uuid()) ->
    ok |
    {error, Error :: term()}.
%% ====================================================================
remove_record(Id) when is_atom(Id) ->
    remove_record(atom_to_list(Id));
remove_record(Id) when is_list(Id) ->
    dao_helper:delete_doc(get_db(), Id).


%% list_records/2
%% ====================================================================
%% @doc Executes view query and parses returned result into #view_result{} record. <br/>
%% Strings from #view_query_args{} are not transformed by {@link dao_helper:name/1},
%% the caller has to do it by himself.
%% @end
-spec list_records(ViewInfo :: #view_info{}, QueryArgs :: #view_query_args{}) ->
    {ok, QueryResult :: #view_result{}} | {error, term()}.
%% ====================================================================
list_records(#view_info{name = ViewName, design = DesignName, db_name = DbName}, QueryArgs) ->
    FormatKey =  %% Recursive lambda:
    fun(F, K) when is_list(K) -> [F(F, X) || X <- K];
      (_F, K) when is_binary(K) -> binary_to_list(K);
      (_F, K) -> K
    end,
    FormatDoc =
    fun([{doc, {[ {_id, Id} | [ {_rev, RevInfo} | D ] ]}}]) ->
      #veil_document{record = doc_to_term({D}), uuid = binary_to_list(Id), rev_info = dao_helper:revision(RevInfo)};
      (_) -> none
    end,

        case dao_helper:query_view(DbName, DesignName, ViewName, QueryArgs) of
            {ok, [{total_and_offset, Total, Offset} | Rows]} ->
              FormattedRows =
                [#view_row{id = binary_to_list(Id), key = FormatKey(FormatKey, Key), value = Value, doc = FormatDoc(Doc)}
                  || {row, {[ {id, Id} | [ {key, Key} | [ {value, Value} | Doc ] ] ]}} <- Rows],
              {ok, #view_result{total = Total, offset = Offset, rows = FormattedRows}};

            {ok, Rows2} when is_list(Rows2)->
              FormattedRows2 =
                [#view_row{id = non, key = FormatKey(FormatKey, Key), value = Value, doc = non}
                  || {row, {[ {key, Key} | [ {value, Value} ] ]}} <- Rows2],
              {ok, #view_result{total = length(Rows2), offset = 0, rows = FormattedRows2}};
          {error, _} = E -> throw(E);
            Other ->
                lager:error("dao_helper:query_view has returned unknown query result: ~p", [Other]),
                throw({unknown_query_result, Other})
        end.


%% ===================================================================
%% Internal functions
%% ===================================================================

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

%% setup_views/1
%% ====================================================================
%% @doc Creates or updates design documents
%% @end
-spec setup_views(DesignStruct :: list()) -> ok.
%% ====================================================================
setup_views(DesignStruct) ->
    DesignFun = fun(#design_info{name = Name, views = ViewList}, DbName) ->  %% Foreach design document
            LastCTX = %% Calculate MD5 sum of current views (read from files)
                lists:foldl(fun(#view_info{name = ViewName}, CTX) ->
                            crypto:hash_update(CTX, load_view_def(ViewName, map) ++ load_view_def(ViewName, reduce))
                        end, crypto:hash_init(md5), ViewList),

            LocalVersion = dao_helper:name(integer_to_list(binary:decode_unsigned(crypto:hash_final(LastCTX)), 16)),
            NewViewList =
                case dao_helper:open_design_doc(DbName, Name) of
                    {ok, #doc{body = Body}} -> %% Design document exists, so lets calculate MD5 sum of its views
                        ViewsField = dao_json:get_field(Body, "views"),
                        DbViews = [ dao_json:get_field(ViewsField, ViewName) || #view_info{name = ViewName} <- ViewList ],
                        EmptyString = fun(Str) when is_binary(Str) -> binary_to_list(Str); %% Helper function converting non-string value to empty string
                                         (_) -> "" end,
                        VStrings = [ EmptyString(dao_json:get_field(V, "map")) ++ EmptyString(dao_json:get_field(V, "reduce")) || {L}=V <- DbViews, is_list(L)],
                        LastCTX1 = lists:foldl(fun(VStr, CTX) -> crypto:hash_update(CTX, VStr) end, crypto:hash_init(md5), VStrings),
                        DbVersion = dao_helper:name(integer_to_list(binary:decode_unsigned(crypto:hash_final(LastCTX1)), 16)),
                        case DbVersion of %% Compare DbVersion with LocalVersion
                            LocalVersion ->
                                lager:info("DB version of design ~p is ~p and matches local version. Design is up to date", [Name, LocalVersion]),
                                [];
                            _Other ->
                                lager:info("DB version of design ~p is ~p and does not match ~p. Rebuilding design document", [Name, _Other, LocalVersion]),
                                ViewList
                        end;
                    _ ->
                        lager:info("Design document ~p in DB ~p not exists. Creating...", [Name, DbName]),
                        ViewList
                end,

            lists:map(fun(#view_info{name = ViewName}) -> %% Foreach view
                case dao_helper:create_view(DbName, Name, ViewName, load_view_def(ViewName, map), load_view_def(ViewName, reduce), LocalVersion) of
                    ok ->
                        lager:info("View ~p in design ~p, DB ~p has been created.", [ViewName, Name, DbName]);
                    _Err ->
                        lager:error("View ~p in design ~p, DB ~p creation failed. Error: ~p", [ViewName, Name, DbName, _Err])
                end
            end, NewViewList),
            DbName
        end,

    DbFun = fun(#db_info{name = Name, designs = Designs}) -> %% Foreach database
            dao_helper:create_db(Name, []),
            lists:foldl(DesignFun, Name, Designs)
        end,

    lists:map(DbFun, DesignStruct),
    ok.

%% load_view_def/2
%% ====================================================================
%% @doc Loads view definition from file.
%% @end
-spec load_view_def(Name :: string(), Type :: map | reduce) -> string().
%% ====================================================================
load_view_def(Name, Type) ->
    case file:read_file(?VIEW_DEF_LOCATION ++ Name ++ (case Type of map -> ?MAP_DEF_SUFFIX; reduce -> ?REDUCE_DEF_SUFFIX end)) of
        {ok, Data} -> binary_to_list(Data);
        _ -> ""
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



%% term_to_doc/1
%% ====================================================================
%% @doc Converts given erlang term() into valid BigCouch document body. Given term should be a record. <br/>
%% All erlang data types are allowed, although using binary() is not recommended (because JSON will treat it like a string and will fail to read it)
%% @end
-spec term_to_doc(Field :: term()) -> term().
%% ====================================================================
term_to_doc(Field) when is_number(Field) ->
    Field;
term_to_doc(Field) when is_boolean(Field); Field =:= null ->
    Field;
term_to_doc(Field) when is_pid(Field) ->
    list_to_binary(?RECORD_FIELD_PID_PREFIX ++ pid_to_list(Field));
term_to_doc(Field) when is_binary(Field) ->
    <<<<?RECORD_FIELD_BINARY_PREFIX>>/binary, Field/binary>>;   %% Binary is saved as string, so we add a prefix
term_to_doc(Field) when is_list(Field) ->
    case io_lib:printable_list(Field) of
        true -> dao_helper:name(Field);
        false -> [term_to_doc(X) || X <- Field]
    end;
term_to_doc(Field) when is_atom(Field) ->
    term_to_doc(?RECORD_FIELD_ATOM_PREFIX ++ atom_to_list(Field));  %% Atom is saved as string, so we add a prefix
term_to_doc(Field) when is_tuple(Field) ->
    IsRec = is_valid_record(Field),

    {InitObj, LField, RecName} =  %% Prepare initial structure for record or simple tuple
        case IsRec of
            true ->
                [RecName1 | Res] = tuple_to_list(Field),
                {dao_json:mk_field(dao_json:mk_obj(), ?RECORD_META_FIELD_NAME, dao_json:mk_str(atom_to_list(RecName1))), Res, RecName1};
            false ->
                {dao_json:mk_obj(), tuple_to_list(Field), none}
        end,
    FoldFun = fun(Elem, {Poz, AccIn}) ->  %% Function used in lists:foldl/3. It parses given record/tuple field
            case IsRec of                 %% and adds to Accumulator object
                true ->
                    {_, Fields, _} = ?dao_record_info(RecName),

                    % TODO temporary fix that enables use of diacritic chars in file names
                    Value = case {RecName, lists:nth(Poz, Fields)} of
                                % Exclusively for filenames, apply conversion to binary
                                {file, name} -> list_to_binary(Elem);
                                % Standard conversion
                                _ -> term_to_doc(Elem)
                            end,
                    % </endfix>

                    {Poz + 1, dao_json:mk_field(AccIn, atom_to_list(lists:nth(Poz, Fields)), Value)};
                false ->
                    {Poz + 1, dao_json:mk_field(AccIn, ?RECORD_TUPLE_FIELD_NAME_PREFIX ++ integer_to_list(Poz), term_to_doc(Elem))}
            end
        end,
    {_, {Ret}} = lists:foldl(FoldFun, {1, InitObj}, LField),
    {lists:reverse(Ret)};
term_to_doc(Field) ->
    lager:error("Cannot convert term to document because field: ~p is not supported", [Field]),
    throw({unsupported_field, Field}).


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
doc_to_term(Field) when is_binary(Field) -> %% Binary type means that it is atom, string or binary.
    SField = binary_to_list(Field),         %% Prefix tells us which type is it
    BinPref = string:str(SField, ?RECORD_FIELD_BINARY_PREFIX),
    AtomPref = string:str(SField, ?RECORD_FIELD_ATOM_PREFIX),
    PidPref = string:str(SField, ?RECORD_FIELD_PID_PREFIX),
    if
        BinPref == 1 -> list_to_binary(string:sub_string(SField, length(?RECORD_FIELD_BINARY_PREFIX) + 1));
        AtomPref == 1 -> list_to_atom(string:sub_string(SField, length(?RECORD_FIELD_ATOM_PREFIX) + 1));
        PidPref == 1 -> list_to_pid(string:sub_string(SField, length(?RECORD_FIELD_PID_PREFIX) + 1));
        true -> SField
    end;
doc_to_term(Field) when is_list(Field) ->
    [doc_to_term(X) || X <- Field];
doc_to_term({Fields}) when is_list(Fields) -> %% Object stores tuple which can be an erlang record
    Fields1 = [{binary_to_list(X), Y} || {X, Y} <- Fields],
    {IsRec, FieldsInit, RecName} =
        case lists:keyfind(?RECORD_META_FIELD_NAME, 1, Fields1) of  %% Search for record meta field
            {_, RecName1} -> %% Meta field found. Check if it is valid record name. Either way - prepare initial working structures
                {case is_valid_record(binary_to_list(RecName1)) of true -> true; _ -> partial end,
                    lists:keydelete(?RECORD_META_FIELD_NAME, 1, Fields1), list_to_atom(binary_to_list(RecName1))};
            _ ->
                DataTmp = [{list_to_integer(lists:filter(fun(E) -> (E >= $0) andalso (E =< $9) end, Num)), Data} || {Num, Data} <- Fields1],
                {false, lists:sort(fun({A, _}, {B, _}) -> A < B end, DataTmp), none}
        end,
    case IsRec of
        false -> %% Object is an tuple. Simply create tuple from successive fields
            list_to_tuple([doc_to_term(Data) || {_, Data} <- FieldsInit]);
        partial -> %% Object is an unsupported record. We are gonna build record based only on current structure from DB
            list_to_tuple([RecName | [doc_to_term(Data) || {_, Data} <- FieldsInit]]);
        true -> %% Object is an supported record. We are gonna build record based on current erlang record structure (new fields will get default values)
            {_, FNames, InitRec} = ?dao_record_info(RecName),
            FoldFun = fun(Elem, {Poz, AccIn}) ->
                    case lists:keyfind(atom_to_list(Elem), 1, FieldsInit) of
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



%% cache_guard/1
%% ====================================================================
%% @doc Loops infinitly (sleeps Timeout ms on each loop) and runs fallowing predefined tasks on each loop:
%%          - FUSE session cleanup
%%      When used in newly spawned process, the process will infinitly fire up the the tasks while
%%      sleeping 'Timeout' ms between subsequent loops.
%%      NOTE: The function crashes is 'dao_cache' ETS is not available.
%% @end
-spec cache_guard(Timeout :: non_neg_integer()) -> no_return().
%% ====================================================================
cache_guard(Timeout) ->
    timer:sleep(Timeout),

    [_ | _] = ets:info(dao_cache), %% Crash this process if dao_cache ETS doesn't exist
                                   %% Because this process is linked to DAO's worker, whole DAO will crash

    %% Run all tasks (async)
    spawn(dao_cluster, clear_sessions, []), %% Clear FUSE session

    cache_guard(Timeout).

%% init_storage/0
%% ====================================================================
%% @doc Inserts storage defined during worker instalation to database (if db already has defined storage,
%% the function only replaces StorageConfigFile with that definition)
%% @end
-spec init_storage() -> ok | {error, Error :: term()}.
%% ====================================================================
init_storage() ->
	try
		%get storage config file path
		GetEnvResult = application:get_env(veil_cluster_node,storage_config_path),
		case GetEnvResult of
			{ok,_} -> ok;
			undefined ->
				lager:error("Could not get 'storage_config_path' environment variable"),
				throw(get_env_error)
		end,
		{ok,StorageFilePath} = GetEnvResult,

		%get storage list from db
		{Status1,ListStorageValue} = dao_lib:apply(dao_vfs, list_storage, [],1),
		case Status1 of
			ok -> ok;
			error ->
				lager:error("Could not list existing storages"),
				throw(ListStorageValue)
		end,
		ActualDbStorages = [X#veil_document.record || X <- ListStorageValue],

		case ActualDbStorages of
			[] -> %db empty, insert storage
				%read from file
				{Status2,FileConsultValue} = file:consult(StorageFilePath),
				case Status2 of
					ok -> ok;
					error ->
						lager:error("Could not read storage config file"),
						throw(FileConsultValue)
				end,
				[StoragePreferences] = FileConsultValue,

				%parse storage preferences
				UserPreferenceToGroupInfo = fun (GroupPreference) ->
					case GroupPreference of
						[{name,cluster_fuse_id},{root,Root}] ->
							#fuse_group_info{name = ?CLUSTER_FUSE_ID, storage_helper = #storage_helper_info{name = "DirectIO", init_args = [Root]}};
						[{name,Name},{root,Root}] ->
							#fuse_group_info{name = Name, storage_helper = #storage_helper_info{name = "DirectIO", init_args = [Root]}}
					end
				end,
				FuseGroups=try
					lists:map(UserPreferenceToGroupInfo,StoragePreferences)
				catch
					_Type:Err ->
						lager:error("Wrong format of storage config file"),
						throw(Err)
				end,

				%create storage
				{Status3, Value} = apply(fslogic_storage, insert_storage, ["ClusterProxy", [], FuseGroups]),
				case Status3 of
					ok ->
						ok;
					error ->
						lager:error("Error during inserting storage to db"),
						throw(Value)
				end;

			_NotEmptyList -> %db not empty
				ok
		end
	catch
		Type:Error ->
			lager:error("Error during storage init: ~p:~p",[Type,Error]),
			{error,Error}
	end,
	ok.
