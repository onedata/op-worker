%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Low level BigCouch DB API
%% @end
%% ===================================================================
-module(dao_helper).

-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/dao_helper.hrl").

-define(ADMIN_USER_CTX, {user_ctx, #user_ctx{roles = [<<"_admin">>]}}).

-ifdef(TEST).
-compile([export_all]).
-endif.

-import(dao_hosts, [call/2]).

%% API
-export([name/1, gen_uuid/0, revision/1]).
-export([list_dbs/0, list_dbs/1, get_db_info/1, get_doc_count/1, create_db/1, create_db/2]).
-export([delete_db/1, delete_db/2, open_doc/2, open_doc/3, insert_doc/2, insert_doc/3, delete_doc/2, delete_docs/2]).
-export([insert_docs/2, insert_docs/3, open_design_doc/2, create_view/6, query_view/3, query_view/4]).

%% ===================================================================
%% API functions
%% ===================================================================

%% ===================================================================
%% DB Management
%% ===================================================================

%% list_dbs/0
%% ====================================================================
%% @doc Lists all dbs
-spec list_dbs() -> {ok, [string()]} | {error, term()}.
%% ====================================================================
list_dbs() ->
    list_dbs("").

%% list_dbs/1
%% ====================================================================
%% @doc Lists all dbs that starts with Prefix
-spec list_dbs(Prefix :: string()) -> {ok, [string()]} | {error, term()}.
%% ====================================================================
list_dbs(Prefix) ->
    case normalize_return_term(call(all_dbs, [name(Prefix)])) of
        {ok, List} when is_list(List) ->
            {ok, [?b2l(X) || X <- List]};
        Error -> Error
    end.

%% get_doc_count/1
%% ====================================================================
%% @doc Returns doc count for given DbName
-spec get_doc_count(DbName :: string()) -> {ok, non_neg_integer()} | {error, database_does_not_exist} | {error, term()}.
%% ====================================================================
get_doc_count(DbName) ->
    case normalize_return_term(call(get_doc_count, [name(DbName)])) of
        {error, {exit_error, database_does_not_exist}} ->
          ?warning("Cannot get_doc_count for not existing db: ~p", [DbName]),
          {error, database_does_not_exist};
        Other -> Other
    end.

%% get_db_info/1
%% ====================================================================
%% @doc Returns db info for the given DbName
-spec get_db_info(DbName :: string()) ->
    {ok, [
    {instance_start_time, binary()} |
    {doc_count, non_neg_integer()} |
    {doc_del_count, non_neg_integer()} |
    {purge_seq, non_neg_integer()} |
    {compact_running, boolean()} |
    {disk_size, non_neg_integer()} |
    {disk_format_version, pos_integer()}
    ]} | {error, database_does_not_exist} | {error, term()}.
%% ====================================================================
get_db_info(DbName) ->
    case normalize_return_term(call(get_db_info, [name(DbName)])) of
        {error, {exit_error, database_does_not_exist}} ->
          ?warning("Cannot get_db_info for not existing db: ~p", [DbName]),
          {error, database_does_not_exist};
        Other -> Other
    end.

%% create_db/1
%% ====================================================================
%% @doc Creates db named DbName. If db already does nothing and returns 'ok'
-spec create_db(DbName :: string()) -> ok | {error, term()}.
%% ====================================================================
create_db(DbName) ->
    create_db(DbName, []).

%% create_db/2
%% ====================================================================
%% @doc Creates db named DbName with Opts.
%% Options can include values for q and n,
%% for example {q, "8"} and {n, "3"}, which
%% control how many shards to split a database into
%% and how many nodes each doc is copied to respectively.
%% @end
-spec create_db(DbName :: string(), Opts :: [Option]) -> ok | {error, term()} when
    Option :: atom() | {atom(), term()}.
%% ====================================================================
create_db(DbName, Opts) ->
    case normalize_return_term(call(create_db, [name(DbName), Opts])) of
        {error, file_exists} -> ok;
        Other -> Other
    end.

%% delete_db/1
%% ====================================================================
%% @doc Deletes db named DbName
-spec delete_db(DbName :: string()) -> ok | {error, database_does_not_exist} | {error, term()}.
%% ====================================================================
delete_db(DbName) ->
    delete_db(DbName, []).

%% delete_db/2
%% ====================================================================
%% @doc Deletes db named DbName
-spec delete_db(DbName :: string(), Opts :: [Option]) -> ok | {error, database_does_not_exist} | {error, term()} when
    Option :: atom() | {atom(), term()}.
%% ====================================================================
delete_db(DbName, Opts) ->
    case normalize_return_term(call(delete_db, [name(DbName), Opts])) of
        {error, {exit_error, database_does_not_exist}} ->
          ?warning("Cannot delete_db for not existing db: ~p", [DbName]),
          {error, database_does_not_exist};
        Other -> Other
    end.


%% ===================================================================
%% Documents Management
%% ===================================================================

%% open_doc/2
%% ====================================================================
%% @doc Returns document with a given DocID
-spec open_doc(DbName :: string(), DocID :: string()) -> {ok, #doc{}} | {error, {not_found, missing | deleted}} | {error, term()}.
%% ====================================================================
open_doc(DbName, DocID) ->
    open_doc(DbName, DocID, []).


%% open_doc/3
%% ====================================================================
%% @doc Returns document with a given DocID
-spec open_doc(DbName :: string(), DocID :: string(), Opts :: [Option]) -> {ok, #doc{}} | {error, {not_found, missing | deleted}} | {error, term()} when
    Option :: atom() | {atom(), term()}.
%% ====================================================================
open_doc(DbName, DocID, Opts) ->
    normalize_return_term(call(open_doc, [name(DbName), name(DocID), Opts])).


%% insert_doc/2
%% ====================================================================
%% @doc Inserts doc to db
-spec insert_doc(DbName :: string(), Doc :: #doc{}) -> {ok, {RevNum :: non_neg_integer(), RevBin :: binary()}} | {error, conflict} | {error, term()}.
%% ====================================================================
insert_doc(DbName, Doc) ->
    insert_doc(DbName, Doc, []).

%% insert_doc/3
%% ====================================================================
%% @doc Inserts doc to db
-spec insert_doc(DbName :: string(), Doc :: #doc{}, Opts :: [Option]) -> {ok, {RevNum :: non_neg_integer(), RevBin :: binary()}} | {error, conflict} | {error, term()} when
    Option :: atom() | {atom(), term()}.
%% ====================================================================
insert_doc(DbName, Doc, Opts) ->
    Go = fun() -> normalize_return_term(call(update_doc, [name(DbName), Doc, Opts])) end,
    case Go() of
        {error, {_, database_does_not_exist}} ->
            create_db(DbName),
            Go();
        Other -> Other
    end.


%% insert_docs/2
%% ====================================================================
%% @doc Inserts list of docs to db
-spec insert_docs(DbName :: string(), [Doc :: #doc{}]) -> {ok, term()} | {error, term()}.
%% ====================================================================
insert_docs(DbName, Docs) ->
    insert_docs(DbName, Docs, []).

%% insert_docs/3
%% ====================================================================
%% @doc Inserts list of docs to db
-spec insert_docs(DbName :: string(), [Doc :: #doc{}], Opts :: [Option]) -> {ok, term()} | {error, term()} when
    Option :: atom() | {atom(), term()}.
%% ====================================================================
insert_docs(DbName, Docs, Opts) ->
    normalize_return_term(call(update_docs, [name(DbName), Docs, Opts])).


%% delete_doc/2
%% ====================================================================
%% @doc Deletes doc from db
-spec delete_doc(DbName :: string(), DocID :: string()) -> ok | {error, missing} | {error, deleted} | {error, term()}.
%% ====================================================================
delete_doc(DbName, DocID) ->
    case open_doc(DbName, DocID) of
        {ok, Doc} ->
            NewDoc = Doc#doc{deleted = true},
            case insert_doc(DbName, NewDoc) of
                {ok, _Rev} ->
                  ok;
                Ierror ->
                  Ierror
            end;
        {error, {not_found, Type}} ->
          {error, Type};
        Err ->
          Err
    end.


%% delete_docs/2
%% ====================================================================
%% @doc Deletes list of docs from db
-spec delete_docs(DbName :: string(), [DocID :: string()]) -> [ok | {error, term()}].
%% ====================================================================
delete_docs(DbName, DocIDs) ->
    [delete_doc(DbName, X) || X <- DocIDs].


%% ===================================================================
%% Views Management
%% ===================================================================

%% open_design_doc/2
%% ====================================================================
%% @doc Returns design document with a given design doc name
-spec open_design_doc(DbName :: string(), DesignName :: string()) -> {ok, #doc{}} | {error, {not_found, missing | deleted}} | {error, term()}.
%% ====================================================================
open_design_doc(DbName, DesignName) ->
    open_doc(DbName, ?DESIGN_DOC_PREFIX ++ DesignName).

%% create_view/6
%% ====================================================================
%% @doc Creates view with given Map and Reduce function. When Reduce = "", reduce function won't be created
-spec create_view(DbName :: string(), DesignName :: string(), ViewName :: string(), Map :: string(), Reduce :: string(), DesignVersion :: integer()) ->
    [ok | {error, term()}].
%% ====================================================================
create_view(DbName, Doc = #doc{}, ViewName, Map, Reduce, DesignVersion) ->
    {MapRd, MapRdValue} =
        case Reduce of
            "" -> {["map"], [dao_json:mk_str(Map)]};
            _ -> {["map", "reduce"], [dao_json:mk_str(Map), dao_json:mk_str(Reduce)]}
        end,
    Doc1 = dao_json:mk_field(Doc, "language", dao_json:mk_str("javascript")),
    DocV = dao_json:mk_field(Doc1, "version", DesignVersion),
    Views =
        case dao_json:get_field(DocV, "views") of
            {error, not_found} -> dao_json:mk_obj();
            Other -> Other
        end,
    VField = dao_json:mk_field(Views, ViewName, dao_json:mk_fields(dao_json:mk_obj(), MapRd, MapRdValue)),
    NewDoc = dao_json:mk_field(DocV, "views", VField),
    case insert_doc(DbName, NewDoc, [?ADMIN_USER_CTX]) of
        ok -> ok;
        {ok, _} -> ok;
        Other1 -> Other1
    end;
create_view(DbName, DesignName, ViewName, Map, Reduce, DesignVersion) ->
    DsgName = ?DESIGN_DOC_PREFIX ++ DesignName,
    case open_doc(DbName, DsgName) of
        {error, {not_found, _}} ->
            create_view(DbName, dao_json:mk_doc(DsgName), ViewName, Map, Reduce, DesignVersion);
        {ok, Doc} -> create_view(DbName, Doc, ViewName, Map, Reduce, DesignVersion);
        Other -> Other
    end.


%% query_view/3
%% ====================================================================
%% @doc Execute a given view with default set of arguments.
%%      Check record #view_query_args for details.
%% @end
-spec query_view(DbName :: string(), DesignName :: string(), ViewName :: string()) -> {ok, QueryResult :: term()} | {error, term()}.
%% ====================================================================
query_view(DbName, DesignName, ViewName) ->
    query_view(DbName, DesignName, ViewName, #view_query_args{view_type = map}).

%% query_view/4
%% ====================================================================
%% @doc Execute a given view.
%%      There are many additional query args that can be passed to a view,
%%      see <a href="http://wiki.apache.org/couchdb/HTTP_view_API#Querying_Options">
%%      query args</a> for details.
%% @end
-spec query_view(DbName :: string(), DesignName :: string(), ViewName :: string(), QueryArgs :: #view_query_args{}) ->
    {ok, QueryResult :: term()} | {error, term()}.
%% ====================================================================
query_view(DbName, DesignName, ViewName, QueryArgs = #view_query_args{view_type = Type}) ->
    QueryArgs1 =
        case Type of
            map -> QueryArgs;
            reduce -> QueryArgs;
            _ -> QueryArgs#view_query_args{view_type = map}
        end,
    normalize_return_term(call(query_view, [name(DbName), name(DesignName), name(ViewName), QueryArgs1])).


%% name/1
%% ====================================================================
%% @doc Converts string/atom to binary
-spec name(Name :: string() | atom()) -> binary().
%% ====================================================================
name(Name) when is_atom(Name) ->
    name(atom_to_list(Name));
name(Name) when is_list(Name) ->
    unicode:characters_to_binary(Name);
name(Name) when is_binary(Name) ->
    Name.

%% revision/1
%% ====================================================================
%% @doc Normalize revision info
-spec revision(RevInfo :: term()) -> term().
%% ====================================================================
revision(RevInfo) when is_binary(RevInfo) ->
    [Num, Rev] = string:tokens(binary_to_list(RevInfo), [$-]),
    Bin = binary:encode_unsigned(list_to_integer(Rev, 16)),
    BinSize = size(Bin),
    {list_to_integer(Num), [<<0:((16-BinSize)*8), Bin/binary>>]};
revision({Num, [Rev | _Old]}) ->
    {Num, [Rev]}.


%% gen_uuid/0
%% ====================================================================
%% @doc Generates UUID with CouchDBs 'utc_random' algorithm
-spec gen_uuid() -> string().
%% ====================================================================
gen_uuid() ->
    {M, S, N} = now(),
    Time = M * 1000000000000 + S * 1000000 + N,
    TimeHex = string:right(integer_to_list(Time, 16), 14, $0),
    ?SEED,
    Rand = [lists:nth(1, integer_to_list(?RND(16)-1, 16)) || _<- lists:seq(1, 18)],
    string:to_lower(string:concat(TimeHex, Rand)).


%% ===================================================================
%% Internal functions
%% ===================================================================

%% normalize_return_term/1
%% ====================================================================
%% @doc Normalizes BigCouch response Term to more readable format
-spec normalize_return_term(Term :: term()) -> OK_Result | ErrorResult when
    OK_Result :: ok | {ok, Response},
    ErrorResult :: {error, Error} | {error, {exit, Error}} | {error, {exit_error, Error}},
    Response :: term(),
    Error :: term().
%% ====================================================================
normalize_return_term(Term) ->
    case Term of
        ok -> ok;
        accepted -> ok;
        {ok, [{error, Error4} | _]} -> {error, Error4};
        {ok, Response} -> {ok, Response};
        {accepted, Response} -> {ok, Response};
        {_, {'EXIT', {Error2, _}}} -> {error, {exit_error, Error2}};
        {_, {'EXIT', Error1}} -> {error, {exit, Error1}};
        {error, Error3} -> {error, Error3};
        Other -> {error, Other}
    end.
