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

-include_lib("veil_modules/dao/couch_db.hrl").

-ifdef(TEST).
-compile([export_all]).
-endif.

-import(dao_hosts, [call/2]).

%% API
-export([list_dbs/0, list_dbs/1, get_db_info/1, get_doc_count/1, create_db/1, create_db/2]).
-export([delete_db/1, delete_db/2]).


%% % DBs
%% -export([all_dbs/0, all_dbs/1, create_db/1, create_db/2, delete_db/1,
%%     delete_db/2, get_db_info/1, get_doc_count/1, set_revs_limit/3,
%%     set_security/3, get_revs_limit/1, get_security/1, get_security/2]).
%%
%% % Documents
%% -export([open_doc/3, open_revs/4, get_missing_revs/2, get_missing_revs/3,
%%     update_doc/3, update_docs/3, purge_docs/2, att_receiver/2]).
%%
%% % Views
%% -export([all_docs/4, changes/4, query_view/3, query_view/4, query_view/6,
%%     get_view_group_info/2]).
%%
%% % miscellany
%% -export([design_docs/1, reset_validation_funs/1, cleanup_index_files/0,
%%     cleanup_index_files/1]).

%% ===================================================================
%% API functions
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
    case call(all_dbs, [name(Prefix)]) of
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
    case call(get_doc_count, [DbName]) of
        {ok, Count}=Ret when is_integer(Count) ->
            Ret;
        {_, {'EXIT', {database_does_not_exist, _}}}->
            {error, database_does_not_exist};
        {error, Error} -> {error, Error};
        Other -> {error, Other}
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
    case call(get_db_info, [name(DbName)]) of
        {ok, Info}=Ret when is_list(Info) ->
            Ret;
        {_, {'EXIT', {database_does_not_exist, _}}}->
            {error, database_does_not_exist};
        {error, Error} -> {error, Error};
        Other -> {error, Other}
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
    call(create_db, [name(DbName), Opts]).

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
    case call(delete_db, [name(DbName), Opts]) of
        ok -> ok;
        {_, {'EXIT', {database_does_not_exist, _}}}->
            {error, database_does_not_exist};
        {error, Error} -> {error, Error};
        Other -> {error, Other}
    end.


%% ===================================================================
%% Internal functions
%% ===================================================================

name(Name) when is_list(Name) ->
    ?l2b(Name);
name(Name) when is_binary(Name) ->
    Name.
