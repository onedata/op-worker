%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(luma_db_links).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/datastore_models.hrl").

-define(SEPARATOR, <<"##">>).
-define(FORESTS_PREFIX, <<"LUMA_DB_LINKS">>).
-define(FOREST_KEY(ForestType, StorageId),
    str_utils:join_binary([?FORESTS_PREFIX, ForestType, StorageId], ?SEPARATOR)).

%% API
-export([add_link/4, delete_link/3, list/4]).

-define(CTX, (luma_db:get_ctx())).

-type forest_type() :: luma_db:table().
-type forest_key() :: binary().
-type key() :: luma_db:db_key().
-type doc_id() :: luma_db:doc_id().
-type limit() :: non_neg_integer() | all.
-type token() :: datastore_links_iter:token() | undefined.
-type storage() :: storage:id() | storage:data().

-export_type([token/0, limit/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec add_link(forest_type(), storage:id(), key(), doc_id()) -> ok.
add_link(ForestType, StorageId, Key, DocId) ->
    TreeId = oneprovider:get_id(),
    ForestKey = forest_key(ForestType, StorageId),
    case ?extract_ok(datastore_model:add_links(?CTX, ForestKey, TreeId, {Key, DocId})) of
        ok -> ok;
        {error, already_exists} -> ok
    end.

-spec delete_link(forest_type(), storage:id(), key()) -> ok.
delete_link(ForestType, StorageId, Key) ->
    TreeId = oneprovider:get_id(),
    ForestKey = forest_key(ForestType, StorageId),
    case datastore_model:delete_links(?CTX, ForestKey, TreeId, Key) of
        ok -> ok;
        {error, not_found} -> ok
    end.


-spec list(forest_type(), storage:id(), undefined | token(), limit) ->
    {{ok, [{key(), doc_id()}]}, token()} | {error, term()}.
list(ForestType, StorageId, Token, Limit) ->
    Token2 = utils:ensure_defined(Token, undefined, #link_token{}),
    Opts = #{token => Token2},
    Opts2 = case Limit of
        all -> Opts;
        _ -> Opts#{size => Limit}
    end,
    ListFun = fun(Key, DocId, Acc) -> [{Key, DocId} | Acc] end,
    case for_each_link(ForestType, StorageId, ListFun, [], Opts2) of
        {{ok, KeysAndDocsIdsReversed}, NewToken} ->
            {{ok, lists:reverse(KeysAndDocsIdsReversed)}, NewToken};
        Error = {error, _} ->
            Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec forest_key(forest_type(), storage()) -> forest_key().
forest_key(ForestType, Storage) ->
    ?FOREST_KEY(atom_to_binary(ForestType, utf8), storage:get_id(Storage)).

-spec for_each_link(
    forest_type(), storage:id(),
    Callback :: fun((key(), doc_id(), AccIn :: term()) -> Acc :: term()),
    Acc0 :: term(), datastore_model:fold_opts()) ->
    {ok, Acc :: term()} | {error, term()}.
for_each_link(ForestType, StorageId, Callback, Acc0, Options) ->
    TreeId = oneprovider:get_id(),
    ForestKey = forest_key(ForestType, StorageId),
    datastore_model:fold_links(?CTX, ForestKey, TreeId, fun
        (#link{name = Name, target = Target}, Acc) ->
            {ok, Callback(Name, Target, Acc)}
    end, Acc0, Options).