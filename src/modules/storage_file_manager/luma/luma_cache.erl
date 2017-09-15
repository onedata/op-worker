%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements generic, temporary cache behaviour for luma
%%% and reverse_luma models. Entries are valid only for specific period.
%%% After that period, callback is called to acquire current value.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_cache).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get/4, invalidate/0]).
-export([save/1, update/2, create/1, get/1, delete/1, exists/1, list/0]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type key() :: datastore:key().
-type record() :: #luma_cache{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type value() :: od_user:id() | od_group:id()| luma:user_ctx().
-type timestamp() :: non_neg_integer().

-export_type([value/0, timestamp/0]).

-define(CTX, #{
    model => ?MODULE,
    routing => local,
    disc_driver => undefined,
    fold_enabled => true
}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Returns value saved in given model. If entry was saved longer ago
%% than LumaCacheTimeout then QueryFun will be called and new value
%% will be saved.
%% @end
%%-------------------------------------------------------------------
-spec get(key(), function(), [term()], luma_config:cache_timeout()) ->
    {ok, Value :: value()} | {error, Reason :: term()}.
get(Key, QueryFun, QueryArgs, LumaCacheTimeout) ->
    CurrentTimestamp = utils:system_time_milli_seconds(),
    case datastore_model:get(?CTX, Key) of
        {error, not_found} ->
            query_and_cache_value(Key, QueryFun, QueryArgs,
                CurrentTimestamp);
        {ok, #document{
            value = #luma_cache{
                timestamp = LastTimestamp,
                value = Value
            }}} ->
            case cache_should_be_renewed(CurrentTimestamp, LastTimestamp,
                LumaCacheTimeout)
            of
                false ->
                    {ok, Value};
                true ->
                    query_and_cache_value(Key, QueryFun, QueryArgs,
                        CurrentTimestamp)
            end
    end.

%%-------------------------------------------------------------------
%% @doc
%% Deletes all cached entries.
%% @end
%%-------------------------------------------------------------------
-spec invalidate() -> ok.
invalidate() ->
    {ok, Docs} = list(),
    lists:foreach(fun(#document{key = Key}) ->
        delete(Key)
    end, Docs).

%%--------------------------------------------------------------------
%% @doc
%% Saves luma cache.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, key()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates luma cache.
%% @end
%%--------------------------------------------------------------------
-spec update(key(), diff()) -> {ok, key()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Creates luma cache.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, key()} | {error, term()}.
create(Doc) ->
    ?extract_key(datastore_model:create(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Returns luma cache.
%% @end
%%--------------------------------------------------------------------
-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes luma cache.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether luma cache exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(key()) -> boolean().
exists(Key) ->
    {ok, Exists} = datastore_model:exists(?CTX, Key),
    Exists.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [key()]} | {error, term()}.
list() ->
    datastore_model:fold(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Calls QueryFun and saves acquired value with CurrentTimestamp.
%% @end
%%-------------------------------------------------------------------
-spec query_and_cache_value(key(), function(), [term()], timestamp()) ->
    {ok, Value :: value()} | {error, Reason :: term()}.
query_and_cache_value(Key, QueryFun, QueryArgs, CurrentTimestamp) ->
    try erlang:apply(QueryFun, QueryArgs) of
        {ok, Value} ->
            Doc = #document{
                key = Key,
                value = #luma_cache{
                    value = Value,
                    timestamp = CurrentTimestamp
                }
            },
            {ok, Key} = save(Doc),
            {ok, Value};
        {error, Reason} ->
            {error, Reason}
    catch
        error:{badmatch, Reason} ->
            {error, Reason}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether cache should be renewed.
%% @end
%%-------------------------------------------------------------------
-spec cache_should_be_renewed(timestamp(), timestamp(), luma_config:cache_timeout()) -> boolean().
cache_should_be_renewed(CurrentTimestamp, LastTimestamp, CacheTimeout) ->
    (CurrentTimestamp - LastTimestamp) > CacheTimeout.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.
