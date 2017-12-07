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

-behaviour(model_behaviour).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").


-type key() :: binary().
-type value() :: od_user:id() | od_group:id()| luma:user_ctx().
-type timestamp() :: non_neg_integer().

-export_type([value/0, timestamp/0]).

%% API
-export([get/4, invalidate/0]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4, list/0]).


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
    CurrentTimestamp = time_utils:system_time_milli_seconds(),
    case get(Key) of
        {error, {not_found, _}} ->
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

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(Document) ->
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    model:execute_with_default_context(?MODULE, create, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(luma_cache_bucket, [], ?LOCAL_ONLY_LEVEL)#model_config{
        list_enabled = {true, return_errors}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    model:execute_with_default_context(?MODULE, list, [?GET_ALL, []]).

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
