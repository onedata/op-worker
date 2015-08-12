%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model that is used to controle memory utilization by local cache.
%%% @end
%%%-------------------------------------------------------------------
-module(local_cache_controller).
-author("Michal Wrzeszcz").
-behaviour(model_behaviour).

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/cache_controller.hrl").
-include("modules/datastore/datastore_model.hrl").
-include("modules/datastore/datastore_engine.hrl").

%% model_behaviour callbacks and API
-export([save/1, get/1, list/0, list/1, exists/1, delete/1, delete/2, update/2, create/1, model_init/0,
    'after'/5, before/4, list_docs_to_be_dumped/0]).

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1. 
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    datastore:save(local_only, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2. 
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(local_only, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1. 
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    datastore:create(local_only, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(local_only, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    datastore:list(local_only, ?MODEL_NAME, ?GET_ALL, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of records older then DocAge (in ms).
%% @end
%%--------------------------------------------------------------------
-spec list(DocAge :: integer()) -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
?LIST_OLDER(local_cache_controller, local_only, ?MODEL_NAME).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of records not persisted.
%% @end
%%--------------------------------------------------------------------
-spec list_docs_to_be_dumped() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
?LIST_DOCS_TO_BE_DUMPED(local_cache_controller, local_only, ?MODEL_NAME).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(local_only, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key(), datastore:delete_predicate()) -> ok | datastore:generic_error().
delete(Key, Pred) ->
    datastore:delete(local_only, ?MODULE, Key, Pred).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1. 
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(local_only, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0. 
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(local_cc_bucket, get_hooks_config(),
        ?DEFAULT_STORE_LEVEL, ?DEFAULT_STORE_LEVEL, false).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5. 
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok | datastore:generic_error().
?AFTER(local_only).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) ->
    ok | {ok, save, [datastore:document()]} | datastore:generic_error().
?BEFORE.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Provides hooks configuration.
%% @end
%%--------------------------------------------------------------------
-spec get_hooks_config() -> list().
get_hooks_config() ->
    caches_controller:get_hooks_config(?LOCAL_CACHES).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates information about usage of a document.
%% @end
%%--------------------------------------------------------------------
-spec update_usage_info(Key :: datastore:key(), ModelName :: model_behaviour:model_type()) ->
    {ok, datastore:key()} | datastore:generic_error().
?UPDATE_USAGE_INFO2(local_cache_controller).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates information about usage of a document and saves doc to memory.
%% @end
%%--------------------------------------------------------------------
-spec update_usage_info(Key :: datastore:key(), ModelName :: model_behaviour:model_type(),
    Doc :: datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().
?UPDATE_USAGE_INFO3(local_only).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if get operation should be performed.
%% @end
%%--------------------------------------------------------------------
-spec check_get(Key :: datastore:key(), ModelName :: model_behaviour:model_type()) ->
    ok | {error, {not_found, model_behaviour:model_type()}}.
?CHECK_GET2(local_cache_controller).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if get link operation should be performed.
%% @end
%%--------------------------------------------------------------------
-spec check_get(Key :: datastore:key(), ModelName :: model_behaviour:model_type(),
    LinkName :: datastore:link_name()) -> ok | {error, link_not_found}.
?CHECK_GET3(local_cache_controller).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Delates info about dumping of cache to disk.
%% @end
%%--------------------------------------------------------------------
-spec delete_dump_info(Uuid :: binary()) ->
    ok | datastore:generic_error().
?DELETE_DUMP_INFO(local_cache_controller).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves dump information after disk operation.
%% @end
%%--------------------------------------------------------------------
-spec end_disk_op(Key :: datastore:key(), ModelName :: model_behaviour:model_type(),
    Op :: atom()) -> ok | {error, ending_disk_op_failed}.
?END_DISK_OP(local_cache_controller).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves dump information about disk operation and decides if it should be done.
%% @end
%%--------------------------------------------------------------------
-spec start_disk_op(Key :: datastore:key(), ModelName :: model_behaviour:model_type(),
    Op :: atom()) -> ok | {ok, save, [SavedValue]} | {error, Error} when
    SavedValue :: datastore:document(),
    Error :: not_last_user | preparing_disk_op_failed.
?START_DISK_OP(local_cache_controller, local_only).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves info about links deleting.
%% @end
%%--------------------------------------------------------------------
-spec log_link_del(Key :: datastore:key(), ModelName :: model_behaviour:model_type(),
    LinkNames :: list() | all, Phase :: start | stop) ->
    ok | {error, preparing_disk_op_failed} | {error, ending_disk_op_failed}.
?LOG_LINK_DEL(local_cache_controller).
