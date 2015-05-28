%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model that is used to controle memory utilization by global cache.
%%% @end
%%%-------------------------------------------------------------------
-module(global_cache_controller).
-author("Michal Wrzeszcz").
-behaviour(model_behaviour).

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_internal.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4, get_cache_uuid/2]).

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
    datastore:save(global_only, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2. 
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(global_only, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1. 
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    datastore:create(global_only, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(global_only, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    datastore:list(global_only, ?MODEL_NAME, ?GET_ALL, []).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(global_only, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1. 
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(global_only, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0. 
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(global_cc_bucket, get_hooks_config()).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5. 
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(ModelName, save, _Level, [Doc], _ReturnValue) ->
    update_usage_info(Doc#document.key, ModelName);
'after'(ModelName, update, _Level, [Key, _Diff], _ReturnValue) ->
    update_usage_info(Key, ModelName);
'after'(ModelName, create, _Level, [Doc], _ReturnValue) ->
    update_usage_info(Doc#document.key, ModelName);
'after'(ModelName, get, _Level, [Key], _ReturnValue) ->
    update_usage_info(Key, ModelName);
'after'(ModelName, delete, _Level, [Key, _Pred], ok) ->
    del_usage_info(Key, ModelName);
'after'(ModelName, exists, _Level, [Key], {ok, true}) ->
    update_usage_info(Key, ModelName);
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4. 
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) ->
    ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

get_hooks_config() ->
    caches_controller:get_hooks_config(?GLOBAL_CACHES).

update_usage_info(Key, ModelName) ->
    Uuid = get_cache_uuid(Key, ModelName),
    V = #global_cache_controller{timestamp = os:timestamp()},
    Doc = #document{key = Uuid, value = V},
    save(Doc).

del_usage_info(Key, ModelName) ->
    Uuid = get_cache_uuid(Key, ModelName),
    delete(Uuid).

get_cache_uuid(Key, ModelName) ->
    base64:encode(term_to_binary({ModelName, Key})).