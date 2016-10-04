%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Cache that maps onedata user to Ceph user.
%%% @end
%%%-------------------------------------------------------------------
-module(ceph_user).
-author("Krzysztof Trzepla").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/helpers.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

%% API
-export([new_ctx/2, new/3, add_ctx/3, add/3, get_all_ctx/1, get_ctx/2]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

-type name() :: binary().
-type key() :: binary().
-type ctx() :: #ceph_user_ctx{}.
-type type() :: #ceph_user{}.

-export_type([name/0, key/0, ctx/0, type/0]).

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:ext_key()} | datastore:generic_error().
save(Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(ceph_user_bucket, [], ?GLOBALLY_CACHED_LEVEL).

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

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates Ceph user context.
%% @end
%%--------------------------------------------------------------------
-spec new_ctx(Name :: name(), Key :: key()) -> UserCtx :: ctx().
new_ctx(Name, Key) ->
    #ceph_user_ctx{user_name = Name, user_key = Key}.

%%--------------------------------------------------------------------
%% @doc
%% Creates Ceph user document.
%% @end
%%--------------------------------------------------------------------
-spec new(UserId :: od_user:id(), StorageId :: storage:id(), UserCtx :: ctx()) ->
    UserDoc :: datastore:document().
new(UserId, StorageId, #ceph_user_ctx{} = UserCtx) ->
    #document{key = UserId, value = add_ctx(StorageId, UserCtx, #ceph_user{})}.

%%--------------------------------------------------------------------
%% @doc
%% Adds Ceph storage ctx to onedata user.
%% @end
%%--------------------------------------------------------------------
-spec add_ctx(StorageId :: storage:id(), UserCtx :: ctx(), User :: #ceph_user{}) ->
    User :: #ceph_user{}.
add_ctx(StorageId, UserCtx, #ceph_user{ctx = Ctx} = User) ->
    User#ceph_user{ctx = maps:put(StorageId, UserCtx, Ctx)}.

%%--------------------------------------------------------------------
%% @doc
%% Returns all Ceph storage contexts for onedata user.
%% @end
%%--------------------------------------------------------------------
-spec get_all_ctx(User :: #ceph_user{}) -> Ctx :: #{storage:id() => ctx()}.
get_all_ctx(#ceph_user{ctx = Ctx}) ->
    Ctx.

%%--------------------------------------------------------------------
%% @doc
%% @equiv helpers_user:get_ctx(?MODULE, UserId, StorageId)
%% @end
%%--------------------------------------------------------------------
-spec get_ctx(UserId :: od_user:id(), StorageId :: storage:id()) ->
    UserCtx :: ctx() | undefined.
get_ctx(UserId, StorageId) ->
    helpers_user:get_ctx(?MODULE, UserId, StorageId).

%%--------------------------------------------------------------------
%% @doc
%% @equiv helpers_user:add(?MODULE, UserId, StorageId, UserCtx)
%% @end
%%--------------------------------------------------------------------
-spec add(UserId :: od_user:id(), StorageId :: storage:id(), UserCtx :: ctx()) ->
    {ok, UserId :: od_user:id()} | {error, Reason :: term()}.
add(UserId, StorageId, UserCtx) ->
    helpers_user:add(?MODULE, UserId, StorageId, UserCtx).