%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Cache that maps onedata user to POSIX user.
%%% @end
%%%-------------------------------------------------------------------
-module(posix_user).
-author("Michal Wrona").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/helpers.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

%% API
-export([new_ctx/2, new/3, add_ctx/3, add/3, get_all_ctx/1, get_ctx/2]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

-type uid() :: non_neg_integer().
-type gid() :: non_neg_integer().
-type ctx() :: #posix_user_ctx{}.
-type type() :: #posix_user{}.

-export_type([uid/0, gid/0, ctx/0, type/0]).

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
    ?MODEL_CONFIG(posix_user_bucket, [], ?GLOBALLY_CACHED_LEVEL).

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
%% Creates POSIX user context.
%% @end
%%--------------------------------------------------------------------
-spec new_ctx(Uid :: uid(), Gid :: gid()) -> UserCtx :: ctx().
new_ctx(Uid, Gid) ->
    #posix_user_ctx{uid = Uid, gid = Gid}.

%%--------------------------------------------------------------------
%% @doc
%% Creates POSIX user document.
%% @end
%%--------------------------------------------------------------------
-spec new(UserId :: onedata_user:id(), StorageId :: storage:id(), UserCtx :: ctx()) ->
    UserDoc :: datastore:document().
new(UserId, StorageId, #posix_user_ctx{} = UserCtx) ->
    #document{key = UserId, value = add_ctx(StorageId, UserCtx, #posix_user{})}.

%%--------------------------------------------------------------------
%% @doc
%% Adds POSIX storage context to onedata user.
%% @end
%%--------------------------------------------------------------------
-spec add_ctx(StorageId :: storage:id(), UserCtx :: ctx(), User :: #posix_user{}) ->
    User :: #posix_user{}.
add_ctx(StorageId, UserCtx, #posix_user{ctx = Ctx} = User) ->
    User#posix_user{ctx = maps:put(StorageId, UserCtx, Ctx)}.

%%--------------------------------------------------------------------
%% @doc
%% Returns all POSIX storage contexts for onedata user.
%% @end
%%--------------------------------------------------------------------
-spec get_all_ctx(User :: #posix_user{}) -> Ctx :: #{storage:id() => ctx()}.
get_all_ctx(#posix_user{ctx = Ctx}) ->
    Ctx.

%%--------------------------------------------------------------------
%% @doc
%% @equiv helpers_user:get_ctx(?MODULE, UserId, StorageId)
%% @end
%%--------------------------------------------------------------------
-spec get_ctx(UserId :: onedata_user:id(), StorageId :: storage:id()) ->
    UserCtx :: ctx() | undefined.
get_ctx(UserId, StorageId) ->
    helpers_user:get_ctx(?MODULE, UserId, StorageId).

%%--------------------------------------------------------------------
%% @doc
%% @equiv helpers_user:add(?MODULE, UserId, StorageId, UserCtx)
%% @end
%%--------------------------------------------------------------------
-spec add(UserId :: onedata_user:id(), StorageId :: storage:id(), UserCtx :: ctx()) ->
    {ok, UserId :: onedata_user:id()} | {error, Reason :: term()}.
add(UserId, StorageId, UserCtx) ->
    helpers_user:add(?MODULE, UserId, StorageId, UserCtx).