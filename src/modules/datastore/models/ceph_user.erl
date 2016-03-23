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
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

%% API
-export([add/4, name/1, key/1]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

-record(ceph_user_credentials, {
    user_name :: name(),
    user_key :: key()
}).

-type name() :: binary().
-type key() :: binary().
-type credentials() :: #ceph_user_credentials{}.

-export_type([name/0, key/0, credentials/0]).

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
%% Adds Ceph storage credentials for onedata user.
%% @end
%%--------------------------------------------------------------------
-spec add(UserId :: onedata_user:id(), StorageId :: storage:id(), UserName :: name(),
    UserKey :: key()) -> {ok, UserId :: onedata_user:id()} | {error, Reason :: term()}.
add(UserId, StorageId, UserName, UserKey) ->
    case ceph_user:get(UserId) of
        {ok, #document{value = CephUser} = Doc} ->
            NewCephUser = add_credentials(CephUser, StorageId, UserName, UserKey),
            ceph_user:save(Doc#document{value = NewCephUser});
        {error, {not_found, _}} ->
            CephUser = new(UserId, StorageId, UserName, UserKey),
            case create(CephUser) of
                {ok, CephUserId} -> {ok, CephUserId};
                {error, already_exists} ->
                    add(UserId, StorageId, UserName, UserKey);
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
%%--------------------------------------------------------------------
%% @doc
%% Returns Ceph user name.
%% @end
%%--------------------------------------------------------------------
-spec name(Credentials :: #ceph_user_credentials{}) -> UserName :: name().
name(#ceph_user_credentials{user_name = UserName}) ->
    UserName.

%%--------------------------------------------------------------------
%% @doc
%% Returns Ceph user key.
%% @end
%%--------------------------------------------------------------------
-spec key(Credentials :: #ceph_user_credentials{}) -> UserKey :: key().
key(#ceph_user_credentials{user_key = UserKey}) ->
    UserKey.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns Ceph user datastore document.
%% @end
%%--------------------------------------------------------------------
-spec new(UserId :: onedata_user:id(), StorageId :: storage:id(),
    UserName :: name(), UserKey :: key()) -> Doc :: #document{}.
new(UserId, StorageId, UserName, UserKey) ->
    #document{key = UserId, value = #ceph_user{
        credentials = maps:put(StorageId, #ceph_user_credentials{
            user_name = UserName,
            user_key = UserKey
        }, #{})}
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds credentials to existing Ceph user.
%% @end
%%--------------------------------------------------------------------
-spec add_credentials(CephUser :: #ceph_user{}, StorageId :: storage:id(),
    UserName :: name(), UserKey :: key()) -> NewCephUser :: #ceph_user{}.
add_credentials(#ceph_user{credentials = Credentials} = CephUser, StorageId, UserName, UserKey) ->
    CephUser#ceph_user{credentials = maps:put(StorageId, #ceph_user_credentials{
        user_name = UserName,
        user_key = UserKey
    }, Credentials)}.
