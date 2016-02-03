%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Cache that maps onedata user to Amazon S3 user.
%%% @end
%%%-------------------------------------------------------------------
-module(s3_user).
-author("Krzysztof Trzepla").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

%% API
-export([add/4, access_key/1, secret_key/1]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

-record(s3_user_credentials, {
    access_key :: access_key(),
    secret_key :: secret_key()
}).

-type access_key() :: binary().
-type secret_key() :: binary().
-type credentials() :: #s3_user_credentials{}.

-export_type([access_key/0, secret_key/0, credentials/0]).

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
    ?MODEL_CONFIG(s3_user_bucket, [], ?GLOBALLY_CACHED_LEVEL).

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
%% Adds Amazon S3 storage credentials for onedata user.
%% @end
%%--------------------------------------------------------------------
-spec add(UserId :: onedata_user:id(), StorageId :: storage:id(), AccessKey :: access_key(),
    SecretKey :: secret_key()) -> {ok, UserId :: onedata_user:id()} | {error, Reason :: term()}.
add(UserId, StorageId, AccessKey, SecretKey) ->
    case s3_user:get(UserId) of
        {ok, #document{value = S3User} = Doc} ->
            NewS3User = add_credentials(S3User, StorageId, AccessKey, SecretKey),
            s3_user:save(Doc#document{value = NewS3User});
        {error, {not_found, _}} ->
            S3User = new(UserId, StorageId, AccessKey, SecretKey),
            case create(S3User) of
                {ok, S3UserId} -> {ok, S3UserId};
                {error, already_exists} ->
                    add(UserId, StorageId, AccessKey, SecretKey);
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns Amazon S3 user access key.
%% @end
%%--------------------------------------------------------------------
-spec access_key(Credentials :: #s3_user_credentials{}) -> AccessKey :: access_key().
access_key(#s3_user_credentials{access_key = AccessKey}) ->
    AccessKey.

%%--------------------------------------------------------------------
%% @doc
%% Returns S3 user secret key.
%% @end
%%--------------------------------------------------------------------
-spec secret_key(Credentials :: #s3_user_credentials{}) -> SecretKey :: secret_key().
secret_key(#s3_user_credentials{secret_key = SecretKey}) ->
    SecretKey.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns Amazon S3 user datastore document.
%% @end
%%--------------------------------------------------------------------
-spec new(UserId :: onedata_user:id(), StorageId :: storage:id(),
    AccessKey :: access_key(), SecretKey :: secret_key()) -> Doc :: #document{}.
new(UserId, StorageId, AccessKey, SecretKey) ->
    #document{key = UserId, value = #s3_user{
        credentials = maps:put(StorageId, #s3_user_credentials{
            access_key = AccessKey,
            secret_key = SecretKey
        }, #{})}
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds credentials to existing Amazon S3 user.
%% @end
%%--------------------------------------------------------------------
-spec add_credentials(S3User :: #s3_user{}, StorageId :: storage:id(),
    AccessKey :: access_key(), SecretKey :: secret_key()) -> NewS3User :: #s3_user{}.
add_credentials(#s3_user{credentials = Credentials} = S3User, StorageId, AccessKey, SecretKey) ->
    S3User#s3_user{credentials = maps:put(StorageId, #s3_user_credentials{
        access_key = AccessKey,
        secret_key = SecretKey
    }, Credentials)}.