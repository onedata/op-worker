%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Cache that maps onedata user to Openstack Swift user.
%%% @end
%%%-------------------------------------------------------------------
-module(swift_user).
-author("Michal Wrona").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

%% API
-export([add/4, user_name/1, password/1]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

-record(swift_user_credentials, {
    user_name :: user_name(),
    password :: password()
}).

-type user_name() :: binary().
-type password() :: binary().
-type credentials() :: #swift_user_credentials{}.

-export_type([user_name/0, password/0, credentials/0]).

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
    ?MODEL_CONFIG(swift_user_bucket, [], ?GLOBALLY_CACHED_LEVEL).

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
%% Adds Openstack Swift storage credentials for onedata user.
%% @end
%%--------------------------------------------------------------------
-spec add(UserId :: onedata_user:id(), StorageId :: storage:id(), UserName :: user_name(),
    Password :: password()) -> {ok, UserId :: onedata_user:id()} | {error, Reason :: term()}.
add(UserId, StorageId, UserName, Password) ->
    case swift_user:get(UserId) of
        {ok, #document{value = SwiftUser} = Doc} ->
            NewSwiftUser = add_credentials(SwiftUser, StorageId, UserName, Password),
            swift_user:save(Doc#document{value = NewSwiftUser});
        {error, {not_found, _}} ->
            SwiftUser = new(UserId, StorageId, UserName, Password),
            case create(SwiftUser) of
                {ok, SwiftUserId} -> {ok, SwiftUserId};
                {error, already_exists} ->
                    add(UserId, StorageId, UserName, Password);
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns Openstack Swift user name.
%% @end
%%--------------------------------------------------------------------
-spec user_name(Credentials :: #swift_user_credentials{}) -> user_name().
user_name(#swift_user_credentials{user_name = UserName}) ->
    UserName.

%%--------------------------------------------------------------------
%% @doc
%% Returns Openstack Swift user password.
%% @end
%%--------------------------------------------------------------------
-spec password(Credentials :: #swift_user_credentials{}) -> password().
password(#swift_user_credentials{password = Password}) ->
    Password.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns Openstack Swift user datastore document.
%% @end
%%--------------------------------------------------------------------
-spec new(UserId :: onedata_user:id(), StorageId :: storage:id(),
    UserName :: user_name(), Password :: password()) -> Doc :: #document{}.
new(UserId, StorageId, UserName, Password) ->
    #document{key = UserId, value = #swift_user{
        credentials = maps:put(StorageId, #swift_user_credentials{
            user_name = UserName,
            password = Password
        }, #{})}
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds credentials to existing Openstack Swift user.
%% @end
%%--------------------------------------------------------------------
-spec add_credentials(SwiftUser :: #swift_user{}, StorageId :: storage:id(),
    UserName :: user_name(), Password :: password()) -> NewSwiftUser :: #swift_user{}.
add_credentials(#swift_user{credentials = Credentials} = SwiftUser, StorageId, UserName, Password) ->
    SwiftUser#swift_user{credentials = maps:put(StorageId, #swift_user_credentials{
        user_name = UserName,
        password = Password
    }, Credentials)}.