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
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

%% API
-export([add/4, uid/1, gid/1]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

-record(posix_user_credentials, {
    uid :: uid(),
    gid :: gid()
}).

-type uid() :: non_neg_integer().
-type gid() :: non_neg_integer().
-type credentials() :: #posix_user_credentials{}.

-export_type([uid/0, gid/0, credentials/0]).

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
%% Adds POSIX storage credentials for onedata user.
%% @end
%%--------------------------------------------------------------------
-spec add(UserId :: onedata_user:id(), StorageId :: storage:id(), Uid :: uid(), Gid :: gid())
        -> {ok, UserId :: onedata_user:id()} | {error, Reason :: term()}.
add(UserId, StorageId, Uid, Gid) ->
    case posix_user:get(UserId) of
        {ok, #document{value = POSIXUser} = Doc} ->
            NewPOSIXUser = add_credentials(POSIXUser, StorageId, Uid, Gid),
            posix_user:save(Doc#document{value = NewPOSIXUser});
        {error, {not_found, _}} ->
            POSIXUser = new(UserId, StorageId, Uid, Gid),
            case create(POSIXUser) of
                {ok, POSIXUserId} -> {ok, POSIXUserId};
                {error, already_exists} ->
                    add(UserId, StorageId, Uid, Gid);
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns POSIX user uid.
%% @end
%%--------------------------------------------------------------------
-spec uid(Credentials :: #posix_user_credentials{}) -> Uid :: uid().
uid(#posix_user_credentials{uid = Uid}) ->
    Uid.

%%--------------------------------------------------------------------
%% @doc
%% Returns POSIX user gid.
%% @end
%%--------------------------------------------------------------------
-spec gid(Credentials :: #posix_user_credentials{}) -> Gid :: gid().
gid(#posix_user_credentials{gid = Gid}) ->
    Gid.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns POSIX user datastore document.
%% @end
%%--------------------------------------------------------------------
-spec new(UserId :: onedata_user:id(), StorageId :: storage:id(),
    Uid :: uid(), Gid :: gid()) -> Doc :: #document{}.
new(UserId, StorageId, Uid, Gid) ->
    #document{key = UserId, value = #posix_user{
        credentials = maps:put(StorageId, #posix_user_credentials{
            uid = Uid,
            gid = Gid
        }, #{})}
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds credentials to existing POSIX user.
%% @end
%%--------------------------------------------------------------------
-spec add_credentials(POSIXUser :: #posix_user{}, StorageId :: storage:id(),
    Uid :: uid(), Gid :: gid()) -> NewPOSIXUser :: #posix_user{}.
add_credentials(#posix_user{credentials = Credentials} = POSIXUser, StorageId, Uid, Gid) ->
    POSIXUser#posix_user{credentials = maps:put(StorageId, #posix_user_credentials{
        uid = Uid,
        gid = Gid
    }, Credentials)}.