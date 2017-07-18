%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for mapping onedata users to storage users.
%%% @end
%%%-------------------------------------------------------------------
-module(reverse_luma).
-author("Jakub Kudzia").

-behaviour(model_behaviour).
-behaviour(luma_cache_behaviour).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

-type model() :: #reverse_luma{}.

-export_type([model/0]).

%% API
-export([get_user_id/3, get_user_id/4, invalidate_cache/0]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4, list/0]).

%% luma_cache callbacks
-export([last_timestamp/1, get_value/1, new/2]).

-define(KEY_SEPARATOR, <<"::">>).
-define(DEFAULT_CACHE_TIMEOUT, timer:minutes(5)).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns od_user:id() for storage user associated with given Uid, Gid.
%% which is appropriate for the local server operations.
%% Ids are cached for timeout defined in #luma_config{} record.
%% If reverse LUMA is disabled, function returns ?ROOT USER_ID.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(binary(), binary(), storage:id(), storage:model()) ->
    {ok, od_user:id()} | {error, Reason :: term()}.
get_user_id(Uid, Gid, StorageId, Storage = #storage{}) ->
    case storage:is_luma_enabled(Storage) of
        false ->
            {ok, ?ROOT_USER_ID};
        true ->
            get_user_id_internal(Uid, Gid, StorageId, Storage)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_user_id(Uid, Gid, StorageId, Storage = #storage{}).
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(binary(), binary(), storage:id() | storage:doc()) ->
    {ok, od_user:id()} | {error, Reason :: term()}.
get_user_id(Uid, Gid, #document{key = StorageId, value = Storage = #storage{}}) ->
    get_user_id(Uid, Gid, StorageId, Storage);
get_user_id(Uid, Gid, StorageId) ->
    {ok, StorageDoc} = storage:get(StorageId),
    get_user_id(Uid, Gid, StorageDoc).

%%-------------------------------------------------------------------
%% @doc
%% Invalidates cached entries
%% @end
%%-------------------------------------------------------------------
-spec invalidate_cache() -> ok.
invalidate_cache() ->
    luma_cache:invalidate(?MODULE).

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
    ?MODEL_CONFIG(reverse_luma_bucket, [], ?LOCAL_ONLY_LEVEL)#model_config{
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
%%% luma_cache callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link luma_cache_behaviour} callback last_timestamp/1.
%% @end
%%--------------------------------------------------------------------
-spec last_timestamp(luma_cache:model()) -> luma_cache:timestamp().
last_timestamp(#reverse_luma{timestamp = Timestamp}) ->
    Timestamp.

%%--------------------------------------------------------------------
%% @doc
%% {@link luma_cache_behaviour} callback get_value/1.
%% @end
%%--------------------------------------------------------------------
-spec get_value(luma_cache:model()) -> luma_cache:value().
get_value(#reverse_luma{user_id = UserId}) ->
    UserId.

%%--------------------------------------------------------------------
%% @doc
%% {@link luma_cache_behaviour} callback new/2.
%% @end
%%--------------------------------------------------------------------
-spec new(luma_cache:value(), luma_cache:timestamp()) -> luma_cache:model().
new(UserId, Timestamp) ->
    #reverse_luma{
        user_id = UserId,
        timestamp = Timestamp
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% internal helper function for get_user_id/3 function
%% @end
%%--------------------------------------------------------------------
-spec get_user_id_internal(binary(), binary(), storage:id(), storage:model()) ->
    {ok, od_user:id()} | {error, Reason :: term()}.
get_user_id_internal(Uid, Gid, StorageId, Storage = #storage{}) ->
    case is_storage_supported(Storage) of
        false ->
            {error, not_supported_storage_type};
        true ->
            get_user_id_from_supported_storage_credentials(Uid, Gid, StorageId, Storage)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Maps user credentials on supported storage to onedata user id.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id_from_supported_storage_credentials(binary(), binary(),
    storage:id(), storage:model()) -> {ok, od_user:id()}.
get_user_id_from_supported_storage_credentials(Uid, Gid, StorageId,
    #storage{
        name = StorageName,
        helpers = [#helper{name = HelperName} | _],
        luma_config = LumaConfig = #luma_config{
            cache_timeout = CacheTimeout
}}) ->

    Key = to_key(StorageId, Uid, Gid),
    luma_cache:get(?MODULE, Key,
        fun reverse_luma_proxy:get_user_id/5,
        [Uid, Gid, StorageName, HelperName, LumaConfig],
        CacheTimeout
    ).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given storage is supported.
%% @end
%%-------------------------------------------------------------------
-spec is_storage_supported(storage:model()) -> boolean().
is_storage_supported(#storage{
    helpers = [#helper{name = HelperName} | _]
}) ->
    lists:member(HelperName, supported_storages()).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% List of storages supported by reverse luma
%% @end
%%-------------------------------------------------------------------
-spec supported_storages() -> [helper:name()].
supported_storages() -> [
    ?POSIX_HELPER_NAME
].

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Concatenates helper name uid, and guid to create unique key for given user
%% @end
%%-------------------------------------------------------------------
-spec to_key(storage:id(), binary(), binary()) -> binary().
to_key(StorageId, Uid, Gid) ->
    Args = [StorageId, Uid, Gid],
    Binaries = [str_utils:to_binary(E) || E <- Args],
    str_utils:join_binary(Binaries, ?KEY_SEPARATOR).