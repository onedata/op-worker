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

-behaviour(luma_cache_behaviour).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("modules/storage_file_manager/helpers/helpers.hrl").

%% API
-export([get_user_id/3, get_user_id/4, invalidate_cache/0]).
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,list/0]).

%% datastore_model callbacks
-export([get_ctx/0]).

%% luma_cache callbacks
-export([last_timestamp/1, get_value/1, new/2]).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type record() :: #permissions_cache_helper{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-export_type([record/0]).

-define(KEY_SEPARATOR, <<"::">>).
-define(DEFAULT_CACHE_TIMEOUT, timer:minutes(5)).

-define(CTX, #{
    model => ?MODULE,
    routing => local,
    disc_driver => undefined,
    fold_enabled => true
}).

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

%%--------------------------------------------------------------------
%% @doc
%% Saves permission cache.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, key()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates permission cache.
%% @end
%%--------------------------------------------------------------------
-spec update(key(), diff()) -> {ok, key()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Creates permission cache.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, key()} | {error, term()}.
create(Doc) ->
    ?extract_key(datastore_model:create(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Returns permission cache.
%% @end
%%--------------------------------------------------------------------
-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes permission cache.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether permission cache exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(key()) -> boolean().
exists(Key) ->
    {ok, Exists} = datastore_model:exists(?CTX, Key),
    Exists.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [key()]} | {error, term()}.
list() ->
    datastore_model:fold(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.

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