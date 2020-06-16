%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements LUMA DB table that associates Onedata user with
%%% credentials on storage, represented by #luma_storege_user record.
%%% Mappings are used to associate onedata users with specific storage users.
%%%
%%% A separate table is created for each storage
%%% so the mappings are actually associated with pair (storage:id(), od_user:id()).
%%%
%%% For more info in luma_storage_user:user() structure please see
%%% luma_storage_user.erl module.
%%%
%%% For more info please read the docs of luma.erl.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_storage_users).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/luma/luma.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    get_or_acquire/2,
    store/3,
    update/3,
    delete/2,
    delete/3,
    clear_all/1,
    store_posix_compatible_mapping/4,
    get_and_describe/2
]).

-type key() :: od_user:id().
-type record() :: luma_storage_user:user().
-type storage() :: storage:id() | storage:data().

-export_type([key/0, record/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec get_or_acquire(storage(), key()) ->
    {ok, record()} | {error, term()}.
get_or_acquire(Storage, UserId) ->
    luma_db:get_or_acquire(Storage, UserId, ?MODULE, fun() ->
        acquire(Storage, UserId)
    end).

-spec store(storage(), luma_onedata_user:user_map(), luma_storage_user:user_map()) ->
    {ok, od_user:id()} | {error, term()}.
store(Storage, UserId, StorageUserMap) when is_binary(UserId) ->
    OnedataUserMap = luma_onedata_user:to_json(luma_onedata_user:new(UserId)),
    store(Storage, OnedataUserMap, StorageUserMap);
store(Storage, OnedataUserMap, StorageUserMap) when is_map(OnedataUserMap) ->
    case luma_sanitizer:sanitize_onedata_user(OnedataUserMap) of
        {ok, OnedataUserMap2} ->
            HelperName = storage:get_helper_name(Storage),
            case luma_sanitizer:sanitize_storage_user(StorageUserMap, HelperName) of
                {ok, StorageUserMap2} ->
                    store_internal(Storage, OnedataUserMap2, StorageUserMap2, ?LOCAL_FEED);
                Error ->
                    Error
            end;
        Error2 ->
            Error2
    end.


-spec update(storage(), od_user:id(), luma_storage_user:user_map()) -> ok | {error, term()}.
update(Storage, UserId, StorageUserMap) ->
    {ok, PrevStorageUser} = case storage:is_posix_compatible(Storage) of
        true ->
            % fetch current version of record, because
            % it is possible that associated uid will be changed
            % so we must be able to delete reverse mapping
            case luma_db:get(Storage, UserId, ?MODULE) of
                {ok, Record} -> {ok, Record};
                {error, not_found} -> {ok, undefined}
            end;
        false ->
            {ok, undefined}
    end,
    case luma_db:update(Storage, UserId, ?MODULE, StorageUserMap) of
        {ok, StorageUser} ->
            maybe_update_reverse_mapping(Storage, PrevStorageUser, StorageUser, UserId, ?LOCAL_FEED);
        Error ->
            Error
    end.


-spec delete(storage:id(), key()) -> ok.
delete(StorageId, UserId) ->
    delete(StorageId, UserId, storage:is_posix_compatible(StorageId)).


-spec delete(storage:id(), key(), DeleteReverseMapping :: boolean()) -> ok.
delete(StorageId, UserId, false) ->
    luma_db:delete(StorageId, UserId, ?MODULE);
delete(StorageId, UserId, true) ->
    Uid = case luma_db:get(StorageId, UserId, ?MODULE) of
        {ok, StorageUser} ->
            StorageCredentials = luma_storage_user:get_storage_credentials(StorageUser),
            binary_to_integer(maps:get(<<"uid">>, StorageCredentials));
        {error, not_found} ->
            undefined
    end,
    case delete(StorageId, UserId, false) of
        ok when Uid =/= undefined ->
            luma_onedata_users:delete_uid_mapping(StorageId, Uid, false);
        ok ->
            ok
    end.


-spec clear_all(storage:id()) -> ok.
clear_all(StorageId) ->
    luma_db:clear_all(StorageId, ?MODULE).

-spec store_posix_compatible_mapping(storage(), od_user:id(), luma:uid(), luma:feed()) ->
    ok | {error, term()}.
store_posix_compatible_mapping(Storage, UserId, Uid, Feed) ->
    StorageUser = luma_storage_user:new_posix_user(Uid),
    luma_db:store(Storage, UserId, ?MODULE, StorageUser, Feed, true).

-spec get_and_describe(storage(), key()) ->
    {ok, luma_storage_user:user_map()} | {error, term()}.
get_and_describe(Storage, UserId) ->
    luma_db:get_and_describe(Storage, UserId, ?MODULE).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec acquire(storage:data(), key()) ->
    {ok, record(), luma:feed()} | {error, term()}.
acquire(Storage, UserId) ->
    Result = case storage:get_luma_feed(Storage) of
        ?AUTO_FEED -> acquire_default_mapping(Storage, UserId);
        ?EXTERNAL_FEED -> acquire_mapping_from_external_feed(Storage, UserId);
        ?LOCAL_FEED -> {error, not_found}
    end,
    case Result of
        {ok, Record, Feed} ->
            maybe_add_reverse_mapping(Storage, Record, UserId, Feed),
            Result;
        Error ->
            Error
    end.


-spec acquire_mapping_from_external_feed(storage:data(), key()) ->
    {ok, record(), luma:feed()} | {error, term()}.
acquire_mapping_from_external_feed(Storage, UserId) ->
    case luma_external_feed:map_onedata_user_to_credentials(UserId, Storage) of
        {ok, StorageUserMap} ->
            {ok, luma_storage_user:new(UserId, StorageUserMap, Storage), ?EXTERNAL_FEED};
        {error, luma_external_feed_error} ->
            {error, not_found};
        OtherError ->
            OtherError
    end.

-spec acquire_default_mapping(storage:data(), key()) ->
    {ok, record(), luma:feed()}.
acquire_default_mapping(Storage, UserId) ->
    {ok, StorageUser} = luma_auto_feed:acquire_user_storage_credentials(Storage, UserId),
    {ok, StorageUser, ?AUTO_FEED}.


-spec store_internal(storage(), luma_onedata_user:user_map(), luma_storage_user:user_map(), luma:feed()) ->
    {ok, od_user:id()} | {error, term()}.
store_internal(Storage, OnedataUserMap, StorageUserMap, Feed) ->
    OnedataUser = luma_onedata_user:new(OnedataUserMap),
    UserId = luma_onedata_user:get_user_id(OnedataUser),
    Record = luma_storage_user:new(UserId, StorageUserMap, Storage),
    case luma_db:store(Storage, UserId, ?MODULE, Record, Feed, false) of
        ok ->
            maybe_add_reverse_mapping(Storage, Record, OnedataUserMap, Feed),
            {ok, UserId};
        {error, _} = Error ->
            Error
    end.

-spec maybe_add_reverse_mapping(storage(), record(), luma_onedata_user:user_map() | od_user:id(), luma:feed()) ->
    ok | {error, term()}.
maybe_add_reverse_mapping(Storage, StorageUser, OnedataUserMap, Feed) ->
    case storage:is_posix_compatible(Storage) of
        true ->
            add_reverse_mapping(Storage, StorageUser, OnedataUserMap, Feed);
        false ->
            ok
    end.

-spec maybe_update_reverse_mapping(storage(), record(), record(), od_user:id(), luma:feed()) -> ok | {error, term()}.
maybe_update_reverse_mapping(Storage, PrevStorageUser, StorageUser, UserId, Feed) ->
    case storage:is_posix_compatible(Storage) of
        true ->
            PrevStorageCredentials = luma_storage_user:get_storage_credentials(PrevStorageUser),
            StorageCredentials = luma_storage_user:get_storage_credentials(StorageUser),
            case PrevStorageCredentials =:= StorageCredentials of
                true ->
                    ok;
                false ->
                    delete_reverse_mapping(Storage, binary_to_integer(maps:get(<<"uid">>, PrevStorageCredentials))),
                    add_reverse_mapping(Storage, StorageUser, UserId, Feed)
            end;
        false ->
            ok
    end.

-spec add_reverse_mapping(storage(), record(), luma_onedata_user:user_map() | od_user:id(), luma:feed()) ->
    ok | {error, term()}.
add_reverse_mapping(Storage, Record, OnedataUserMap, Feed) ->
    StorageCredentials = luma_storage_user:get_storage_credentials(Record),
    Uid = binary_to_integer(maps:get(<<"uid">>, StorageCredentials)),
    luma_onedata_users:update_or_store_uid_mapping(Storage, Uid, OnedataUserMap, Feed).

-spec delete_reverse_mapping(storage(), luma:uid()) -> ok | {error, term()}.
delete_reverse_mapping(Storage, Uid) ->
    luma_onedata_users:delete_uid_mapping(storage:get_id(Storage), Uid, false).