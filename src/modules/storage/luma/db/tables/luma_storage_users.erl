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

-export_type([key/0, record/0]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec get_or_acquire(storage:data(), key()) ->
    {ok, record()} | {error, term()}.
get_or_acquire(StorageData, UserId) ->
    luma_db:get_or_acquire(StorageData, UserId, ?MODULE, fun() ->
        acquire(StorageData, UserId)
    end).


-spec store(storage:data(), luma_onedata_user:user_map(), luma_storage_user:user_map()) ->
    {ok, od_user:id()} | {error, term()}.
store(StorageData, UserId, StorageUserMap) when is_binary(UserId) ->
    OnedataUserMap = luma_onedata_user:to_json(luma_onedata_user:new(UserId)),
    store(StorageData, OnedataUserMap, StorageUserMap);
store(StorageData, OnedataUserMap, StorageUserMap) when is_map(OnedataUserMap) ->
    case luma_sanitizer:sanitize_onedata_user(OnedataUserMap) of
        {ok, OnedataUserMap2} ->
            HelperName = storage:get_helper_name(StorageData),
            case luma_sanitizer:sanitize_storage_user(StorageUserMap, HelperName) of
                {ok, StorageUserMap2} ->
                    store_internal(StorageData, OnedataUserMap2, StorageUserMap2, ?LOCAL_FEED);
                Error ->
                    Error
            end;
        Error2 ->
            Error2
    end.


-spec update(storage:data(), od_user:id(), luma_storage_user:user_map()) -> ok | {error, term()}.
update(StorageData, UserId, StorageUserMap) ->
    PrevStorageUser = case storage:is_posix_compatible(StorageData) of
        true ->
            % fetch current version of record, because
            % it is possible that associated uid will be changed
            % so we must be able to delete reverse mapping
            case luma_db:get(StorageData, UserId, ?MODULE) of
                {ok, Record} -> Record;
                {error, not_found} -> undefined
            end;
        false ->
            undefined
    end,
    case luma_db:update(StorageData, UserId, ?MODULE, StorageUserMap) of
        {ok, StorageUser} ->
            maybe_update_reverse_mapping(StorageData, PrevStorageUser, StorageUser, UserId, ?LOCAL_FEED);
        Error ->
            Error
    end.


-spec delete(storage:data(), key()) -> ok.
delete(StorageData, UserId) ->
    delete(StorageData, UserId, storage:is_posix_compatible(StorageData)).


-spec delete(storage:data(), key(), DeleteReverseMapping :: boolean()) -> ok.
delete(StorageData, UserId, false) ->
    luma_db:delete(StorageData, UserId, ?MODULE);
delete(StorageData, UserId, true) ->
    Uid = case luma_db:get(StorageData, UserId, ?MODULE) of
        {ok, StorageUser} ->
            StorageCredentials = luma_storage_user:get_storage_credentials(StorageUser),
            binary_to_integer(maps:get(<<"uid">>, StorageCredentials));
        {error, not_found} ->
            undefined
    end,
    case delete(StorageData, UserId, false) of
        ok when Uid =/= undefined ->
            delete_reverse_mapping(StorageData, Uid);
        ok ->
            ok
    end.


-spec clear_all(storage:data()) -> ok.
clear_all(StorageData) ->
    luma_db:clear_all(StorageData, ?MODULE).


-spec store_posix_compatible_mapping(storage:data(), od_user:id(), luma:uid(), luma:feed()) ->
    ok | {error, term()}.
store_posix_compatible_mapping(StorageData, UserId, Uid, Feed) ->
    StorageUser = luma_storage_user:new_posix_user(Uid),
    luma_db:store(StorageData, UserId, ?MODULE, StorageUser, Feed).


-spec get_and_describe(storage:data(), key()) ->
    {ok, luma_storage_user:user_map()} | {error, term()}.
get_and_describe(StorageData, UserId) ->
    luma_db:get_and_describe(StorageData, UserId, ?MODULE).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec acquire(storage:data(), key()) ->
    {luma_db:cache_policy(), record(), luma:feed()} | {error, term()}.
acquire(StorageData, UserId) ->
    Result = case storage:get_luma_feed(StorageData) of
        ?AUTO_FEED -> acquire_default_mapping(StorageData, UserId);
        ?EXTERNAL_FEED -> acquire_mapping_from_external_feed(StorageData, UserId);
        ?LOCAL_FEED -> {error, not_found}
    end,
    case Result of
        {error, _} = Error ->
            Error;
        {_, Record, Feed} ->
            maybe_add_reverse_mapping(StorageData, Record, UserId, Feed),
            Result
    end.


%% @private
-spec acquire_mapping_from_external_feed(storage:data(), key()) ->
    {luma_db:cache_policy(), record(), luma:feed()} | {error, term()}.
acquire_mapping_from_external_feed(StorageData, UserId) ->
    case luma_external_feed:map_onedata_user_to_credentials(UserId, StorageData) of
        {ok, StorageUserMap} ->
            {cache, luma_storage_user:new(UserId, StorageUserMap, StorageData), ?EXTERNAL_FEED};
        {error, luma_external_feed_error} ->
            {error, not_found};
        OtherError ->
            OtherError
    end.


%% @private
-spec acquire_default_mapping(storage:data(), key()) ->
    {luma_db:cache_policy(), record(), luma:feed()}.
acquire_default_mapping(Storage, UserId) ->
    {ok, StorageUser} = luma_auto_feed:acquire_user_storage_credentials(Storage, UserId),
    {nocache, StorageUser, ?AUTO_FEED}.


%% @private
-spec store_internal(storage:data(), luma_onedata_user:user_map(), luma_storage_user:user_map(), luma:feed()) ->
    {ok, od_user:id()} | {error, term()}.
store_internal(StorageData, OnedataUserMap, StorageUserMap, Feed) ->
    OnedataUser = luma_onedata_user:new(OnedataUserMap),
    UserId = luma_onedata_user:get_user_id(OnedataUser),
    Record = luma_storage_user:new(UserId, StorageUserMap, StorageData),
    case luma_db:store(StorageData, UserId, ?MODULE, Record, Feed, ?NO_OVERWRITE, []) of
        ok ->
            maybe_add_reverse_mapping(StorageData, Record, OnedataUserMap, Feed),
            {ok, UserId};
        {error, _} = Error ->
            Error
    end.


%% @private
-spec maybe_add_reverse_mapping(storage:data(), record(), luma_onedata_user:user_map() | od_user:id(), luma:feed()) ->
    ok | {error, term()}.
maybe_add_reverse_mapping(StorageData, StorageUser, OnedataUserMap, Feed) ->
    case storage:is_posix_compatible(StorageData) of
        true ->
            add_reverse_mapping(StorageData, StorageUser, OnedataUserMap, Feed);
        false ->
            ok
    end.


%% @private
-spec maybe_update_reverse_mapping(storage:data(), record(), record(), od_user:id(), luma:feed()) -> ok | {error, term()}.
maybe_update_reverse_mapping(StorageData, PrevStorageUser, StorageUser, UserId, Feed) ->
    case storage:is_posix_compatible(StorageData) of
        true ->
            PrevStorageCredentials = luma_storage_user:get_storage_credentials(PrevStorageUser),
            StorageCredentials = luma_storage_user:get_storage_credentials(StorageUser),
            case PrevStorageCredentials =:= StorageCredentials of
                true ->
                    ok;
                false ->
                    delete_reverse_mapping(StorageData, binary_to_integer(maps:get(<<"uid">>, PrevStorageCredentials))),
                    add_reverse_mapping(StorageData, StorageUser, UserId, Feed)
            end;
        false ->
            ok
    end.


%% @private
-spec add_reverse_mapping(storage:data(), record(), luma_onedata_user:user_map() | od_user:id(), luma:feed()) ->
    ok | {error, term()}.
add_reverse_mapping(StorageData, Record, OnedataUserMap, Feed) ->
    StorageCredentials = luma_storage_user:get_storage_credentials(Record),
    Uid = binary_to_integer(maps:get(<<"uid">>, StorageCredentials)),
    case storage:is_posix_compatible(StorageData) andalso storage:is_imported(StorageData) of
        true ->
            luma_onedata_users:update_or_store_uid_mapping(StorageData, Uid, OnedataUserMap, Feed);
        false ->
            ok
    end.


%% @private
-spec delete_reverse_mapping(storage:data(), luma:uid()) -> ok | {error, term()}.
delete_reverse_mapping(StorageData, Uid) ->
    case storage:is_posix_compatible(StorageData) andalso storage:is_imported(StorageData) of
        true -> luma_onedata_users:delete_uid_mapping(StorageData, Uid, false);
        false -> ok
    end.
