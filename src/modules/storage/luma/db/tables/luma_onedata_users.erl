%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements LUMA DB table that associates storage user
%%% with Onedata user, represented by #luma_onedata_user record.
%%% Tha mappings are used by storage import in 2 cases:
%%%  1) to associate synchronized file owners (represented as UID)
%%%     with corresponding Onedata user,
%%%  2) to associate synchronized ACLs, set for specific, named user,
%%%     with corresponding Onedata user.
%%%
%%% A separate table is created for each storage
%%% so the mappings are actually associated with pair (storage:id(), luma:uid() | luma:acl_who()).
%%%
%%%
%%% For more info please read the docs of luma.erl module.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_onedata_users).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/luma/luma.hrl").

%% API
-export([
    map_uid_to_onedata_user/2,
    map_acl_user_to_onedata_user/2,
    store_by_uid/3,
    store_by_acl_user/3,
    update_or_store_uid_mapping/4,
    delete_uid_mapping/2,
    delete_uid_mapping/3,
    delete_acl_user_mapping/2,
    clear_all/1,
    get_by_uid_and_describe/2,
    get_by_acl_user_and_describe/2
]).

-define(UID, uid).
-define(ACL, acl).
-define(UID_PREFIX, <<"UID">>).
-define(ACL_PREFIX, <<"ACL">>).
-define(SEPARATOR, <<"%%">>).
-define(KEY(InternalKey, Mode), encode_key(InternalKey, Mode)).

-type key() :: binary().    % <<"UID" | "ACL, ?SEPARATOR, Uid | AclUser>>
-type record() :: luma_onedata_user:user().
-type internal_key() :: luma:uid() | luma:acl_who().
-type key_type() :: ?UID | ?ACL.

-export_type([key/0, record/0]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec map_uid_to_onedata_user(storage:data(), luma:uid()) ->
    {ok, record()} | {error, term()}.
map_uid_to_onedata_user(StorageData, Uid) ->
   luma_db:get_or_acquire(StorageData, ?KEY(ensure_integer(Uid), ?UID), ?MODULE, fun() ->
       acquire(StorageData, Uid, ?UID)
    end, [?POSIX_STORAGE, ?IMPORTED_STORAGE]).


-spec map_acl_user_to_onedata_user(storage:data(), luma:acl_who()) ->
    {ok, record()} | {error, term()}.
map_acl_user_to_onedata_user(StorageData, AclUser) ->
    luma_db:get_or_acquire(StorageData, ?KEY(AclUser, ?ACL), ?MODULE, fun() ->
        acquire(StorageData, AclUser, ?ACL)
    end, [?POSIX_STORAGE, ?IMPORTED_STORAGE]).


-spec store_by_acl_user(storage:data(), luma:acl_who(), luma_onedata_group:group_map()) -> ok | {error, term()}.
store_by_acl_user(StorageData, AclUser, OnedataUserMap) ->
    ?extract_ok(store(StorageData, ?KEY(AclUser, ?ACL), OnedataUserMap, ?LOCAL_FEED)).


-spec store_by_uid(storage:data(), luma:uid(), luma_onedata_user:user_map()) -> ok | {error, term()}.
store_by_uid(StorageData, Uid, OnedataUserMap) ->
    Uid2 = ensure_integer(Uid),
    case store(StorageData, ?KEY(Uid2, ?UID), OnedataUserMap, ?LOCAL_FEED) of
        {ok, OnedataUser} ->
            UserId = luma_onedata_user:get_user_id(OnedataUser),
            add_reverse_mapping(StorageData, UserId, Uid2, ?LOCAL_FEED);
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% This function is called by luma_storage_users module to update/add
%% reverse mapping, therefore there is no need to update corresponding
%% entry in luma_storage_users.
%% @end
%%--------------------------------------------------------------------
-spec update_or_store_uid_mapping(storage:data(), luma:uid(), od_user:id() | luma_onedata_user:user_map(),
    luma:feed()) -> ok | {error, term()}.
update_or_store_uid_mapping(StorageData, Uid, UserId, Feed) when is_binary(UserId) ->
    OnedataUserMap = luma_onedata_user:to_json(luma_onedata_user:new(UserId)),
    update_or_store_uid_mapping(StorageData, Uid, OnedataUserMap, Feed);
update_or_store_uid_mapping(StorageData, Uid, OnedataUserMap, Feed) ->
    DefaultRecord = luma_onedata_user:new(OnedataUserMap),
    luma_db:update_or_store(StorageData, ?KEY(ensure_integer(Uid), ?UID), ?MODULE,
        OnedataUserMap, DefaultRecord, Feed).


-spec delete_uid_mapping(storage:data(), luma:uid()) -> ok | {error, term()}.
delete_uid_mapping(StorageData, Uid) ->
    delete_uid_mapping(StorageData, Uid, true).


-spec delete_uid_mapping(storage:data(), luma:uid(), DeleteReverseMapping :: boolean()) -> ok | {error, term()}.
delete_uid_mapping(StorageData, Uid, false) ->
    luma_db:delete(StorageData, ?KEY(ensure_integer(Uid), ?UID), ?MODULE, [?POSIX_STORAGE, ?IMPORTED_STORAGE]);
delete_uid_mapping(StorageData, Uid, true) ->
    Uid2 = ensure_integer(Uid),
    UserId = case luma_db:get(StorageData, ?KEY(Uid2, ?UID), ?MODULE) of
        {ok, OnedataUser} ->
            luma_onedata_user:get_user_id(OnedataUser);
        {error, not_found} ->
            undefined
    end,
    case delete_uid_mapping(StorageData, Uid2, false) of
        ok when UserId =/= undefined ->
            luma_storage_users:delete(StorageData, UserId);
        ok ->
            ok;
        Error ->
            Error
    end.


-spec delete_acl_user_mapping(storage:data(), luma:acl_who()) ->  ok | {error, term()}.
delete_acl_user_mapping(StorageData, AclUser) ->
    luma_db:delete(StorageData, ?KEY(AclUser, ?ACL), ?MODULE, [?POSIX_STORAGE, ?IMPORTED_STORAGE]).


-spec clear_all(storage:data()) -> ok | {error, term()}.
clear_all(StorageData) ->
    luma_db:clear_all(StorageData, ?MODULE).


-spec get_by_uid_and_describe(storage:data(), luma:uid()) ->
    {ok, luma_onedata_user:user_map()} | {error, term()}.
get_by_uid_and_describe(StorageData, Uid) ->
    luma_db:get_and_describe(StorageData, ?KEY(ensure_integer(Uid), ?UID), ?MODULE, [?POSIX_STORAGE, ?IMPORTED_STORAGE]).


-spec get_by_acl_user_and_describe(storage:data(), luma:acl_who()) ->
    {ok, luma_onedata_user:user_map()} | {error, term()}.
get_by_acl_user_and_describe(StorageData, AclUser) ->
    luma_db:get_and_describe(StorageData, ?KEY(AclUser, ?ACL), ?MODULE, [?POSIX_STORAGE, ?IMPORTED_STORAGE]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec encode_key(internal_key(), key_type()) -> key().
encode_key(Uid, ?UID) ->
    <<?UID_PREFIX/binary, ?SEPARATOR/binary, (str_utils:to_binary(Uid))/binary>>;
encode_key(AclUser, ?ACL) ->
    <<?ACL_PREFIX/binary, ?SEPARATOR/binary, AclUser/binary>>.


%% @private
-spec acquire(storage:data(), internal_key(), key_type()) ->
    {luma_db:cache_policy(), record(), luma:feed()} | {error, term()}.
acquire(StorageData, Key, Mode) ->
    case storage:get_luma_feed(StorageData) of
        ?EXTERNAL_FEED ->
            acquire_from_external_feed(StorageData, Key, Mode);
        _ ->
            {error, not_found}
    end.


%% @private
-spec acquire_from_external_feed(storage:data(), internal_key(), key_type()) ->
    {luma_db:cache_policy(), record(), luma:feed()} | {error, term()}.
acquire_from_external_feed(StorageData, Uid, ?UID) ->
    acquire_uid_mapping(StorageData, Uid);
acquire_from_external_feed(StorageData, AclUser, ?ACL) ->
    acquire_acl_mapping(StorageData, AclUser).


%% @private
-spec acquire_uid_mapping(storage:data(), luma:uid()) ->
    {luma_db:cache_policy(), record(), luma:feed()} | {error, term()}.
acquire_uid_mapping(StorageData, Uid) ->
    case luma_external_feed:map_uid_to_onedata_user(Uid, StorageData) of
        {ok, OnedataUserMap} ->
            OnedataUser = luma_onedata_user:new(OnedataUserMap),
            UserId = luma_onedata_user:get_user_id(OnedataUser),
            add_reverse_mapping(StorageData, UserId, Uid, ?EXTERNAL_FEED),
            {cache, OnedataUser, ?EXTERNAL_FEED};
        Error ->
            Error
    end.


%% @private
-spec acquire_acl_mapping(storage:data(), luma:acl_who()) ->
    {luma_db:cache_policy(), record(), luma:feed()} | {error, term()}.
acquire_acl_mapping(StorageData, AclUser) ->
    case luma_external_feed:map_acl_user_to_onedata_user(AclUser, StorageData) of
        {ok, OnedataUserMap} ->
            {cache, luma_onedata_user:new(OnedataUserMap), ?EXTERNAL_FEED};
        Error ->
            Error
    end.


%% @private
-spec store(storage:data(), key(), luma_onedata_user:user_map(), luma:feed()) ->
    {ok, record()} | {error, term()}.
store(StorageData, Key, OnedataUser, Feed) ->
    case luma_sanitizer:sanitize_onedata_user(OnedataUser) of
        {ok, OnedataUserMap2} ->
            Record = luma_onedata_user:new(OnedataUserMap2),
            case luma_db:store(StorageData, Key, ?MODULE, Record, Feed, ?FORCE_OVERWRITE,
                [?POSIX_STORAGE, ?IMPORTED_STORAGE]
            ) of
                ok -> {ok, Record};
                Error -> Error
            end;
        Error2 ->
            Error2
    end.


%% @private
-spec add_reverse_mapping(storage:data(), od_user:id(), luma:uid(), luma:feed()) -> ok | {error, term()}.
add_reverse_mapping(StorageData, UserId, Uid, Feed) ->
    luma_storage_users:store_posix_compatible_mapping(StorageData, UserId, Uid, Feed).


%% @private
-spec ensure_integer(integer() | binary()) -> integer().
ensure_integer(Integer) when is_integer(Integer) -> Integer;
ensure_integer(Binary) when is_binary(Binary) -> binary_to_integer(Binary).
