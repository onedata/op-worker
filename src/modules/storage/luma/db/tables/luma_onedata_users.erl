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
%%% Tha mappings are used by storage_sync in 2 cases:
%%%  1) to associate synchronized file owners (represented as UID)
%%%     with corresponding Onedata user,
%%%  2) to associate synchronized ACLs, set for specific, named user,
%%%     with corresponding Onedata user.
%%%
%%% A separate table is created for each storage
%%% so the mappings are actually associated with pair (storage:id(), luma:uid() | luma:acl_who()).
%%%
%%% Mappings may be set in 2 ways:
%%%  * preconfigured using REST API in case EMBEDDED_LUMA
%%%    is set for given storage.
%%%  * cached after querying external, 3rd party LUMA server in case
%%%    EXTERNAL_LUMA mode is set for given storage.
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
-type storage() :: storage:id() | storage:data().

-export_type([key/0, record/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec map_uid_to_onedata_user(storage:data(), luma:uid()) ->
    {ok, record()} | {error, term()}.
map_uid_to_onedata_user(Storage, Uid) ->
   luma_db:get_or_acquire(Storage, ?KEY(ensure_integer(Uid), ?UID), ?MODULE, fun() ->
       acquire(Storage, Uid, ?UID)
    end).

-spec map_acl_user_to_onedata_user(storage:data(), luma:acl_who()) ->
    {ok, record()} | {error, term()}.
map_acl_user_to_onedata_user(Storage, AclUser) ->
    luma_db:get_or_acquire(Storage, ?KEY(AclUser, ?ACL), ?MODULE, fun() ->
        acquire(Storage, AclUser, ?ACL)
    end).

-spec store_by_acl_user(storage(), luma:acl_who(), luma_onedata_group:group_map()) -> ok | {error, term()}.
store_by_acl_user(Storage, AclUser, OnedataUserMap) ->
    ?extract_ok(store(Storage, ?KEY(AclUser, ?ACL), OnedataUserMap, ?LOCAL_FEED)).


-spec store_by_uid(storage(), luma:uid(), luma_onedata_user:user_map()) -> ok | {error, term()}.
store_by_uid(Storage, Uid, OnedataUserMap) ->
    Uid2 = ensure_integer(Uid),
    case store(Storage, ?KEY(Uid2, ?UID), OnedataUserMap, ?LOCAL_FEED) of
        {ok, OnedataUser} ->
            UserId = luma_onedata_user:get_user_id(OnedataUser),
            add_reverse_mapping(Storage, UserId, Uid2, ?LOCAL_FEED);
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
-spec update_or_store_uid_mapping(storage(), luma:uid(), od_user:id() | luma_onedata_user:user_map(),
    luma:feed()) -> ok | {error, term()}.
update_or_store_uid_mapping(Storage, Uid, UserId, Feed) when is_binary(UserId) ->
    OnedataUserMap = luma_onedata_user:to_json(luma_onedata_user:new(UserId)),
    update_or_store_uid_mapping(Storage, Uid, OnedataUserMap, Feed);
update_or_store_uid_mapping(Storage, Uid, OnedataUserMap, Feed) ->
    DefaultRecord = luma_onedata_user:new(OnedataUserMap),
    luma_db:update_or_store(Storage, ?KEY(ensure_integer(Uid), ?UID), ?MODULE,
        OnedataUserMap, DefaultRecord, Feed).


-spec delete_uid_mapping(storage:id(), luma:uid()) -> ok.
delete_uid_mapping(StorageId, Uid) ->
    delete_uid_mapping(StorageId, Uid, true).


-spec delete_uid_mapping(storage:id(), luma:uid(), DeleteReverseMapping :: boolean()) -> ok.
delete_uid_mapping(StorageId, Uid, false) ->
    luma_db:delete(StorageId, ?KEY(ensure_integer(Uid), ?UID), ?MODULE);
delete_uid_mapping(StorageId, Uid, true) ->
    Uid2 = ensure_integer(Uid),
    UserId = case luma_db:get(StorageId, ?KEY(Uid2, ?UID), ?MODULE) of
        {ok, OnedataUser} ->
            luma_onedata_user:get_user_id(OnedataUser);
        {error, not_found} ->
            undefined
    end,
    case delete_uid_mapping(StorageId, Uid2, false) of
        ok when UserId =/= undefined ->
            luma_storage_users:delete(StorageId, UserId);
        ok ->
            ok
    end.


-spec delete_acl_user_mapping(storage:id(), luma:acl_who()) -> ok.
delete_acl_user_mapping(StorageId, AclUser) ->
    luma_db:delete(StorageId, ?KEY(AclUser, ?ACL), ?MODULE).

-spec clear_all(storage:id()) -> ok | {error, term()}.
clear_all(StorageId) ->
    luma_db:clear_all(StorageId, ?MODULE).

-spec get_by_uid_and_describe(storage(), luma:uid()) ->
    {ok, json_utils:json_map()} | {error, term()}.
get_by_uid_and_describe(Storage, Uid) ->
    luma_db:get_and_describe(Storage, ?KEY(ensure_integer(Uid), ?UID), ?MODULE).

-spec get_by_acl_user_and_describe(storage(), luma:acl_who()) ->
    {ok, json_utils:json_map()} | {error, term()}.
get_by_acl_user_and_describe(Storage, AclUser) ->
    luma_db:get_and_describe(Storage, ?KEY(AclUser, ?ACL), ?MODULE).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec encode_key(internal_key(), key_type()) -> key().
encode_key(Uid, ?UID) ->
    <<?UID_PREFIX/binary, ?SEPARATOR/binary, (str_utils:to_binary(Uid))/binary>>;
encode_key(AclUser, ?ACL) ->
    <<?ACL_PREFIX/binary, ?SEPARATOR/binary, AclUser/binary>>.


-spec acquire(storage(), internal_key(), key_type()) ->
    {ok, record(), luma:feed()} | {error, term()}.
acquire(Storage, Key, Mode) ->
    case storage:get_luma_feed(Storage) of
        ?EXTERNAL_FEED ->
            acquire_from_external_feed(Storage, Key, Mode);
        _ ->
            {error, not_found}
    end.


-spec acquire_from_external_feed(storage(), internal_key(), key_type()) ->
    {ok, record(), luma:feed()} | {error, term()}.
acquire_from_external_feed(Storage, Uid, ?UID) ->
    acquire_uid_mapping(Storage, Uid);
acquire_from_external_feed(Storage, AclUser, ?ACL) ->
    acquire_acl_mapping(Storage, AclUser).


-spec acquire_uid_mapping(storage:data(), luma:uid()) ->
    {ok, record(), luma:feed()} | {error, term()}.
acquire_uid_mapping(Storage, Uid) ->
    case luma_external_feed:map_uid_to_onedata_user(Uid, Storage) of
        {ok, OnedataUserMap} ->
            OnedataUser = luma_onedata_user:new(OnedataUserMap),
            UserId = luma_onedata_user:get_user_id(OnedataUser),
            add_reverse_mapping(Storage, UserId, Uid, ?EXTERNAL_FEED),
            {ok, OnedataUser, ?EXTERNAL_FEED};
        Error ->
            Error
    end.

-spec acquire_acl_mapping(storage:data(), luma:acl_who()) ->
    {ok, record(), luma:feed()} | {error, term()}.
acquire_acl_mapping(Storage, AclUser) ->
    case luma_external_feed:map_acl_user_to_onedata_user(AclUser, Storage) of
        {ok, OnedataUserMap} ->
            {ok, luma_onedata_user:new(OnedataUserMap), ?EXTERNAL_FEED};
        Error ->
            Error
    end.

-spec store(storage(), key(), luma_onedata_user:user_map(), luma:feed()) ->
    {ok, record()} | {error, term()}.
store(Storage, Key, OnedataUser, Feed) ->
    case luma_sanitizer:sanitize_onedata_user(OnedataUser) of
        {ok, OnedataUserMap2} ->
            Record = luma_onedata_user:new(OnedataUserMap2),
            case luma_db:store(Storage, Key, ?MODULE, Record, Feed) of
                ok -> {ok, Record};
                Error -> Error
            end;
        Error2 ->
            Error2
    end.

-spec add_reverse_mapping(storage(), od_user:id(), luma:uid(), luma:feed()) -> ok | {error, term()}.
add_reverse_mapping(Storage, UserId, Uid, Feed) ->
    luma_storage_users:store_posix_compatible_mapping(Storage, UserId, Uid, Feed).

-spec ensure_integer(integer() | binary()) -> integer().
ensure_integer(Integer) when is_integer(Integer) -> Integer;
ensure_integer(Binary) when is_binary(Binary) -> binary_to_integer(Binary).