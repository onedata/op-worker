%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
% TODO update
%%% This module is used for storing reverse LUMA mappings.
%%% Mappings are used by storage_sync mechanism to associate
%%% storage users/groups with specific users/groups in onedata.
%%% Documents of this model are stored per StorageId.
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

-behaviour(luma_db).

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([
    map_uid_to_onedata_user/2,
    map_acl_user_to_onedata_user/2,
    store_uid_mapping/3,
    clear_all/1
]).

%% luma_db callbacks
-export([acquire/2]).

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
map_uid_to_onedata_user(Storage, Uid) ->
   luma_db:get(Storage, ?KEY(Uid, ?UID), ?MODULE).

-spec map_acl_user_to_onedata_user(storage:data(), luma:acl_who()) ->
    {ok, record()} | {error, term()}.
map_acl_user_to_onedata_user(Storage, AclUser) ->
    luma_db:get(Storage, ?KEY(AclUser, ?ACL), ?MODULE).

-spec store_uid_mapping(storage:data(), luma:uid(), od_user:id()) -> ok.
store_uid_mapping(Storage, Uid, UserId) ->
    OnedataUser = luma_onedata_user:new(UserId),
    luma_db:store(Storage, ?KEY(Uid, ?UID), ?MODULE, OnedataUser).

-spec clear_all(storage:id()) -> ok | {error, term()}.
clear_all(StorageId) ->
    luma_db:clear_all(StorageId, ?MODULE).

%%%===================================================================
%%% luma_db callbacks
%%%===================================================================

-spec acquire(storage:data(), key()) ->
    {ok, record()} | {error, term()}.
acquire(Storage, Key) ->
    case decode_key(Key) of
        {Uid, ?UID} -> acquire_uid_mapping(Uid, Storage);
        {AclUser, ?ACL} -> acquire_acl_mapping(AclUser, Storage)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec encode_key(internal_key(), key_type()) -> key().
encode_key(Uid, ?UID) ->
    <<?UID_PREFIX/binary, ?SEPARATOR/binary, (integer_to_binary(Uid))/binary>>;
encode_key(AclUser, ?ACL) ->
    <<?ACL_PREFIX/binary, ?SEPARATOR/binary, AclUser/binary>>.

-spec decode_key(key()) -> {internal_key(), key_type()}.
decode_key(Key) ->
    case binary:split(Key, ?SEPARATOR, [global]) of
        [?UID_PREFIX, UidBin] -> {binary_to_integer(UidBin), ?UID};
        [?ACL_PREFIX, AclUser] -> {AclUser, ?ACL}
    end.

-spec acquire_uid_mapping(luma:uid(), storage:data()) ->
    {ok, record()} | {error, term()}.
acquire_uid_mapping(Uid, Storage) ->
    case external_reverse_luma:map_uid_to_onedata_user(Uid, Storage) of
        {ok, OnedataUserMap} ->
            OnedataUser = luma_onedata_user:new(OnedataUserMap),
            UserId = luma_onedata_user:get_user_id(OnedataUser),
            % cache reverse mapping
            luma_storage_users:store_posix_compatible_mapping(Storage, UserId, Uid),
            {ok, OnedataUser};
        Error ->
            Error
    end.

-spec acquire_acl_mapping(luma:acl_who(), storage:data()) ->
    {ok, record()} | {error, term()}.
acquire_acl_mapping(AclUser, Storage) ->
    case external_reverse_luma:map_acl_user_to_onedata_user(AclUser, Storage) of
        {ok, OnedataUserMap} ->
            {ok, luma_onedata_user:new(OnedataUserMap)};
        Error ->
            Error
    end.