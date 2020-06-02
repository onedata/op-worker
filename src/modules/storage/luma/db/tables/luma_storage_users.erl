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
%%% Mappings may be set in 3 ways:
%%%  * filled by default algorithm in case NO_LUMA mode is set for given
%%%    storage - if storage is POSIX compatible, UID is generated and
%%%    used in both fields: storage_credentials and display_uid.
%%%    On POSIX incompatible storages, helper's AdminCtx is used as
%%%    storage_credentials and display_uid is generated.
%%%    (see acquire_default_mapping function).
%%%  * preconfigured using REST API in case EMBEDDED_LUMA
%%%    is set for given storage
%%%  * cached after querying external, 3rd party LUMA server in case
%%%    EXTERNAL_LUMA mode is set for given storage
%%%
%%% For more info please read the docs of luma.erl.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_storage_users).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get/2, store_posix_compatible_mapping/3, clear_all/1]).

-type key() :: od_user:id().
-type record() :: luma_storage_user:user().
-type storage() :: storage:id() | storage:data().

-export_type([key/0, record/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec get(storage(), key()) ->
    {ok, record()} | {error, term()}.
get(Storage, UserId) ->
    luma_db:get(Storage, UserId, ?MODULE, fun() ->
        acquire(Storage, UserId)
    end).

-spec store_posix_compatible_mapping(storage:data(), key(), luma:uid()) -> ok.
store_posix_compatible_mapping(Storage, UserId, Uid) ->
    LumaUser = luma_storage_user:new_posix_user(Uid),
    luma_db:store(Storage, UserId, ?MODULE, LumaUser).

-spec clear_all(storage:id()) -> ok.
clear_all(StorageId) ->
    luma_db:clear_all(StorageId, ?MODULE).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec acquire(storage:data(), key()) ->
    {ok, record()} | {error, term()}.
acquire(Storage, UserId) ->
    case storage:is_luma_enabled(Storage) of
        true -> acquire_mapping_from_external_luma(Storage, UserId);
        false -> acquire_default_mapping(Storage, UserId)
    end.


-spec acquire_mapping_from_external_luma(storage:data(), key()) ->
    {ok, record()} | {error, term()}.
acquire_mapping_from_external_luma(Storage, UserId) ->
    case external_luma:map_onedata_user_to_credentials(UserId, Storage) of
        {ok, StorageUserMap} ->
            {ok, luma_storage_user:new(UserId, StorageUserMap, Storage)};
        {error, external_luma_error} ->
            {error, not_found};
        OtherError ->
            OtherError
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function returns default mapping for user in case of NO_LUMA mode.
%% On POSIX compatible storages it generates uid basing on UserId.
%% On POSIX incompatible storages it returns AdminCtx of the storage.
%% @end
%%--------------------------------------------------------------------
-spec acquire_default_mapping(storage:data(), key()) ->
    {ok, record()} | {error, term()}.
acquire_default_mapping(Storage, UserId) ->
    StorageCredentials = case storage:is_posix_compatible(Storage) of
        true ->
            Uid = luma_utils:generate_uid(UserId),
            #{<<"uid">> => integer_to_binary(Uid)};
        false ->
            Helper = storage:get_helper(Storage),
            helper:get_admin_ctx(Helper)
    end,
    StorageUserMap = #{<<"storageCredentials">> => StorageCredentials},
    {ok, luma_storage_user:new(UserId, StorageUserMap, Storage)}.
