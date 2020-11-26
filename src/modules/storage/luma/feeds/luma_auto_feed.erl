%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions for acquiring automatic (default)
%%% mappings for LUMA DB.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_auto_feed).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/luma/luma.hrl").
-include_lib("ctool/include/http/headers.hrl").

%% API
-export([
    acquire_user_storage_credentials/2,
    acquire_default_display_credentials/2,
    generate_uid/1, generate_gid/1,
    generate_posix_credentials/1,
    generate_posix_credentials/2,
    acquire_default_posix_storage_credentials/2]).

%% Exported CT tests
-export([generate_posix_identifier/2]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function returns automatic mapping of Onedata user to storage
%% user.
%% On POSIX compatible storages it generates uid basing on UserId.
%% On POSIX incompatible storages it returns AdminCtx of the storage.
%% @end
%%--------------------------------------------------------------------
-spec acquire_user_storage_credentials(storage:data(), od_user:id()) ->
    {ok, luma_storage_user:user()}.
acquire_user_storage_credentials(Storage, UserId) ->
    StorageCredentials = case storage:is_posix_compatible(Storage) of
        true ->
            Uid = generate_uid(UserId),
            #{<<"uid">> => integer_to_binary(Uid)};
        false ->
            Helper = storage:get_helper(Storage),
            helper:get_admin_ctx(Helper)
    end,
    StorageUserMap = #{<<"storageCredentials">> => StorageCredentials},
    {ok, luma_storage_user:new(UserId, StorageUserMap, Storage)}.


%%--------------------------------------------------------------------
%% @doc
%% This function returns fallback posix credentials which are
%% UID and GID of storage mountpoint directory.
%% @end
%%--------------------------------------------------------------------
-spec acquire_default_posix_storage_credentials(storage:data(), od_space:id()) ->
    {ok, luma_posix_credentials:credentials()}.
acquire_default_posix_storage_credentials(Storage, SpaceId) ->
    StorageFileCtx = storage_file_ctx:new(<<?DIRECTORY_SEPARATOR>>, SpaceId, storage:get_id(Storage)),
    {#statbuf{st_uid = Uid, st_gid = Gid}, _} = storage_file_ctx:stat(StorageFileCtx),
    {ok, luma_posix_credentials:new(Uid, Gid)}.


-spec acquire_default_display_credentials(storage:data(), od_space:id()) ->
    {ok, luma_posix_credentials:credentials()}.
acquire_default_display_credentials(Storage, SpaceId) ->
    case storage:is_posix_compatible(Storage) of
        true ->
            luma_spaces_posix_storage_defaults:get_or_acquire(Storage, SpaceId);
        false ->
            {FallbackUid, FallbackGid} = generate_posix_credentials(SpaceId),
            {ok, luma_posix_credentials:new(FallbackUid, FallbackGid)}
    end.


-spec generate_uid(od_user:id()) -> luma:uid().
generate_uid(UserId) ->
    {ok, UidRange} = application:get_env(?APP_NAME, uid_range),
    generate_posix_identifier(UserId, UidRange).

-spec generate_gid(od_space:id()) -> luma:gid().
generate_gid(SpaceId) ->
    {ok, GidRange} = application:get_env(?APP_NAME, gid_range),
    generate_posix_identifier(SpaceId, GidRange).

-spec generate_posix_credentials(od_user:id(), od_space:id()) -> {luma:uid(), luma:gid()}.
generate_posix_credentials(UserId, SpaceId) ->
    {generate_uid(UserId), generate_gid(SpaceId)}.

-spec generate_posix_credentials(od_space:id()) -> {luma:uid(), luma:gid()}.
generate_posix_credentials(SpaceId) ->
    generate_posix_credentials(?SPACE_OWNER_ID(SpaceId), SpaceId).


%%%===================================================================
%%% Exported for CT tests
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Generates POSIX storage identifier (UID, GID) as a hash of passed Id.
%% @end
%%--------------------------------------------------------------------
-spec generate_posix_identifier(od_user:id() | od_space:id(),
    Range :: {non_neg_integer(), non_neg_integer()}) -> luma:uid() | luma:gid().
generate_posix_identifier(Id, {Low, High}) ->
    PosixId = crypto:bytes_to_integer(Id),
    Low + (PosixId rem (High - Low)).