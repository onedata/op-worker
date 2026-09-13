%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Facade module for all LUMA CRUD operations called from outside the LUMA
%%% subsystem (e.g., rpc_api, storage lifecycle).
%%%
%%% NOTE: writes that change how a user resolves to storage credentials emit the
%%% 'helper_params_changed' event, so that direct IO clients re-fetch their
%%% params - op-worker itself resolves them anew for every operation and needs no
%%% notification. Only two of the five tables feed that resolution:
%%% luma_storage_users and luma_spaces_posix_storage_defaults. Writes to the
%%% display defaults and to the reverse mapping tables change nothing on the IO
%%% path and are therefore silent, and so is the lazy population performed by
%%% luma_db:get_or_acquire during ordinary traffic - it stores exactly what the
%%% requester has just been given.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_crud_api).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/logging.hrl").

-export([
    clear_db/1,
    clear_db/2,

    storage_users_store/3,
    storage_users_get_and_describe/2,
    storage_users_update/3,
    storage_users_delete/2,

    spaces_display_defaults_store/3,
    spaces_display_defaults_get_and_describe/2,
    spaces_display_defaults_delete/2,

    spaces_posix_storage_defaults_store/3,
    spaces_posix_storage_defaults_get_and_describe/2,
    spaces_posix_storage_defaults_delete/2,

    onedata_users_store_by_uid/3,
    onedata_users_get_by_uid_and_describe/2,
    onedata_users_delete_uid_mapping/2,
    onedata_users_store_by_acl_user/3,
    onedata_users_get_by_acl_user_and_describe/2,
    onedata_users_delete_acl_user_mapping/2,

    onedata_groups_store/3,
    onedata_groups_get_and_describe/2,
    onedata_groups_delete/2
]).


%%%===================================================================
%%% Management
%%%===================================================================


-spec clear_db(storage:id() | storage:data() | storage_config:doc()) -> ok.
clear_db(StorageIdOrData) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    StorageId = storage:get_id(StorageData),
    LumaGeneration = storage:get_luma_generation(StorageData),

    ?info("Clearing LUMA DB tables for storage '~ts' (generation: ~B)", [StorageId, LumaGeneration]),

    Tables = [
        {luma_storage_users, fun luma_storage_users:clear_all/1},
        {luma_spaces_display_defaults, fun luma_spaces_display_defaults:clear_all/1},
        {luma_spaces_posix_storage_defaults, fun luma_spaces_posix_storage_defaults:clear_all/1},
        {luma_onedata_users, fun luma_onedata_users:clear_all/1},
        {luma_onedata_groups, fun luma_onedata_groups:clear_all/1}
    ],
    lists:foreach(fun({TableName, ClearFun}) ->
        ?info("Clearing LUMA table '~ts'", [TableName]),
        ClearFun(StorageData)
    end, Tables),

    % NOTE: when clearing the dead generation after a LUMA config change, the
    % event has already been emitted for that change (see storage_updater) - a
    % duplicate only makes the clients re-fetch twice
    emit_helper_params_changed_event(StorageData),

    ?info("Successfully cleared LUMA DB").


-spec clear_db(storage:id() | storage:data(), od_space:id()) -> ok | {error, term()}.
clear_db(StorageIdOrData, SpaceId) ->
    StorageData = ensure_storage_data(StorageIdOrData),

    % NOTE: no event - this is called when the space is being unsupported, so the
    % clients are losing access to it altogether
    luma_spaces_display_defaults:delete(StorageData, SpaceId),
    luma_spaces_posix_storage_defaults:delete(StorageData, SpaceId).


%%%===================================================================
%%% Storage Users
%%%===================================================================


-spec storage_users_store(
    storage:id() | storage:data(),
    od_user:id() | luma_onedata_user:user_map(),
    luma_storage_user:user_map()
) ->
    {ok, od_user:id()} | {error, term()}.
storage_users_store(StorageIdOrData, OnedataUserMap, StorageUserMap) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    emit_event_on_success(StorageData, luma_storage_users:store(
        StorageData, OnedataUserMap, StorageUserMap
    )).


-spec storage_users_get_and_describe(storage:id() | storage:data(), od_user:id()) ->
    {ok, luma_storage_user:user_map()} | {error, term()}.
storage_users_get_and_describe(StorageIdOrData, UserId) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    luma_storage_users:get_and_describe(StorageData, UserId).


-spec storage_users_update(storage:id() | storage:data(), od_user:id(), luma_storage_user:user_map()) ->
    ok | {error, term()}.
storage_users_update(StorageIdOrData, UserId, StorageUserMap) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    emit_event_on_success(StorageData, luma_storage_users:update(
        StorageData, UserId, StorageUserMap
    )).


-spec storage_users_delete(storage:id() | storage:data(), od_user:id()) -> ok.
storage_users_delete(StorageIdOrData, UserId) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    emit_event_on_success(StorageData, luma_storage_users:delete(StorageData, UserId)).


%%%===================================================================
%%% Spaces Display Defaults
%%%===================================================================


-spec spaces_display_defaults_store(
    storage:id() | storage:data(),
    od_space:id(),
    luma_posix_credentials:credentials_map()
) ->
    ok | {error, term()}.
spaces_display_defaults_store(StorageIdOrData, SpaceId, DisplayDefaults) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    luma_spaces_display_defaults:store(StorageData, SpaceId, DisplayDefaults).


-spec spaces_display_defaults_get_and_describe(storage:id() | storage:data(), od_space:id()) ->
    {ok, luma_posix_credentials:credentials_map()} | {error, term()}.
spaces_display_defaults_get_and_describe(StorageIdOrData, SpaceId) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    luma_spaces_display_defaults:get_and_describe(StorageData, SpaceId).


-spec spaces_display_defaults_delete(storage:id() | storage:data(), od_space:id()) -> ok.
spaces_display_defaults_delete(StorageIdOrData, SpaceId) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    luma_spaces_display_defaults:delete(StorageData, SpaceId).


%%%===================================================================
%%% Spaces Posix Storage Defaults
%%%===================================================================


-spec spaces_posix_storage_defaults_store(
    storage:id() | storage:data(),
    od_space:id(),
    luma_posix_credentials:credentials_map()
) ->
    ok | {error, term()}.
spaces_posix_storage_defaults_store(StorageIdOrData, SpaceId, PosixDefaults) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    emit_event_on_success(StorageData, luma_spaces_posix_storage_defaults:store(
        StorageData, SpaceId, PosixDefaults
    )).


-spec spaces_posix_storage_defaults_get_and_describe(storage:id() | storage:data(), od_space:id()) ->
    {ok, luma_posix_credentials:credentials_map()} | {error, term()}.
spaces_posix_storage_defaults_get_and_describe(StorageIdOrData, SpaceId) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    luma_spaces_posix_storage_defaults:get_and_describe(StorageData, SpaceId).


-spec spaces_posix_storage_defaults_delete(storage:id() | storage:data(), od_space:id()) -> ok.
spaces_posix_storage_defaults_delete(StorageIdOrData, SpaceId) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    emit_event_on_success(StorageData, luma_spaces_posix_storage_defaults:delete(StorageData, SpaceId)).


%%%===================================================================
%%% Onedata Users
%%%===================================================================


-spec onedata_users_store_by_uid(
    storage:id() | storage:data(),
    luma:uid(),
    luma_onedata_user:user_map()
) ->
    ok | {error, term()}.
onedata_users_store_by_uid(StorageIdOrData, Uid, OnedataUser) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    luma_onedata_users:store_by_uid(StorageData, Uid, OnedataUser).


-spec onedata_users_get_by_uid_and_describe(storage:id() | storage:data(), luma:uid()) ->
    {ok, luma_onedata_user:user_map()} | {error, term()}.
onedata_users_get_by_uid_and_describe(StorageIdOrData, Uid) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    luma_onedata_users:get_by_uid_and_describe(StorageData, Uid).


-spec onedata_users_delete_uid_mapping(storage:id() | storage:data(), luma:uid()) ->
    ok | {error, term()}.
onedata_users_delete_uid_mapping(StorageIdOrData, Uid) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    luma_onedata_users:delete_uid_mapping(StorageData, Uid).


-spec onedata_users_store_by_acl_user(
    storage:id() | storage:data(),
    luma:acl_who(),
    luma_onedata_user:user_map()
) ->
    ok | {error, term()}.
onedata_users_store_by_acl_user(StorageIdOrData, AclUser, OnedataUser) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    luma_onedata_users:store_by_acl_user(StorageData, AclUser, OnedataUser).


-spec onedata_users_get_by_acl_user_and_describe(storage:id() | storage:data(), luma:acl_who()) ->
    {ok, luma_onedata_user:user_map()} | {error, term()}.
onedata_users_get_by_acl_user_and_describe(StorageIdOrData, AclUser) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    luma_onedata_users:get_by_acl_user_and_describe(StorageData, AclUser).


-spec onedata_users_delete_acl_user_mapping(storage:id() | storage:data(), luma:acl_who()) ->
    ok | {error, term()}.
onedata_users_delete_acl_user_mapping(StorageIdOrData, AclUser) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    luma_onedata_users:delete_acl_user_mapping(StorageData, AclUser).


%%%===================================================================
%%% Onedata Groups
%%%===================================================================


-spec onedata_groups_store(
    storage:id() | storage:data(),
    luma:acl_who(),
    luma_onedata_group:group_map()
) ->
    ok | {error, term()}.
onedata_groups_store(StorageIdOrData, AclGroup, OnedataGroup) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    luma_onedata_groups:store(StorageData, AclGroup, OnedataGroup).


-spec onedata_groups_get_and_describe(storage:id() | storage:data(), luma:acl_who()) ->
    {ok, luma_onedata_group:group_map()} | {error, term()}.
onedata_groups_get_and_describe(StorageIdOrData, AclGroup) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    luma_onedata_groups:get_and_describe(StorageData, AclGroup).


-spec onedata_groups_delete(storage:id() | storage:data(), luma:acl_who()) ->
    ok | {error, term()}.
onedata_groups_delete(StorageIdOrData, AclGroup) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    luma_onedata_groups:delete(StorageData, AclGroup).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ensure_storage_data(storage:id() | storage:data() | storage_config:doc()) -> storage:data().
ensure_storage_data(StorageIdOrDataOrConfig) ->
    {ok, Data} = storage:get(StorageIdOrDataOrConfig),
    Data.


%% @private
-spec emit_event_on_success(storage:data(), Result) -> Result when
    Result :: ok | {ok, term()} | {error, term()}.
emit_event_on_success(StorageData, ok) ->
    emit_helper_params_changed_event(StorageData),
    ok;
emit_event_on_success(StorageData, Result = {ok, _}) ->
    emit_helper_params_changed_event(StorageData),
    Result;
emit_event_on_success(_StorageData, Error = {error, _}) ->
    Error.


%% @private
-spec emit_helper_params_changed_event(storage:data()) -> ok.
emit_helper_params_changed_event(StorageData) ->
    fslogic_event_emitter:emit_helper_params_changed(storage:get_id(StorageData)).
