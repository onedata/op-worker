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
    clear_db_with_stale_namespaces/1,
    clear_stale_db_namespace/2,
    clear_space_entries/2,

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


%%--------------------------------------------------------------------
%% @doc
%% Deletes every entry a storage maps through, i.e. those in its current LUMA DB
%% namespace. Entries in namespaces it has already left behind are none of the
%% caller's concern - luma_db_garbage_collector deletes those.
%% @end
%%--------------------------------------------------------------------
-spec clear_db(storage:id() | storage:data() | storage_config:doc()) -> ok.
clear_db(StorageIdOrData) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    clear_all_tables(StorageData),
    emit_helper_params_changed_event(StorageData).


%%--------------------------------------------------------------------
%% @doc
%% Deletes every entry of a storage, in its current LUMA DB namespace and in all
%% the stale ones - for a storage that is going away. The garbage collector
%% reaches stale namespaces through the storage config, so once that document is
%% gone they can no longer be found; this is the last chance to delete them.
%% @end
%%--------------------------------------------------------------------
-spec clear_db_with_stale_namespaces(storage:id() | storage:data() | storage_config:doc()) -> ok.
clear_db_with_stale_namespaces(StorageIdOrData) ->
    StorageData = ensure_storage_data(StorageIdOrData),
    Namespaces = [
        storage:get_luma_db_namespace(StorageData)
        | storage:get_stale_luma_db_namespaces(StorageData)
    ],
    lists:foreach(fun(Namespace) ->
        clear_all_tables(StorageData, Namespace)
    end, Namespaces),

    emit_helper_params_changed_event(StorageData).


%%--------------------------------------------------------------------
%% @doc
%% Deletes the entries a storage left behind in one of its stale LUMA DB
%% namespaces - see luma_db and luma_db_garbage_collector.
%%
%% No event is emitted: a stale namespace backs no lookup, so its entries going
%% away changes nothing for the clients, and the event for the change that made
%% it stale has already been emitted (see storage_updater).
%% @end
%%--------------------------------------------------------------------
-spec clear_stale_db_namespace(
    storage:id() | storage:data() | storage_config:doc(),
    undefined | luma_db:namespace()
) ->
    ok.
clear_stale_db_namespace(StorageIdOrData, Namespace) ->
    clear_all_tables(ensure_storage_data(StorageIdOrData), Namespace).


%%--------------------------------------------------------------------
%% @doc
%% Deletes the entries a storage keeps for one space - the two tables that hold
%% any. Unlike the functions above, this narrows the scope by space, not by
%% namespace, and so always works within the storage's current one.
%% @end
%%--------------------------------------------------------------------
-spec clear_space_entries(storage:id() | storage:data(), od_space:id()) -> ok | {error, term()}.
clear_space_entries(StorageIdOrData, SpaceId) ->
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
-spec clear_all_tables(storage:data()) -> ok.
clear_all_tables(StorageData) ->
    clear_all_tables(StorageData, storage:get_luma_db_namespace(StorageData)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% The whole LUMA DB API addresses entries through storage data, deriving the
%% namespace from it, so a namespace other than the storage's current one is
%% reached by handing it storage data that points at the former. This is the
%% only place that does so.
%% @end
%%--------------------------------------------------------------------
-spec clear_all_tables(storage:data(), undefined | luma_db:namespace()) -> ok.
clear_all_tables(StorageData, Namespace) ->
    NamespacedStorageData = storage:with_luma_db_namespace(StorageData, Namespace),

    ?info("Clearing LUMA DB tables for storage '~ts' (namespace: ~tp)", [
        storage:get_id(StorageData), Namespace
    ]),

    Tables = [
        {luma_storage_users, fun luma_storage_users:clear_all/1},
        {luma_spaces_display_defaults, fun luma_spaces_display_defaults:clear_all/1},
        {luma_spaces_posix_storage_defaults, fun luma_spaces_posix_storage_defaults:clear_all/1},
        {luma_onedata_users, fun luma_onedata_users:clear_all/1},
        {luma_onedata_groups, fun luma_onedata_groups:clear_all/1}
    ],
    lists:foreach(fun({TableName, ClearFun}) ->
        ?info("Clearing LUMA table '~ts'", [TableName]),
        ClearFun(NamespacedStorageData)
    end, Tables),

    ?info("Successfully cleared LUMA DB").


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
