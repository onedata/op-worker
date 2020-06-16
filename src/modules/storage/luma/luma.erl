%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
% TODO update, nowa tabela feedy
%%% This is API module for Local User Mapping database (aka LUMA DB) .
%%% It contains functions that should be used for mapping onedata users
%%% to storage users and the other way round.
%%%
%%% LUMA DB consists of 5 tables, represented by the following modules:
%%%  * luma_storage_users,
%%%  * luma_spaces_posix_storage_defaults,
%%%  * luma_spaces_display_defaults,
%%%  * luma_onedata_users,
%%%  * luma_onedata_groups.
%%%
%%%
%%% There are 3 types of feed for LUMA DB that can be configured for a specific storage:
%%%  * AUTO_FEED - LUMA DB will be automatically filled.
%%%  * LOCAL_FEED - custom mappings in LUMA DB can be set using REST API.
%%%                 It must be configured before supporting space with the storage.
%%%  * EXTERNAL_FEED - external HTTP server that implements required API (described on onedata.org).
%%%                    The server is lazily queried for custom mappings
%%%                    which are then stored in the LUMA DB.
%%%
%%% Functions exported by this module can be divided into 3 groups:
%%%  * LUMA functions
%%%  * reverse LUMA functions
%%%  * management functions
%%%
%%% Each of the above groups is described below.
%%%
%%%-------------------------------------------------------------------
%%% LUMA API
%%%-------------------------------------------------------------------
%%% This API is used to map onedata user to 2 types of credentials:
%%%  * storage_credentials() - these are credentials passed to helper.
%%%    They allow to perform operations on storage in context
%%%    of a specific user.
%%%  * display_credentials() - these are POSIX credentials (UID & GID)
%%%    which are returned in getattr response. They are used to present
%%%    file owners in the result of ls operation in Oneclient.
%%%
%%% Credentials are stored in 3 tables:
%%%  * luma_storage_users - this model is used for storing
%%%    storage_credentials() for a pair (storage:id(), od_user:id()).
%%     It also stores display_uid which is a component of
%%     display_credentials().
%%%  * luma_spaces_posix_storage_defaults - this model is used
%%%    only on POSIX compatible storages (described below
%%%    in the section considering acquiring of credentials on POSIX
%%%    compatible storages).
%%%  * luma_spaces_display_defaults - this model is used for storing
%%%    default display credentials for a given space support
%%%    identified by pair (storage:id(), od_space:id()).

%%%    Please see luma_storage_users.erl, luma_spaces_posix_storage_defaults.erl
%%%    and luma_spaces_display_defaults.erl for more info
%%%    (i.e. how tables are filled according to LUMA feed
%%%    for given storage).
%%%
%%% On each type of storage, display_credentials() have the
%%% following structure:
%%% {#luma_storage_user.display_uid, #luma_posix_credentials.gid}
%%%
%%% where #luma_posix_credentials record is acquired from luma_spaces_display_defaults table.
%%%
%%%
%%% storage_credentials() are composed differently depending on
%%% the storage type (whether it is POSIX-compatible).
%%%
%%% On POSIX incompatible storages, storage_credentials() are
%%% basically #luma_storage_user.storage_credentials field.
%%%
%%% On POSIX compatible storages,
%%% (currently POSIX, GLUSTERFS, NULLDEVICE) only uid field is stored
%%% in #luma_storage_user.storage_credentials. That is because we treat all
%%% space members as a space group. That means, that when accessing
%%% a file, group permissions are checked for all members of the space.
%%% For consistency, it is vital to ensure, that all files created in
%%% the space will have the same GID owner (as all space members are
%%% in the same, virtual, "space group").
%%% Due to above reasons, storage_credentials() on POSIX compatible
%%% storages are constructed in the following way:
%%% #{
%%%     <<"uid">> => maps:get(<<"uid">>, #luma_storage_user.storage_credentials),
%%      #luma_posix_credentials record is acquired from luma_spaces_posix_storage_defaults table
%%%     <<"gid">> => #luma_posix_credentials.gid
%%% }
%%%
%%%-------------------------------------------------------------------
%%% Reverse LUMA API
%%%-------------------------------------------------------------------
%%% This API is used only by storage_sync mechanism.
%%% It is used to map storage users/groups to users/groups in onedata.
%%%
%%% If LOCAL_FEED mode is set for given storage, mappings should
%%% be set in the luma_onedata_users and luma_onedata_groups tables
%%% using REST API.
%%%
%%% If EXTERNAL_FEED mode is set for given storage, external 3rd party
%%% server is queried for mappings and mappings are stored in
%%% luma_onedata_users and luma_onedata_groups tables.
%%%
%%% There are 3 operations implemented:
%%%  * map_uid_to_onedata_user - which allows to map storage owner
%%%    (identified by UID) of a specific, synchronized file to a onedata user.
%%%    The resulting user becomes owner of a file  which means that it is
%%%    set as file's owner in file_meta document).
%%%    If LUMA service is disabled for given storage, virtual ?SPACE_OWNER(SpaceId)
%%%    becomes owner of a synchronized file.
%%%  * map_acl_user_to_onedata_user - which allows to map a named ACL user
%%%    to a onedata user. This mapping allows to associate ACE with a specific
%%%    logical user.
%%%    Enabling LUMA service is necessary for synchronizing storage NFSv4 ACLs.
%%%    If LUMA service is disabled for given storage, this operation will
%%%    return error.
%%%  * map_acl_group_to_onedata_group - which allows to map a named ACL group
%%%    to a onedata group. This mapping allows to associate ACE with a specific
%%%    logical group.
%%%    Enabling LUMA service is necessary for synchronizing storage NFSv4 ACLs.
%%%    If LUMA service is disabled for given storage, this operation will
%%%    return error.
%%%
%%% GIDs on synced storage - IMPORTANT NOTE!!!
%%%
%%% It is possible that synced files have different GIDs. We do not try
%%% to map them to onedata groups model as it's not compatible with POSIX
%%% groups model.
%%% It is encouraged that admin of the legacy storage that is to be synced
%%% should ensure that the file structure is compliant with Onedata model
%%% (all files in the space should have the same group owner).
%%% If such "preparation" is not performed we must accept that even if
%%% access is granted by logical permissions check, it may be denied by the
%%% storage.
%%%
%%% We only save synced gid in file_meta and use this GID as the override
%%% for display_gid but only in the syncing provider.
%%% (see file_ctx:get_display_credentials/1)
%%%
%%%-------------------------------------------------------------------
%%% Management API
%%%-------------------------------------------------------------------
%%% Other function, exported by this module, that are not directly
%%% related to mapping users.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(luma).

-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% LUMA API
-export([
    map_to_storage_credentials/3,
    map_to_storage_credentials/4,
    map_to_display_credentials/3
]).

%% Reverse LUMA API
-export([
    map_uid_to_onedata_user/3,
    map_acl_user_to_onedata_user/2,
    map_acl_group_to_onedata_group/2
]).

%% Management API functions
-export([
    clear_db/1,
    clear_db/2,
    add_helper_specific_fields/5
]).

-type uid() :: luma_posix_credentials:uid().
-type gid() :: luma_posix_credentials:gid().
-type acl_who() :: binary().
-type display_credentials() :: {uid(), gid()}.
-type storage_credentials() :: helper:user_ctx().

-type feed() :: luma_config:feed().

-export_type([
    uid/0, gid/0, acl_who/0,
    storage_credentials/0, display_credentials/0,
    feed/0
]).

%%%===================================================================
%%% LUMA functions
%%%===================================================================

-spec map_to_storage_credentials(od_user:id(), od_space:id(), storage:id() | storage:data()) ->
    {ok, storage_credentials()} | {error, term()}.
map_to_storage_credentials(UserId, SpaceId, StorageId) ->
    map_to_storage_credentials(?ROOT_SESS_ID, UserId, SpaceId, StorageId).

-spec map_to_storage_credentials(session:id(), od_user:id(), od_space:id(),
    storage:id() | storage:data()) -> {ok, storage_credentials()} | {error, term()}.
map_to_storage_credentials(SessId, UserId, SpaceId, Storage) ->
    case map_to_storage_credentials_internal(UserId, SpaceId, Storage) of
        {ok, StorageCredentials} ->
            LumaMode = storage:get_luma_feed(Storage),
            add_helper_specific_fields(UserId, SessId, StorageCredentials, storage:get_helper(Storage), LumaMode);
        Error ->
            Error
    end.


-spec map_to_display_credentials(od_user:id(), od_space:id(), storage:id() | storage:data() | undefined) ->
    {ok, display_credentials()} | {error, term()}.
map_to_display_credentials(?ROOT_USER_ID, _SpaceId, _Storage) ->
    {ok, {?ROOT_UID, ?ROOT_GID}};
map_to_display_credentials(OwnerId, SpaceId, undefined) ->
    % unsupported space;
    {ok, luma_auto_feed:generate_posix_credentials(OwnerId, SpaceId)};
map_to_display_credentials(OwnerId, SpaceId, Storage) ->
    try
        case fslogic_uuid:is_space_owner(OwnerId) of
            true -> map_space_owner_to_display_credentials(Storage, SpaceId);
            false -> map_normal_user_to_display_credentials(OwnerId, Storage, SpaceId)
        end
    catch
        Error:Reason ->
            ?error_stacktrace("luma:map_to_display_credentials for user: ~p on storage: ~p failed with "
                "unexpected error ~p:~p.", [OwnerId, storage:get_id(Storage), Error, Reason]),
            {error, ?EACCES}
    end.

%%%===================================================================
%%% Reverse LUMA functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns od_user:id() for storage user associated with given Uid.
%% If LUMA service is disabled, function returns ?SPACE_OWNER_ID(SpaceId).
%% @end
%%--------------------------------------------------------------------
-spec map_uid_to_onedata_user(uid(), od_space:id(), storage:id()) ->
    {ok, od_user:id()} | {error, term()}.
map_uid_to_onedata_user(Uid, SpaceId, StorageId) ->
    {ok, Storage} = storage:get(StorageId),
    case storage:is_not_auto_luma_feed(Storage) andalso is_reverse_luma_supported(Storage) of
        true ->
            case luma_onedata_users:map_uid_to_onedata_user(Storage, Uid) of
                {ok, OnedataUser} ->
                    {ok, luma_onedata_user:get_user_id(OnedataUser)};
                Error ->
                    Error
            end;
        false ->
            {ok, ?SPACE_OWNER_ID(SpaceId)}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns od_user:id() for storage user associated with given
%% NFSv4 ACL username.
%% If LUMA service is disabled, function returns error.
%% @end
%%--------------------------------------------------------------------
-spec map_acl_user_to_onedata_user(acl_who(), storage:id()) ->
    {ok, od_user:id()} | {error, term()}.
map_acl_user_to_onedata_user(AclUser, StorageId) ->
    {ok, Storage} = storage:get(StorageId),
    case {storage:is_not_auto_luma_feed(Storage), is_reverse_luma_supported(Storage)} of
        {true, true} ->
            case luma_onedata_users:map_acl_user_to_onedata_user(Storage, AclUser) of
                {ok, OnedataUser} ->
                    {ok, luma_onedata_user:get_user_id(OnedataUser)};
                Error ->
                    Error
            end;
        {_, false} ->
            {error, reverse_luma_not_supported};
        {false, _} ->
            {error, luma_disabled}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns od_group:id() for storage group associated with given
%% NFSv4 ACL group.
%% If LUMA service is disabled, function returns error.
%% @end
%%--------------------------------------------------------------------
-spec map_acl_group_to_onedata_group(acl_who(), storage:id()) ->
    {ok, od_group:id()} | {error, term()}.
map_acl_group_to_onedata_group(AclGroup, StorageId) ->
    {ok, Storage} = storage:get(StorageId),
    case {storage:is_not_auto_luma_feed(Storage), is_reverse_luma_supported(Storage)} of
        {true, true} ->
            case luma_onedata_groups:map_acl_group_to_onedata_group(Storage, AclGroup) of
                {ok, OnedataGroup} ->
                    {ok, luma_onedata_group:get_group_id(OnedataGroup)};
                Error ->
                    Error
            end;
        {_, false} ->
            {error, reverse_luma_not_supported};
        {false, _} ->
            {error, luma_disabled}
    end.

%%%===================================================================
%%% Management API functions
%%%===================================================================

-spec clear_db(storage:id()) -> ok | {error, term()}.
clear_db(StorageId) ->
    luma_storage_users:clear_all(StorageId),
    luma_spaces_display_defaults:clear_all(StorageId),
    luma_spaces_posix_storage_defaults:clear_all(StorageId),
    luma_onedata_users:clear_all(StorageId),
    luma_onedata_groups:clear_all(StorageId).

-spec clear_db(storage:id(), od_space:id()) -> ok | {error, term()}.
clear_db(StorageId, SpaceId) ->
    % todo rename
    luma_spaces_display_defaults:delete(StorageId, SpaceId),
    luma_spaces_posix_storage_defaults:delete(StorageId, SpaceId).


-spec add_helper_specific_fields(od_user:id(), session:id(), luma:storage_credentials(),
    helpers:helper(), luma:feed()) -> any().
add_helper_specific_fields(UserId, SessionId, StorageCredentials, Helper, LumaFeed) ->
    case helper:get_name(Helper) of
        ?WEBDAV_HELPER_NAME ->
            add_webdav_specific_fields(UserId, SessionId, StorageCredentials, Helper, LumaFeed);
        _Other ->
            {ok, StorageCredentials}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec map_to_storage_credentials_internal(od_user:id(), od_space:id(),
    storage:id() | storage:data()) -> {ok, storage_credentials()} | {error, term()}.
map_to_storage_credentials_internal(?ROOT_USER_ID, _SpaceId, Storage) ->
    Helper = storage:get_helper(Storage),
    {ok, helper:get_admin_ctx(Helper)};
map_to_storage_credentials_internal(UserId, SpaceId, Storage) ->
    try
        {ok, StorageData} = storage:get(Storage),
        case fslogic_uuid:is_space_owner(UserId) of
            true ->
                map_space_owner_to_storage_credentials(StorageData, SpaceId);
            false ->
                map_onedata_user_to_storage_credentials(UserId, StorageData, SpaceId)
        end
    catch
        Error2:Reason ->
            ?error_stacktrace("luma:map_to_storage_credentials for user: ~p on storage: ~p failed with "
            "unexpected error ~p:~p.", [UserId, storage:get_id(Storage), Error2, Reason]),
            {error, ?EACCES}
    end.


-spec add_webdav_specific_fields(od_user:id(), session:id(), storage_credentials(), helpers:helper(), feed()) ->
    {ok, luma:storage_credentials()} | {error, term()}.
add_webdav_specific_fields(UserId, SessionId, StorageCredentials = #{
    <<"credentialsType">> := <<"oauth2">>
}, Helper, LumaFeed) ->
    {UserId2, SessionId2} = case fslogic_uuid:is_space_owner(UserId) of
        true ->
            % space owner uses helper admin_ctx
            {?ROOT_USER_ID, ?ROOT_SESS_ID};
        false ->
            {UserId, SessionId}
    end,
    choose_idp_and_fill_in_webdav_oauth2_token(UserId2, SessionId2, StorageCredentials, Helper, LumaFeed);
add_webdav_specific_fields(_UserId, _SessionId, StorageCredentials, _Helper, _LumaFeed) ->
    {ok, StorageCredentials}.


-spec choose_idp_and_fill_in_webdav_oauth2_token(od_user:id(), session:id(), luma:storage_credentials(),
    helpers:helper(), feed()) -> {ok, luma:storage_credentials()} | {error, term()}.
choose_idp_and_fill_in_webdav_oauth2_token(UserId, SessionId, StorageCredentials, Helper, LumaFeed) ->
    HelperArgs = helper:get_args(Helper),
    case maps:get(<<"oauth2IdP">>, HelperArgs, undefined) of
        undefined ->
            % OAuth2IdP was not explicitly set, try to infer it
            case provider_logic:zone_get_offline_access_idps() of
                {ok, [OAuth2IdP]} ->
                    fill_in_webdav_oauth2_token(UserId, SessionId, StorageCredentials, OAuth2IdP, LumaFeed);
                {ok, []} ->
                    ?error("Empty list of identity providers retrieved from Onezone"),
                    {error, missing_identity_provider};
                {ok, _} ->
                    ?error("Ambiguous list of identity providers retrieved from Onezone"),
                    {error, ambiguous_identity_provider}
            end;
        OAuth2IdP ->
            fill_in_webdav_oauth2_token(UserId, SessionId, StorageCredentials, OAuth2IdP, LumaFeed)
    end.


-spec fill_in_webdav_oauth2_token(od_user:id(), session:id(), luma:storage_credentials(),
    binary(), feed()) -> {ok, luma:storage_credentials()} | {error, term()}.
fill_in_webdav_oauth2_token(?ROOT_USER_ID, ?ROOT_SESS_ID, AdminCredentials = #{
    <<"onedataAccessToken">> := OnedataAccessToken,
    <<"adminId">> := AdminId
}, OAuth2IdP, _LumaFeed) ->
    TokenCredentials = auth_manager:build_token_credentials(
        OnedataAccessToken, undefined, undefined,
        undefined, disallow_data_access_caveats
    ),
    {ok, {IdPAccessToken, TTL}} = idp_access_token:acquire(
        AdminId, TokenCredentials, OAuth2IdP
    ),
    AdminCtx2 = maps:remove(<<"onedataAccessToken">>, AdminCredentials),
    {ok, AdminCtx2#{
        <<"accessToken">> => IdPAccessToken,
        <<"accessTokenTTL">> => integer_to_binary(TTL)
    }};
fill_in_webdav_oauth2_token(_UserId, _SessionId, AdminCredentials = #{
    <<"onedataAccessToken">> := OnedataAccessToken,
    <<"adminId">> := AdminId
}, OAuth2IdP, ?AUTO_FEED) ->
    % use AdminCtx in case of LUMA ?AUTO_FEED
    TokenCredentials = auth_manager:build_token_credentials(
        OnedataAccessToken, undefined, undefined,
        undefined, disallow_data_access_caveats
    ),
    {ok, {IdPAccessToken, TTL}} = idp_access_token:acquire(AdminId, TokenCredentials, OAuth2IdP),
    AdminCtx2 = maps:remove(<<"onedataAccessToken">>, AdminCredentials),
    {ok, AdminCtx2#{
        <<"accessToken">> => IdPAccessToken,
        <<"accessTokenTTL">> => integer_to_binary(TTL)
    }};
fill_in_webdav_oauth2_token(UserId, SessionId, StorageCredentials, OAuth2IdP, _LumaFeed) ->
    {ok, {IdPAccessToken, TTL}} = idp_access_token:acquire(UserId, SessionId, OAuth2IdP),
    UserCtx2 = maps:remove(<<"onedataAccessToken">>, StorageCredentials),
    {ok, UserCtx2#{
        <<"accessToken">> => IdPAccessToken,
        <<"accessTokenTTL">> => integer_to_binary(TTL)
    }}.

-spec map_space_owner_to_storage_credentials(storage:data(), od_space:id()) ->
    {ok, storage_credentials()} | {error, term()}.
map_space_owner_to_storage_credentials(Storage, SpaceId) ->
    case storage:is_posix_compatible(Storage) of
        true ->
            {ok, SpaceDefaults} = luma_spaces_posix_storage_defaults:get_or_acquire(Storage, SpaceId),
            DefaultUid = luma_posix_credentials:get_uid(SpaceDefaults),
            DefaultGid = luma_posix_credentials:get_gid(SpaceDefaults),
            {ok, #{
                <<"uid">> => integer_to_binary(DefaultUid),
                <<"gid">> => integer_to_binary(DefaultGid)
            }};
        false ->
            Helper = storage:get_helper(Storage),
            {ok, helper:get_admin_ctx(Helper)}
    end.

-spec map_onedata_user_to_storage_credentials(od_user:id(), storage:data(), od_space:id()) ->
    {ok, storage_credentials()} | {error, term()}.
map_onedata_user_to_storage_credentials(UserId, Storage, SpaceId) ->
    case luma_storage_users:get_or_acquire(Storage, UserId) of
        {ok, LumaUser} ->
            StorageCredentials = luma_storage_user:get_storage_credentials(LumaUser),
            StorageCredentials2 = maps:remove(<<"type">>, StorageCredentials),
            case storage:is_posix_compatible(Storage) of
                true ->
                    {ok, SpaceDefaults} = luma_spaces_posix_storage_defaults:get_or_acquire(Storage, SpaceId),
                    DefaultGid = luma_posix_credentials:get_gid(SpaceDefaults),
                    {ok, StorageCredentials2#{<<"gid">> => integer_to_binary(DefaultGid)}};
                false ->
                    {ok, StorageCredentials2}
            end;
        Error ->
            Error
    end.

-spec map_space_owner_to_display_credentials(storage:id() | storage:data(), od_space:id()) ->
    {ok, display_credentials()}.
map_space_owner_to_display_credentials(Storage, SpaceId) ->
    {ok, SpaceDefaults} = luma_spaces_display_defaults:get_or_acquire(Storage, SpaceId),
    DisplayUid = luma_posix_credentials:get_uid(SpaceDefaults),
    DisplayGid = luma_posix_credentials:get_gid(SpaceDefaults),
    {ok, {DisplayUid, DisplayGid}}.

-spec map_normal_user_to_display_credentials(od_user:id(), storage:id() | storage:data(), od_space:id()) ->
    {ok, display_credentials()} | {error, term()}.
map_normal_user_to_display_credentials(OwnerId, Storage, SpaceId) ->
    case luma_storage_users:get_or_acquire(Storage, OwnerId) of
        {ok, LumaUser} ->
            {ok, SpaceDefaults} = luma_spaces_display_defaults:get_or_acquire(Storage, SpaceId),
            DisplayUid = luma_storage_user:get_display_uid(LumaUser),
            DisplayGid = luma_posix_credentials:get_gid(SpaceDefaults),
            {ok, {DisplayUid, DisplayGid}};
        Error ->
            Error
    end.

-spec is_reverse_luma_supported(storage:data()) -> boolean().
is_reverse_luma_supported(Storage) ->
    Helper = storage:get_helper(Storage),
    helper:is_posix_compatible(Helper).