%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% The main module responsible for Local User Mapping (aka LUMA).
%%% It contains functions that should be used for mapping onedata users
%%% to storage users and the other way round.
%%%
%%% There are 3 modes of LUMA that can be configured for a specific storage:
%%%  * NO_LUMA - no luma service, default mappings are used
%%%  * EMBEDDED_LUMA - custom mappings can be set using REST API
%%%                    and stored in the database.
%%%                    It must be configured before supporting space with
%%%                    the storage.
%%%  * EXTERNAL_LUMA - external HTTP server that implements required
%%%                    LUMA API (described on onedata.org).
%%%                    The server is lazily queried for custom mappings
%%%                    which are then stored in the database. This mode
%%%                    can be treated as a lazy feed for embedded luma store.
%%%
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
%%%  * storage_credentials() - these are basically credentials passed
%%%    to helper. They allow to perform operations on storage in context
%%%    of a specific user.
%%%  * display_credentials() - these are POSIX credentials (UID & GID)
%%%    which are returned in getattr response. They are used to present
%%%    file owners in the result of ls operation in Oneclient.
%%%
%%% % TODO trzeba opisac priorytet (albo odeslac do doków gdzie to jest)
%%% % TODO trzeba opisac co mozna skonfigurowac, ze jest mapowanie dla uzytkownika i dla space'a
%%% % TODO trzeba opisac czemu na posix-like jest troche inaczej (groupa)
%%%
%%%
%%%-------------------------------------------------------------------
%%% Reverse LUMA API
%%%-------------------------------------------------------------------
%%% This API is used only by storage_sync mechanism.
%%% It is used to map storage users/groups to users/groups in onedata.
%%%
%%% If EMBEDDED_LUMA mode is set for given storage, mappings should
%%% be set in the luma_reverse_cache model using REST API.
%%%
%%% If EXTERNAL_LUMA mode is set for given storage, external 3rd party
%%% server is queried for mappings and mappings are stored in
%%% luma_reverse_cache model
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
    invalidate_cache/1,
    invalidate_cache/2,
    add_helper_specific_fields/4
]).

-type uid() :: non_neg_integer().
-type gid() :: non_neg_integer().
-type display_credentials() :: {uid(), gid()}.
-type storage_credentials() :: helper:user_ctx().
-type space_mapping_response() :: external_luma:space_mapping_response().

-type acl_who() :: binary().

-export_type([uid/0, gid/0, storage_credentials/0, display_credentials/0,
    space_mapping_response/0, acl_who/0]).

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
            add_helper_specific_fields(UserId, SessId, StorageCredentials, storage:get_helper(Storage));
        Error ->
            Error
    end.


-spec map_to_display_credentials(od_user:id(), od_space:id(), storage:id() | storage:data() | undefined) ->
    {ok, display_credentials()} | {error, term()}.
map_to_display_credentials(?ROOT_USER_ID, _SpaceId, _Storage) ->
    {ok, {?ROOT_UID, ?ROOT_GID}};
map_to_display_credentials(OwnerId, SpaceId, undefined) ->
    % unsupported space;
    {ok, {luma_utils:generate_uid(OwnerId), luma_utils:generate_gid(SpaceId)}};
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
    case storage:is_luma_enabled(Storage) andalso is_reverse_luma_supported(Storage) of
        true ->
            luma_reverse_cache:map_uid_to_onedata_user(Storage, Uid);
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
    case {storage:is_luma_enabled(Storage), is_reverse_luma_supported(Storage)} of
        {true, true} ->
            luma_reverse_cache:map_acl_user_to_onedata_user(Storage, AclUser);
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
    case {storage:is_luma_enabled(Storage), is_reverse_luma_supported(Storage)} of
        {true, true} ->
            luma_reverse_cache:map_acl_group_to_onedata_group(Storage, AclGroup);
        {_, false} ->
            {error, reverse_luma_not_supported};
        {false, _} ->
            {error, luma_disabled}
    end.

%%%===================================================================
%%% Management API functions
%%%===================================================================

-spec invalidate_cache(storage:id()) -> ok | {error, term()}.
invalidate_cache(StorageId) ->
    luma_users_cache:delete(StorageId),
    luma_spaces_cache:delete(StorageId),
    luma_reverse_cache:delete(StorageId).

-spec invalidate_cache(storage:id(), od_space:id()) -> ok | {error, term()}.
invalidate_cache(StorageId, SpaceId) ->
    luma_spaces_cache:delete(StorageId, SpaceId).


-spec add_helper_specific_fields(od_user:id(), session:id(), luma:storage_credentials(),
    helpers:helper()) -> any().
add_helper_specific_fields(UserId, SessionId, StorageCredentials, Helper) ->
    case helper:get_name(Helper) of
        ?WEBDAV_HELPER_NAME ->
            add_webdav_specific_fields(UserId, SessionId, StorageCredentials, Helper);
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
                map_normal_user_to_storage_credentials(UserId, StorageData, SpaceId)
        end
    catch
        Error2:Reason ->
            ?error_stacktrace("luma:map_to_storage_credentials for user: ~p on storage: ~p failed with "
            "unexpected error ~p:~p.", [UserId, storage:get_id(Storage), Error2, Reason]),
            {error, ?EACCES}
    end.


-spec add_webdav_specific_fields(od_user:id(), session:id(), storage_credentials(), helpers:helper()) ->
    {ok, luma:storage_credentials()} | {error, term()}.
add_webdav_specific_fields(UserId, SessionId, StorageCredentials = #{
    <<"credentialsType">> := <<"oauth2">>
}, Helper) ->
    {UserId2, SessionId2} = case fslogic_uuid:is_space_owner(UserId) of
        true ->
            % space owner uses helper admin_ctx
            {?ROOT_USER_ID, ?ROOT_SESS_ID};
        false ->
            {UserId, SessionId}
    end,
    fill_in_webdav_oauth2_token(UserId2, SessionId2, StorageCredentials, Helper);
add_webdav_specific_fields(_UserId, _SessionId, StorageCredentials, _Helper) ->
    {ok, StorageCredentials}.


-spec fill_in_webdav_oauth2_token(od_user:id(), session:id(), luma:storage_credentials(),
    helpers:helper()) -> {ok, luma:storage_credentials()} | {error, term()}.
fill_in_webdav_oauth2_token(UserId, SessionId, StorageCredentials, Helper = #helper{
    args = #{<<"oauth2IdP">> := OAuth2IdP}
}) ->
    fill_in_webdav_oauth2_token(UserId, SessionId, StorageCredentials, Helper, OAuth2IdP);
fill_in_webdav_oauth2_token(UserId, SessionId, UserCtx, Helper = #helper{}) ->
    % OAuth2IdP was not explicitly set, try to infer it
    case provider_logic:zone_get_offline_access_idps() of
        {ok, [OAuth2IdP]} ->
            fill_in_webdav_oauth2_token(UserId, SessionId, UserCtx, Helper, OAuth2IdP);
        {ok, []} ->
            ?error("Empty list of identity providers retrieved from Onezone"),
            {error, missing_identity_provider};
        {ok, _} ->
            ?error("Ambiguous list of identity providers retrieved from Onezone"),
            {error, ambiguous_identity_provider}
    end.


-spec fill_in_webdav_oauth2_token(od_user:id(), session:id(), luma:storage_credentials(),
    helpers:helper(), binary()) -> {ok, luma:storage_credentials()} | {error, term()}.
fill_in_webdav_oauth2_token(?ROOT_USER_ID, ?ROOT_SESS_ID, AdminCredentials = #{
    <<"onedataAccessToken">> := OnedataAccessToken,
    <<"adminId">> := AdminId
}, _Helper, OAuth2IdP) ->
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
fill_in_webdav_oauth2_token(UserId, SessionId, StorageCredentials, #helper{
    insecure = false
}, OAuth2IdP) ->
    {ok, {IdPAccessToken, TTL}} = idp_access_token:acquire(UserId, SessionId, OAuth2IdP),
    UserCtx2 = maps:remove(<<"onedataAccessToken">>, StorageCredentials),
    {ok, UserCtx2#{
        <<"accessToken">> => IdPAccessToken,
        <<"accessTokenTTL">> => integer_to_binary(TTL)
    }};
fill_in_webdav_oauth2_token(_UserId, _SessionId, AdminCredentials = #{
    <<"onedataAccessToken">> := OnedataAccessToken,
    <<"adminId">> := AdminId
}, #helper{insecure = true}, OAuth2IdP) ->
    TokenCredentials = auth_manager:build_token_credentials(
        OnedataAccessToken, undefined, undefined,
        undefined, disallow_data_access_caveats
    ),
    {ok, {IdPAccessToken, TTL}} = idp_access_token:acquire(AdminId, TokenCredentials, OAuth2IdP),
    AdminCtx2 = maps:remove(<<"onedataAccessToken">>, AdminCredentials),
    {ok, AdminCtx2#{
        <<"accessToken">> => IdPAccessToken,
        <<"accessTokenTTL">> => integer_to_binary(TTL)
    }}.

-spec map_space_owner_to_storage_credentials(storage:data(), od_space:id()) ->
    {ok, storage_credentials()} | {error, term()}.
map_space_owner_to_storage_credentials(Storage, SpaceId) ->
    case storage:is_posix_compatible(Storage) of
        true ->
            {ok, SpacePosixCredentials} = luma_spaces_cache:get(Storage, SpaceId),
            DefaultUid = luma_space:get_default_uid(SpacePosixCredentials),
            DefaultGid = luma_space:get_default_gid(SpacePosixCredentials),
            {ok, #{
                <<"uid">> => integer_to_binary(DefaultUid),
                <<"gid">> => integer_to_binary(DefaultGid)
            }};
        false ->
            Helper = storage:get_helper(Storage),
            {ok, helper:get_admin_ctx(Helper)}
    end.

-spec map_normal_user_to_storage_credentials(od_user:id(), storage:data(), od_space:id()) ->
    {ok, storage_credentials()} | {error, term()}.
map_normal_user_to_storage_credentials(UserId, Storage, SpaceId) ->
    case luma_users_cache:get(Storage, UserId) of
        {ok, LumaUser} ->
            StorageCredentials = luma_user:get_storage_credentials(LumaUser),
            case storage:is_posix_compatible(Storage) of
                true ->
                    {ok, SpacePosixCredentials} = luma_spaces_cache:get(Storage, SpaceId),
                    Uid = binary_to_integer(maps:get(<<"uid">>, StorageCredentials)),
                    % cache reverse mapping
                    luma_reverse_cache:cache_uid_mapping(Storage, Uid, UserId),
                    DefaultGid = luma_space:get_default_gid(SpacePosixCredentials),
                    {ok, StorageCredentials#{<<"gid">> => integer_to_binary(DefaultGid)}};
                false ->
                    {ok, StorageCredentials}
            end;
        Error ->
            Error
    end.

-spec map_space_owner_to_display_credentials(storage:id() | storage:data(), od_space:id()) ->
    {ok, display_credentials()}.
map_space_owner_to_display_credentials(Storage, SpaceId) ->
    {ok, SpacePosixCredentials} = luma_spaces_cache:get(Storage, SpaceId),
    DisplayUid = luma_space:get_display_uid(SpacePosixCredentials),
    DisplayGid = luma_space:get_display_gid(SpacePosixCredentials),
    {ok, {DisplayUid, DisplayGid}}.

-spec map_normal_user_to_display_credentials(od_user:id(), storage:id() | storage:data(), od_space:id()) ->
    {ok, display_credentials()} | {error, term()}.
map_normal_user_to_display_credentials(OwnerId, Storage, SpaceId) ->
    case luma_users_cache:get(Storage, OwnerId) of
        {ok, LumaUser} ->
            {ok, SpacePosixCredentials} = luma_spaces_cache:get(Storage, SpaceId),
            DisplayUid = luma_user:get_display_uid(LumaUser),
            DisplayGid = luma_space:get_display_gid(SpacePosixCredentials),
            {ok, {DisplayUid, DisplayGid}};
        Error ->
            Error
    end.

-spec is_reverse_luma_supported(storage:data()) -> boolean().
is_reverse_luma_supported(Storage) ->
    Helper = storage:get_helper(Storage),
    helper:is_posix_compatible(Helper).