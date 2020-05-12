%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% WRITEME jk
%%% TODO OPISAĆ LUMA - > local user mapping, entry module
%%% storage credentials, display credentials
%%% opisać jaki ma być priorytet
%%% @end
%%%-------------------------------------------------------------------
-module(luma).

-author("Krzysztof Trzepla").
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% LUMA
-export([
    map_to_storage_credentials/3,
    map_to_storage_credentials/4,
    map_to_display_credentials/3
]).

%% Reverse LUMA
-export([
    map_uid_to_onedata_user/3,
    map_acl_user_to_onedata_user/2,
    map_acl_group_to_onedata_group/2
]).

%% API functions
-export([invalidate/1, invalidate/2, add_helper_specific_fields/4]).

-type uid() :: non_neg_integer().
-type gid() :: non_neg_integer().
-type display_credentials() :: {uid(), gid()}.
-type storage_credentials() :: helper:user_ctx().
-type user_entry() :: external_luma:user_entry().
-type space_entry() :: external_luma:space_entry().

-type acl_who() :: binary().

-export_type([uid/0, gid/0, storage_credentials/0, display_credentials/0,
    space_entry/0, user_entry/0, acl_who/0]).


% TODO przekazywać storage czy storage id?
% TODO dodac force run synca

%todo error handling
%TODO ogarnac cache i jego inwalidacje na życzenie


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
map_to_display_credentials(OwnerId, SpaceId, StorageId) when is_binary(StorageId) ->
    {ok, Storage} = storage:get(StorageId),
    map_to_display_credentials(OwnerId, SpaceId, Storage);
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
%% If LUMA is disabled, function returns ?SPACE_OWNER_ID(SpaceId).
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
%% If LUMA is disabled, function returns error.
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
%% If LUMA is disabled, function returns error.
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
%%% API functions
%%%===================================================================

-spec invalidate(storage:id()) -> ok | {error, term()}.
invalidate(StorageId) ->
    luma_users_cache:delete(StorageId),
    luma_spaces_cache:delete(StorageId),
    luma_reverse_cache:delete(StorageId).

-spec invalidate(storage:id(), od_space:id()) -> ok | {error, term()}.
invalidate(StorageId, SpaceId) ->
    luma_spaces_cache:delete(StorageId, SpaceId).

-spec is_reverse_luma_supported(storage:data()) -> boolean().
is_reverse_luma_supported(Storage) ->
    Helper = storage:get_helper(Storage),
    helper:is_posix_compatible(Helper).

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
            true -> map_space_owner_to_storage_credentials(StorageData, SpaceId);
            false -> map_normal_user_to_storage_credentials(UserId, StorageData, SpaceId)
        end
    catch
        Error2:Reason ->
            ?error_stacktrace("luma:map_to_storage_credentials for user: ~p on storage: ~p failed with "
            "unexpected error ~p:~p.", [UserId, storage:get_id(Storage), Error2, Reason]),
            {error, ?EACCES}
    end.


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


% todo napisac w docku, ze to moze sie zdarzyc jak plik byl zaimportowany przez space_ownera albo usera jeszcze nie ma
-spec map_space_owner_to_storage_credentials(storage:data(), od_space:id()) ->
    {ok, storage_credentials()} | {error, term()}.
map_space_owner_to_storage_credentials(Storage, SpaceId) ->
    case storage:is_posix_compatible(Storage) of
        true ->
            % TODo handle error here, jaki zwracac blad?
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
                    % TODo handle error here, jaki zwracac blad?
                    {ok, SpacePosixCredentials} = luma_spaces_cache:get(Storage, SpaceId),
                    DefaultGid = luma_space:get_default_gid(SpacePosixCredentials),
                    {ok, StorageCredentials#{<<"gid">> => integer_to_binary(DefaultGid)}};
                false ->
                    {ok, StorageCredentials}
            end;
        Error ->
            Error
    end.

-spec map_space_owner_to_display_credentials(storage:data(), od_space:id()) -> {ok, display_credentials()}.
map_space_owner_to_display_credentials(Storage, SpaceId) ->
    % TODo handle error here, jaki zwracac blad?
    {ok, SpacePosixCredentials} = luma_spaces_cache:get(Storage, SpaceId),
    DisplayUid = luma_space:get_display_uid(SpacePosixCredentials),
    DisplayGid = luma_space:get_display_gid(SpacePosixCredentials),
    {ok, {DisplayUid, DisplayGid}}.

-spec map_normal_user_to_display_credentials(od_user:id(), storage:data(), od_space:id()) ->
    {ok, display_credentials()} | {error, term()}.
map_normal_user_to_display_credentials(OwnerId, Storage, SpaceId) ->
    case luma_users_cache:get(Storage, OwnerId) of
        {ok, LumaUser} ->
            % TODo handle error here, jaki zwracac blad?
            {ok, SpacePosixCredentials} = luma_spaces_cache:get(Storage, SpaceId),
            DisplayUid = luma_user:get_display_uid(LumaUser),
            DisplayGid = luma_space:get_display_gid(SpacePosixCredentials),
            {ok, {DisplayUid, DisplayGid}};
        Error ->
            Error
    end.