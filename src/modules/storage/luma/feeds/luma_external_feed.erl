%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions for acquiring mappings for LUMA DB
%%% from external feed.
%%% WARNING!!!
%%% Functions from this module shall be called only internally by luma_db
%%% module to populate the database.
%%% DO NOT USE IT TO FETCH MAPPINGS!!!
%%% @end
%%%-------------------------------------------------------------------
-module(luma_external_feed).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/luma/luma_external_feed.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/http/codes.hrl").

%% Onedata to storage API
-export([
    map_onedata_user_to_credentials/2,
    fetch_default_posix_credentials/2,
    fetch_default_display_credentials/2
]).

%% storage to Onedata API
-export([
    map_uid_to_onedata_user/2,
    map_acl_user_to_onedata_user/2,
    map_acl_group_to_onedata_group/2
]).

%% exported for mocking in CT tests
-export([http_client_post/3]).

%% Onedata user mapping request body is represented as a map:
%% #{
%%     <<"storageId">> => storage:id(), // guaranteed
%%     <<"onedataUserId">> => od_user:id(),  // guaranteed
%%     <<"idpIdentities">> => [idp_identity()], // guaranteed
%%     <<"userDetails">> => additional_user_details() //optional
%% }.
-type onedata_user_mapping_request_body() :: json_utils:json_map().


%% Idp identity is represented as a map:
%% #{
%%     <<"idp">> => binary(),
%%     <<"subjectId">> => binary()
%% }
-type idp_identity() ::json_utils:json_map().

%% Additional user details are represented as a map:
%% #{
%%     <<"id">> => od_user:id(),
%%     <<"username">> => binary()
%%     <<"emails">> => [binary()]
%%     <<"linkedAccounts">> => [od_user:linked_account()]
%% }
-type additional_user_details() :: json_utils:json_map().

-type storage_user() :: luma_storage_user:user_map().
-type onedata_user() :: luma_onedata_user:user_map().
-type onedata_group() :: luma_onedata_group:group_map().

%%%===================================================================
%%% API functions for mapping Onedata entities to storage entities
%%%===================================================================

-spec map_onedata_user_to_credentials(od_user:id(), storage:data()) ->
    {ok, storage_user()} | {error, Reason :: term()}.
map_onedata_user_to_credentials(UserId, Storage) ->
    Body = prepare_onedata_user_mapping_request_body(UserId, Storage),
    case do_request(?ONEDATA_USER_TO_CREDENTIALS_PATH, Body, Storage) of
        {ok, ?HTTP_200_OK, _RespHeaders, RespBody} ->
            HelperName = storage:get_helper_name(Storage),
            case luma_sanitizer:sanitize_storage_user(RespBody, HelperName) of
                {ok, StorageUser} -> {ok, StorageUser};
                {error, _} -> {error, luma_external_feed_error}
            end;
        {ok, Code = ?HTTP_404_NOT_FOUND, _RespHeaders, RespBody} ->
            log_http_error(
                "Mapping user ~tp to storage credentials on storage ~tp not found.",
                [UserId, storage:get_id(Storage)], Code, RespBody),
            {error, not_found};
        {ok, Code, _RespHeaders, RespBody} ->
            log_http_error(
                "Mapping user ~tp to storage credentials on storage ~tp failed.",
                [UserId, storage:get_id(Storage)], Code, RespBody
            ),
            {error, luma_external_feed_error};
        {error, Reason} ->
            log_unexpected_error(
                "Mapping user ~tp to storage credentials on storage ~tp failed.",
                [UserId, storage:get_id(Storage)], Reason),
            {error, luma_external_feed_error}
    end.


-spec fetch_default_posix_credentials(od_space:id(), storage:data()) ->
    {ok, luma_posix_credentials:credentials_map()} | {error, term()}.
fetch_default_posix_credentials(SpaceId, Storage) ->
    Body = #{
        <<"storageId">> => storage:get_id(Storage),
        <<"spaceId">> => SpaceId
    },
    case do_request(?DEFAULT_POSIX_CREDENTIALS_PATH, Body, Storage) of
        {ok, ?HTTP_200_OK, _RespHeaders, RespBody} ->
            case luma_sanitizer:sanitize_posix_credentials(RespBody) of
                {ok, PosixCredentials} -> {ok, PosixCredentials};
                {error, _} -> {error, luma_external_feed_error}
            end;
        {ok, ?HTTP_404_NOT_FOUND, _RespHeaders, _RespBody} ->
            {error, not_found};
        {ok, Code, _RespHeaders, RespBody} ->
            log_http_error(
                "Default storage credentials for storage ~tp supporting space ~tp could not be fetched.",
                [storage:get_id(Storage), SpaceId], Code, RespBody
            ),
            {error, luma_external_feed_error};
        {error, Reason} ->
            log_unexpected_error(
                "Default storage credentials for storage ~tp supporting space ~tp could not be fetched.",
                [storage:get_id(Storage), SpaceId], Reason
            ),
            {error, luma_external_feed_error}
    end.


-spec fetch_default_display_credentials(od_space:id(), storage:data()) ->
    {ok, luma_posix_credentials:credentials_map()} | {error, term()}.
fetch_default_display_credentials(SpaceId, Storage) ->
    Body = #{
        <<"storageId">> => storage:get_id(Storage),
        <<"spaceId">> => SpaceId
    },
    case do_request(?DISPLAY_CREDENTIALS_PATH, Body, Storage) of
        {ok, ?HTTP_200_OK, _RespHeaders, RespBody} ->
            case luma_sanitizer:sanitize_posix_credentials(RespBody) of
                {ok, PosixCredentials} -> {ok, PosixCredentials};
                {error, _} -> {error, luma_external_feed_error}
            end;
        {ok, ?HTTP_404_NOT_FOUND, _RespHeaders, _RespBody} ->
            {error, not_found};
        {ok, Code, _RespHeaders, RespBody} ->
            log_http_error(
                "Display credentials for storage ~tp supporting space ~tp could not be fetched.",
                [storage:get_id(Storage), SpaceId], Code, RespBody
            ),
            {error, luma_external_feed_error};
        {error, Reason} ->
            log_unexpected_error(
                "Display credentials for storage ~tp supporting space ~tp could not be fetched.",
                [storage:get_id(Storage), SpaceId], Reason),
            {error, luma_external_feed_error}
    end.


%%%===================================================================
%%% API functions for mapping storage entities to Onedata entities
%%%===================================================================

-spec map_uid_to_onedata_user(luma:uid(), storage:data()) ->
    {ok, onedata_user()} | {error, term()}.
map_uid_to_onedata_user(Uid, Storage) ->
    Body =  #{
        <<"uid">> => Uid,
        <<"storageId">> => storage:get_id(Storage)
    },
    case do_request(?UID_TO_ONEDATA_USER_PATH, Body, Storage) of
        {ok, ?HTTP_200_OK, _RespHeaders, RespBody} ->
            case luma_sanitizer:sanitize_onedata_user(RespBody) of
                {ok, OnedataUserMap} -> {ok, OnedataUserMap};
                {error, _} -> {error, luma_external_feed_error}
            end;
        {ok, ?HTTP_404_NOT_FOUND, _RespHeaders, _RespBody} ->
            {error, not_found};
        {ok, Code, _RespHeaders, RespBody} ->
            log_http_error(
                "Mapping uid ~d on storage ~tp failed.",
                [Uid, storage:get_id(Storage)], Code, RespBody
            ),
            {error, luma_external_feed_error};
        {error, Reason} ->
            log_unexpected_error(
                "Mapping uid ~d on storage ~tp failed.",
                [Uid, storage:get_id(Storage)], Reason
            ),
            {error, Reason}
    end.


-spec map_acl_user_to_onedata_user(binary(), storage:data()) ->
    {ok, onedata_user()} | {error, term()}.
map_acl_user_to_onedata_user(AclUser, Storage) ->
    Body = #{
        <<"aclUser">> => AclUser,
        <<"storageId">> => storage:get_id(Storage)
    },
    case do_request(?ACL_USER_TO_ONEDATA_USER_PATH, Body, Storage) of
        {ok, ?HTTP_200_OK, _RespHeaders, RespBody} ->
            case luma_sanitizer:sanitize_onedata_user(RespBody) of
                {ok, OnedataUserMap} -> {ok, OnedataUserMap};
                {error, _} -> {error, luma_external_feed_error}
            end;
        {ok, ?HTTP_404_NOT_FOUND, _RespHeaders, _RespBody} ->
            {error, not_found};
        {ok, Code, _RespHeaders, RespBody} ->
            log_http_error(
                "Mapping acl user ~ts on storage ~tp failed.",
                [AclUser, storage:get_id(Storage)], Code, RespBody
            ),
            {error, luma_external_feed_error};
        {error, Reason} ->
            log_unexpected_error(
                "Mapping acl user ~ts on storage ~tp failed.",
                [AclUser, storage:get_id(Storage)], Reason
            ),
            {error, Reason}
    end.

-spec map_acl_group_to_onedata_group(binary(), storage:data()) ->
    {ok, onedata_group()} | {error, term()}.
map_acl_group_to_onedata_group(AclGroup, Storage) ->
    Body = #{
        <<"aclGroup">> => AclGroup,
        <<"storageId">> => storage:get_id(Storage)
    },
    case do_request(?ACL_GROUP_TO_ONEDATA_GROUP_PATH, Body, Storage) of
        {ok, ?HTTP_200_OK, _RespHeaders, RespBody} ->
            case luma_sanitizer:sanitize_onedata_group(RespBody) of
                {ok, OnedataGroupMap} -> {ok, OnedataGroupMap};
                {error, _} -> {error, luma_external_feed_error}
            end;
        {ok, ?HTTP_404_NOT_FOUND, _RespHeaders, _RespBody} ->
            {error, not_found};
        {ok, Code, _RespHeaders, RespBody} ->
            log_http_error(
                "Mapping acl group ~ts on storage ~tp failed.",
                [AclGroup, storage:get_id(Storage)], Code, RespBody
            ),
            {error, luma_external_feed_error};
        {error, Reason} ->
            log_unexpected_error(
                "Mapping acl group ~ts on storage ~tp failed.",
                [AclGroup, storage:get_id(Storage)], Reason
            ),
            {error, Reason}
    end.

%%%===================================================================
%%% Exported for CT tests
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Simple wrapper for http_client:post.
%% This function is used to avoid mocking http_client in tests.
%% Mocking http_client is dangerous because meck's reloading and
%% purging the module can result in node_manager being killed.
%%-------------------------------------------------------------------
-spec http_client_post(http_client:url(), http_client:headers(),
    http_client:body()) -> http_client:response().
http_client_post(Url, ReqHeaders, ReqBody) ->
    http_client:post(Url, ReqHeaders, ReqBody).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec do_request(binary(), map(), storage:data()) ->
    {ok, http_client:code(), http_client:headers(), json_utils:json_term()} |
    {error, term()}.
do_request(Endpoint, ReqBody, Storage) ->
    LumaConfig = storage:get_luma_config(Storage),
    Url = ?LUMA_URL(LumaConfig, Endpoint),
    ReqHeaders = prepare_request_headers(LumaConfig),
    EncodedRequestBody = json_utils:encode(ReqBody),
    case luma_external_feed:http_client_post(Url, ReqHeaders, EncodedRequestBody) of
        {ok, Code, Headers, EncodedRespBody} ->
            case decode_body(EncodedRespBody) of
                {ok, RespBody} ->
                    {ok, Code, Headers, RespBody};
                {error, _} = Error ->
                    Error
            end;
        Other ->
            Other
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns LUMA request headers based on LumaConfig.
%% @end
%%-------------------------------------------------------------------
-spec prepare_request_headers(luma_config:config()) -> map().
prepare_request_headers(LumaConfig) ->
    case luma_config:get_api_key(LumaConfig) of
        undefined ->
            #{?HDR_CONTENT_TYPE => <<"application/json">>};
        APIKey ->
            #{
                ?HDR_CONTENT_TYPE => <<"application/json">>,
                ?HDR_X_AUTH_TOKEN => APIKey
            }
    end.

-spec decode_body(binary()) -> {ok, json_utils:json_term()} | {error, term()}.
decode_body(EncodedBody) ->
    try
        {ok, json_utils:decode(EncodedBody)}
    catch
        _:invalid_json ->
            ?ERROR_MALFORMED_DATA
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs user context request that will be sent to the external
%% LUMA feed to acquire mapping of Onedata user to storage user.
%% @end
%%--------------------------------------------------------------------
-spec prepare_onedata_user_mapping_request_body(od_user:id(), storage:data()) ->
    onedata_user_mapping_request_body().
prepare_onedata_user_mapping_request_body(UserId, Storage) ->
    StorageId = storage:get_id(Storage),
    AdditionalUserDetails = get_additional_user_details(UserId),
    #{
        <<"storageId">> => StorageId,
        <<"onedataUserId">> => UserId,
        <<"idpIdentities">> => get_idp_identities(AdditionalUserDetails),
        <<"userDetails">> => AdditionalUserDetails
    }.


-spec get_additional_user_details(od_user:id()) -> additional_user_details().
get_additional_user_details(UserId) ->
    case user_logic:get_protected_data(?ROOT_SESS_ID, UserId) of
        {ok, #document{key = UserId, value = User}} ->
            #{
                <<"id">> => UserId,
                <<"username">> => utils:undefined_to_null(User#od_user.username),
                <<"emails">> => User#od_user.emails,
                <<"linkedAccounts">> => User#od_user.linked_accounts
            };
        {error, _} ->
            #{}
    end.

-spec get_idp_identities(additional_user_details()) -> [idp_identity()].
get_idp_identities(AdditionalUserDetails) ->
    lists:map(fun(LinkedAccount) ->
        maps:with([<<"idp">>, <<"subjectId">>], LinkedAccount)
    end, maps:get(<<"linkedAccounts">>, AdditionalUserDetails, [])).

-spec log_http_error(string(), [term()], http_client:code(), json_utils:json_term()) -> ok.
log_http_error(Format, Args, Code, ResponseBody) ->
    ?error(
        Format ++ "~nRequest to external LUMA DB feed returned error code ~tp and body ~tp.",
        Args ++ [Code, ResponseBody]
    ).

-spec log_unexpected_error(string(), [term()], term()) -> ok.
log_unexpected_error(Format, Args, Reason) ->
    ?error(
        Format ++ "~nUnexpected error ~tp occurred on request to external LUMA DB feed.",
        Args ++ [Reason]
    ).
