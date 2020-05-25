%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for querying external, third party LUMA service
%%% for storage user context.
%%% @end
%%%-------------------------------------------------------------------
-module(external_luma).
-author("Krzysztof Trzepla").
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/luma/external_luma.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/http/codes.hrl").


%% API
-export([
    map_onedata_user_to_credentials/2,
    fetch_default_posix_credentials/2,
    fetch_default_display_credentials/2
]).

%% User mapping request body is represented as a map:
%% #{
%%     <<"storageId">> => storage:id(), // guaranteed
%%     <<"onedataUserId">> => od_user:id(),  // guaranteed
%%     <<"idpIdentities">> => [idp_identity()], // guaranteed
%%     <<"userDetails">> => additional_user_details() //optional
%% }.
-type user_mapping_request() :: json_utils:json_map().


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

%% Expected result of mapping onedata user to user credentials is
%% expressed in the form of the following map.
%% #{
%%     <<"storageCredentials">> => helpers:user_ctx(),
%%     <<"displayUid">> => integer() // optional
%% }
-type storage_user() :: json_utils:json_map().

%% Expected result of fetching default posix/default display credentials 
%% for space is expressed in the form of the following map.

%% #{
%%     <<"uid">> => non_neg_integer(),
%%     <<"gid">> => non_neg_integer()
%% }
-type posix_compatible_credentials() :: json_utils:json_map().

-export_type([storage_user/0, posix_compatible_credentials/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Queries third party LUMA service for the storage user context.
%% @end
%%--------------------------------------------------------------------
-spec map_onedata_user_to_credentials(od_user:id(), storage:data()) ->
    {ok, storage_user()} | {error, Reason :: term()}.
map_onedata_user_to_credentials(UserId, Storage) ->
    Body = prepare_user_request_body(UserId, Storage),
    case luma_utils:do_luma_request(?ONEDATA_USER_TO_CREDENTIALS_PATH, Body, Storage) of
        {ok, ?HTTP_200_OK, _RespHeaders, RespBody} ->
            sanitize_user_mapping_response(RespBody, Storage);
        {ok, ?HTTP_404_NOT_FOUND, _RespHeaders, RespBody} ->
            ?error("Mapping user ~p to storage credentials on storage ~p not found.~n"
            "Request to external LUMA service returned ~p.",
                [UserId, storage:get_id(Storage), json_utils:decode(RespBody)]),
            {error, not_found};
        {ok, Code, _RespHeaders, RespBody} ->
            ?error("Mapping user ~p to storage credentials on storage ~p failed .~n"
            "Request to external LUMA service returned code ~p and body ~p.",
                [UserId, storage:get_id(Storage), Code, json_utils:decode(RespBody)]),
            {error, external_luma_error};
        {error, Reason} ->
            ?error("Mapping user ~p to storage credentials on storage ~p failed .~n"
            "Unexpected error ~p.", [UserId, storage:get_id(Storage), Reason]),
            {error, external_luma_error}
    end.


-spec fetch_default_posix_credentials(od_space:id(), storage:data()) ->
    {ok, posix_compatible_credentials()} | {error, term()}.
fetch_default_posix_credentials(SpaceId, Storage) ->
    Body = #{
        <<"storageId">> => storage:get_id(Storage),
        <<"spaceId">> => SpaceId
    },
    case luma_utils:do_luma_request(?DEFAULT_POSIX_CREDENTIALS_PATH, Body, Storage) of
        {ok, ?HTTP_200_OK, _RespHeaders, RespBody} ->
            sanitize_space_defaults_response(RespBody);
        {ok, ?HTTP_404_NOT_FOUND, _RespHeaders, _RespBody} ->
            {error, not_found};
        {ok, Code, _RespHeaders, RespBody} ->
            ?error("Default storage credentials for storage ~p supporting space ~p could not be fetched.~n"
            "Request to external LUMA service returned code ~p and body ~p.",
                [storage:get_id(Storage), SpaceId, Code, json_utils:decode(RespBody)]),
            {error, external_luma_error};
        {error, Reason} ->
            ?error("Default storage credentials for storage ~p supporting space ~p could not be fetched.~n"
            "Unexpected error ~p.", [storage:get_id(Storage), SpaceId, Reason]),
            {error, external_luma_error}
    end.

-spec fetch_default_display_credentials(od_space:id(), storage:data()) ->
    {ok, posix_compatible_credentials()} | {error, term()}.
fetch_default_display_credentials(SpaceId, Storage) ->
    Body = #{
        <<"storageId">> => storage:get_id(Storage),
        <<"spaceId">> => SpaceId
    },
    case luma_utils:do_luma_request(?ONECLIENT_DISPLAY_OVERRIDE_PATH, Body, Storage) of
        {ok, ?HTTP_200_OK, _RespHeaders, RespBody} ->
            sanitize_space_defaults_response(RespBody);
        {ok, ?HTTP_404_NOT_FOUND, _RespHeaders, _RespBody} ->
            {error, not_found};
        {ok, Code, _RespHeaders, RespBody} ->
            ?error("Display credentials for storage ~p supporting space ~p could not be fetched.~n"
            "Request to external LUMA service returned code ~p and body ~p.",
                [storage:get_id(Storage), SpaceId, Code, json_utils:decode(RespBody)]),
            {error, external_luma_error};
        {error, Reason} ->
            ?error("Display credentials for storage ~p supporting space ~p could not be fetched.~n"
            "Unexpected error ~p.", [storage:get_id(Storage), SpaceId, Reason]),
            {error, external_luma_error}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs user context request that will be sent to the external LUMA service.
%% @end
%%--------------------------------------------------------------------
-spec prepare_user_request_body(od_user:id(), storage:data()) -> user_mapping_request().
prepare_user_request_body(UserId, Storage) ->
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


-spec sanitize_user_mapping_response(binary(), storage:data()) -> {ok, storage_user()} | {error, term()}.
sanitize_user_mapping_response(Response, Storage) ->
    try
        DecodedResponse = json_utils:decode(Response),
        UserEntry = middleware_sanitizer:sanitize_data(DecodedResponse, #{
            required => #{<<"storageCredentials">> => {json, storage_credentials_custom_constraint(Storage)}},
            optional => #{<<"displayUid">> => {integer, {not_lower_than, 0}}}
        }),
        {ok, UserEntry}
    catch
        Error:Reason ->
            ?error_stacktrace("Parsing external LUMA user mapping response failed due to ~p:~p.", [Error, Reason]),
            {error, external_luma_error}
    end.

-spec sanitize_space_defaults_response(binary()) -> {ok, posix_compatible_credentials()} | {error, term()}.
sanitize_space_defaults_response(Response) ->
    try
        DecodedResponse = json_utils:decode(Response),
        SpaceCredentials = middleware_sanitizer:sanitize_data(DecodedResponse, #{
            optional => #{
                <<"uid">> => {integer, {not_lower_than, 0}},
                <<"gid">> => {integer, {not_lower_than, 0}}
            }
        }),
        {ok, SpaceCredentials}
    catch
        Error:Reason ->
            ?error_stacktrace("Parsing external LUMA space mapping response failed due to ~p:~p.", [Error, Reason]),
            {error, external_luma_error}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Converts all integer values in a map to binary.
%% @end
%%-------------------------------------------------------------------
-spec integers_to_binary(map()) -> map().
integers_to_binary(Map) ->
    maps:map(fun
        (_, Value) when is_integer(Value) -> integer_to_binary(Value);
        (_, Value) -> Value
    end, Map).

-spec storage_credentials_custom_constraint(storage:data()) ->
    fun((luma:storage_credentials()) -> {true, luma:storage_credentials()} | false).
storage_credentials_custom_constraint(Storage) ->
    Helper = storage:get_helper(Storage),
    HelperName = helper:get_name(Helper),
    fun(StorageCredentials) ->
        % TODO VFS-6312 delete this case after using middleware_sanitizer in
        % helper_params:validate_user_ctx as it does not check
        % whether uid is non negative integer
        case helper:is_posix_compatible(HelperName) of
            true ->
                case sanitize_user_posix_credentials(StorageCredentials) of
                    {ok, SanitizedCredentials} ->
                        % storage credentials are passed to helper so we store them as binaries
                        {true, integers_to_binary(SanitizedCredentials)};
                    {error, _} ->
                        false
                end;
            false ->
                % storage credentials are passed to helper so we store them as binaries
                StorageCredentialsBinaries = integers_to_binary(StorageCredentials),
                case helper_params:validate_user_ctx(HelperName, StorageCredentialsBinaries) of
                    ok -> {true, StorageCredentialsBinaries};
                    {error, _} -> false
                end
        end
    end.

-spec sanitize_user_posix_credentials(luma:storage_credentials()) ->
    {ok, luma:storage_credentials()} | {error, term()}.
sanitize_user_posix_credentials(PosixCredentials) ->
    try
        SanitizedCredentials = middleware_sanitizer:sanitize_data(PosixCredentials, #{
            required => #{<<"uid">> => {integer, {not_lower_than, 0}}}
        }),
        {ok, SanitizedCredentials}
    catch
        throw:Error ->
            Error
    end.