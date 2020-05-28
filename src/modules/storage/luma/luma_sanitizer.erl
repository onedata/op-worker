%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(luma_sanitizer).
-author("Jakub Kudzia").

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include("modules/storage/luma/luma.hrl").

%% API
-export([
    sanitize_storage_user/2,
    sanitize_posix_credentials/1,
    sanitize_onedata_user/1,
    sanitize_onedata_group/1
]).

-type storage_user() :: luma_storage_user:user_map().
-type onedata_user() :: luma_onedata_user:user_map().
-type onedata_group() :: luma_onedata_group:group_map().

%%%===================================================================
%%% API functions
%%%===================================================================

-spec sanitize_storage_user(json_utils:json_term(), helper:name()) ->
    {ok, storage_user()} | {error, term()}.
sanitize_storage_user(StorageUserMap, HelperName) when is_map(StorageUserMap) ->
    try
        {ok, middleware_sanitizer:sanitize_data(StorageUserMap, #{
            required => #{<<"storageCredentials">> => {json, storage_credentials_custom_constraint(HelperName)}},
            optional => #{<<"displayUid">> => {integer, {not_lower_than, 0}}}
        })}
    catch
        throw:Error ->
            ?error_stacktrace("Sanitizing LUMA storage user failed due to ~p", [Error]),
            Error
    end.

-spec sanitize_posix_credentials(json_utils:json_term()) ->
    {ok, luma_posix_credentials:credentials_map()} | {error, term()}.
sanitize_posix_credentials(PosixCredentials) ->
    try
        {ok, middleware_sanitizer:sanitize_data(PosixCredentials, #{
            optional => #{
                <<"uid">> => {integer, {not_lower_than, 0}},
                <<"gid">> => {integer, {not_lower_than, 0}}
            }
        })}
    catch
        throw:Error ->
            ?error_stacktrace("Sanitizing POSIX compatible credentials failed due to ~p", [Error]),
            Error
    end.

-spec sanitize_onedata_user(json_utils:json_term()) -> {ok, onedata_user()} | {error, term()}.
sanitize_onedata_user(OnedataUserMap) ->
    try
        sanitize_user_mapping_scheme(OnedataUserMap),
        SanitizedData = case maps:get(<<"mappingScheme">>, OnedataUserMap) of
            ?IDP_USER_SCHEME -> sanitize_idp_user_scheme(OnedataUserMap);
            ?ONEDATA_USER_SCHEME -> sanitize_onedata_user_scheme(OnedataUserMap)
        end,
        {ok, SanitizedData}
    catch
        throw:Error ->
            ?error_stacktrace("Sanitizing LUMA onedata user failed due to ~p: ~p", [Error, OnedataUserMap]),
            Error
    end.

-spec sanitize_onedata_group(json_utils:json_term()) -> {ok, onedata_group()} | {error, term()}.
sanitize_onedata_group(OnedataGroupMap) ->
    try
        sanitize_group_mapping_scheme(OnedataGroupMap),
        SanitizedData = case maps:get(<<"mappingScheme">>, OnedataGroupMap) of
            ?IDP_ENTITLEMENT_SCHEME -> sanitize_idp_group_scheme(OnedataGroupMap);
            ?ONEDATA_GROUP_SCHEME -> sanitize_onedata_group_scheme(OnedataGroupMap)
        end,
        {ok, SanitizedData}
    catch
        throw:Error ->
            ?error_stacktrace("Sanitizing LUMA onedata group failed due to ~p", [Error]),
            Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec storage_credentials_custom_constraint(helper:name()) ->
    fun((luma:storage_credentials()) -> {true, luma:storage_credentials()} | false).
storage_credentials_custom_constraint(HelperName) ->
    fun(StorageCredentials) ->
        % TODO VFS-6312 delete this case after using middleware_sanitizer in
        % helper_params:validate_user_ctx as it does not check
        % whether uid is non negative integer
        case helper:is_posix_compatible(HelperName) of
            true ->
                SanitizedCredentials =  sanitize_posix_storage_user_credentials(StorageCredentials),
                % storage credentials are passed to helper so we store them as binaries
                {true, integers_to_binary(SanitizedCredentials)};
            false ->
                % storage credentials are passed to helper so we store them as binaries
                StorageCredentialsBinaries = integers_to_binary(StorageCredentials),
                case helper_params:validate_user_ctx(HelperName, StorageCredentialsBinaries) of
                    ok -> {true, StorageCredentialsBinaries};
                    {error, _} -> false
                end
        end
    end.


-spec sanitize_posix_storage_user_credentials(luma:storage_credentials()) ->
    luma:storage_credentials().
sanitize_posix_storage_user_credentials(PosixCredentials) ->
    middleware_sanitizer:sanitize_data(PosixCredentials, #{
        required => #{<<"uid">> => {integer, {not_lower_than, 0}}}
    }).


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


-spec sanitize_user_mapping_scheme(onedata_user()) -> onedata_user().
sanitize_user_mapping_scheme(OnedataUserMap) ->
    middleware_sanitizer:sanitize_data(OnedataUserMap, #{
        required => #{<<"mappingScheme">> => {binary, [?ONEDATA_USER_SCHEME, ?IDP_USER_SCHEME]}}
    }).

-spec sanitize_group_mapping_scheme(onedata_group()) -> onedata_group().
sanitize_group_mapping_scheme(OnedataGroupMap) ->
    middleware_sanitizer:sanitize_data(OnedataGroupMap, #{
        required => #{<<"mappingScheme">> => {binary, [?ONEDATA_GROUP_SCHEME, ?IDP_ENTITLEMENT_SCHEME]}}
    }).


-spec sanitize_idp_user_scheme(onedata_user()) -> onedata_user().
sanitize_idp_user_scheme(OnedataUserMap) ->
    middleware_sanitizer:sanitize_data(OnedataUserMap, #{
        required => #{
            <<"mappingScheme">> => {binary, [?IDP_USER_SCHEME]},
            <<"idp">> => {binary, non_empty},
            <<"subjectId">> => {binary, non_empty}
        }
    }).


-spec sanitize_onedata_user_scheme(onedata_user()) -> onedata_user().
sanitize_onedata_user_scheme(Response) ->
    middleware_sanitizer:sanitize_data(Response, #{
        required => #{
            <<"mappingScheme">> => {binary, [?ONEDATA_USER_SCHEME]},
            <<"onedataUserId">> => {binary, non_empty}
        }
    }).


-spec sanitize_idp_group_scheme(onedata_group()) -> onedata_group().
sanitize_idp_group_scheme(Response) ->
    middleware_sanitizer:sanitize_data(Response, #{
        required => #{
            <<"mappingScheme">> => {binary, [?IDP_ENTITLEMENT_SCHEME]},
            <<"idp">> => {binary, non_empty},
            <<"idpEntitlement">> => {binary, non_empty}
        }
    }).


-spec sanitize_onedata_group_scheme(onedata_group()) -> onedata_group().
sanitize_onedata_group_scheme(Response) ->
    middleware_sanitizer:sanitize_data(Response, #{
        required => #{
            <<"mappingScheme">> => {binary, [?ONEDATA_GROUP_SCHEME]},
            <<"onedataGroupId">> => {binary, non_empty}
        }
    }).