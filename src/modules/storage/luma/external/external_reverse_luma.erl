%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for querying external, third party reverse
%%% LUMA service.
%%% @end
%%%-------------------------------------------------------------------
-module(external_reverse_luma).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/luma/external_luma.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/http/codes.hrl").

%% API
-export([
    map_uid_to_onedata_user/2,
    map_acl_user_to_onedata_user/2,
    map_acl_group_to_onedata_group/2
]).

%% User mapping response expected format is represented as a map:
%% #{
%%      <<"mappingScheme">> => ?ONEDATA_USER_SCHEME | ?IDP_USER_SCHEME,
%%
%%      % in case of ?ONEDATA_USER_SCHEME
%%      <<"onedataUserId">> => binary()
%%
%%      % in case of ?IDP_USER_SCHEME
%%      <<"idp">> => binary(),
%%      <<"subjectId">> => binary()
%% }
-type onedata_user() :: json_utils:json_map().

%% Group mapping response expected format is represented as a map:
%% #{
%%      <<"mappingScheme">> => ?ONEDATA_GROUP_SCHEME | ?IDP_ENTITLEMENT_SCHEME,
%%
%%      % in case of ?ONEDATA_GROUP_SCHEME
%%      <<"onedataGroupId">> => binary()
%%
%%      % in case of ?IDP_ENTITLEMENT_SCHEME
%%      <<"idp">> => binary(),
%%      <<"idpEntitlement">> => binary()
%% }
-type onedata_group() :: json_utils:json_map().


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Queries third party reverse LUMA service for id of user associated
%% with given Uid and Gid on storage named StorageId.
%% @end
%%--------------------------------------------------------------------
-spec map_uid_to_onedata_user(luma:uid(), storage:data()) ->
    {ok, onedata_user()} | {error, term()}.
map_uid_to_onedata_user(Uid, Storage) ->
    Body =  #{
        <<"uid">> => Uid,
        <<"storageId">> => storage:get_id(Storage)
    },
    case luma_utils:do_luma_request(?UID_TO_ONEDATA_USER_PATH, Body, Storage) of
        {ok, ?HTTP_200_OK, _RespHeaders, RespBody} ->
            sanitize_user_mapping(RespBody);
        {ok, ?HTTP_404_NOT_FOUND, _RespHeaders, _RespBody} ->
            {error, not_found};
        {ok, Code, _RespHeaders, RespBody} ->
            ?error("Mapping uid ~d on storage ~p failed.~n"
            "Request to external LUMA service returned code ~p and body ~p.",
                [Uid, storage:get_id(Storage), Code, json_utils:decode(RespBody)]),
            {error, external_luma_error};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Queries third party reverse LUMA service for id of user associated
%% with given Uid and Gid on storage named StorageId.
%% @end
%%--------------------------------------------------------------------
-spec map_acl_user_to_onedata_user(binary(), storage:data()) ->
    {ok, onedata_user()} | {error, term()}.
map_acl_user_to_onedata_user(AclUser, Storage) ->
    Body = #{
        <<"aclUser">> => AclUser,
        <<"storageId">> => storage:get_id(Storage)
    },
    case luma_utils:do_luma_request(?ACL_USER_TO_ONEDATA_USER_PATH, Body, Storage) of
        {ok, ?HTTP_200_OK, _RespHeaders, RespBody} ->
            sanitize_user_mapping(RespBody);
        {ok, ?HTTP_404_NOT_FOUND, _RespHeaders, _RespBody} ->
            {error, not_found};
        {ok, Code, _RespHeaders, RespBody} ->
            ?error("Mapping acl user ~s on storage ~p failed.~n"
            "Request to external LUMA service returned code ~p and body ~p.",
                [AclUser, storage:get_id(Storage), Code, json_utils:decode(RespBody)]),
            {error, external_luma_error};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Queries third party reverse LUMA service for id of group associated
%% with given Gid on storage named StorageId
%% @end
%%--------------------------------------------------------------------
-spec map_acl_group_to_onedata_group(binary(), storage:data()) ->
    {ok, onedata_group()} | {error, term()}.
map_acl_group_to_onedata_group(AclGroup, Storage) ->
    Body = #{
        <<"aclGroup">> => AclGroup,
        <<"storageId">> => storage:get_id(Storage)
    },
    case luma_utils:do_luma_request(?ACL_GROUP_TO_ONEDATA_GROUP_PATH, Body, Storage) of
        {ok, ?HTTP_200_OK, _RespHeaders, RespBody} ->
            sanitize_group_mapping(RespBody);
        {ok, ?HTTP_404_NOT_FOUND, _RespHeaders, _RespBody} ->
            {error, not_found};
        {ok, Code, _RespHeaders, RespBody} ->
            ?error("Mapping acl group ~s on storage ~p failed.~n"
            "Request to external LUMA service returned code ~p and body ~p.",
                [AclGroup, storage:get_id(Storage), Code, json_utils:decode(RespBody)]),
            {error, external_luma_error};
        {error, Reason} ->
            {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec sanitize_user_mapping(binary()) -> {ok, onedata_user()} | {error, term()}.
sanitize_user_mapping(Response) ->
    DecodedResponse = json_utils:decode(Response),
    try
        MappingScheme = maps:get(<<"mappingScheme">>, DecodedResponse),
        SanitizedData = case MappingScheme of
            ?IDP_USER_SCHEME -> sanitize_idp_user_scheme(DecodedResponse);
            ?ONEDATA_USER_SCHEME -> sanitize_onedata_user_scheme(DecodedResponse)
        end,
        {ok, SanitizedData#{<<"mappingScheme">> => MappingScheme}}
    catch
        Error:Reason ->
            ?error_stacktrace("Parsing external reverse LUMA user mapping response failed due to ~p:~p.",
                [Error, Reason]),
            {error, external_luma_error}
    end.

-spec sanitize_idp_user_scheme(onedata_user()) -> onedata_user().
sanitize_idp_user_scheme(Response) ->
    middleware_sanitizer:sanitize_data(Response, #{
       required => #{
           <<"idp">> => {binary, non_empty},
           <<"subjectId">> => {binary, non_empty}
       }
    }).


-spec sanitize_onedata_user_scheme(onedata_user()) -> onedata_user().
sanitize_onedata_user_scheme(Response) ->
    middleware_sanitizer:sanitize_data(Response, #{
        required => #{<<"onedataUserId">> => {binary, non_empty}}
    }).


-spec sanitize_group_mapping(binary()) -> {ok, onedata_group()} | {error, term()}.
sanitize_group_mapping(Response) ->
    DecodedResponse = json_utils:decode(Response),
    try
        MappingScheme = maps:get(<<"mappingScheme">>, DecodedResponse),
        SanitizedData = case MappingScheme of
            ?IDP_ENTITLEMENT_SCHEME -> sanitize_idp_group_scheme(DecodedResponse);
            ?ONEDATA_GROUP_SCHEME -> sanitize_onedata_group_scheme(DecodedResponse)
        end,
        {ok, SanitizedData#{<<"mappingScheme">> => MappingScheme}}
    catch
        Error:Reason ->
            ?error_stacktrace("Parsing external reverse LUMA group mapping response failed due to ~p:~p.",
                [Error, Reason]),
            {error, external_luma_error}
    end.


-spec sanitize_idp_group_scheme(onedata_group()) -> onedata_group().
sanitize_idp_group_scheme(Response) ->
    middleware_sanitizer:sanitize_data(Response, #{
        required => #{
            <<"idp">> => {binary, non_empty},
            <<"idpEntitlement">> => {binary, non_empty}
        }
    }).


-spec sanitize_onedata_group_scheme(onedata_group()) -> onedata_group().
sanitize_onedata_group_scheme(Response) ->
    middleware_sanitizer:sanitize_data(Response, #{
        required => #{<<"onedataGroupId">> => {binary, non_empty}}
    }).