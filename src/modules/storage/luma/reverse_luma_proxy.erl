%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for querying external, third party reverse
%%% LUMA service for storage user context.
%%% @end
%%%-------------------------------------------------------------------
-module(reverse_luma_proxy).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_user_id/4, get_user_id_by_name/4,
    get_group_id/5, get_group_id_by_name/5]).

%% exported for test reasons
-export([http_client_post/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Queries third party reverse LUMA service for id of user associated
%% with given Uid and Gid on storage named StorageId.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(integer(), od_storage:id(), storage_config:name(), luma_config:config()) ->
    {ok, od_user:id()} | {error, term()}.
get_user_id(Uid, StorageId, StorageName, LumaConfig = #luma_config{url = LumaUrl}) ->
    Url = str_utils:format_bin("~s/resolve_user", [LumaUrl]),
    ReqHeaders = luma_proxy:get_request_headers(LumaConfig),
    ReqBody = get_user_request_body(Uid, StorageId, StorageName),
    case ?MODULE:http_client_post(Url, ReqHeaders, ReqBody) of
        {ok, 200, _RespHeaders, RespBody} ->
            Response = json_utils:decode(RespBody),
            Idp = maps:get(<<"idp">>, Response),
            SubjectId = maps:get(<<"subjectId">>, Response),
            UserId = idp_to_onedata_user_id(Idp, SubjectId),
            {ok, UserId};
        {ok, Code, _RespHeaders, RespBody} ->
            {error, {Code, json_utils:decode(RespBody)}};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Queries third party reverse LUMA service for id of user associated
%% with given Uid and Gid on storage named StorageId.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id_by_name(binary(), od_storage:id(), storage_config:name(),
    luma_config:config()) -> {ok, od_user:id()} | {error, term()}.
get_user_id_by_name(Name, StorageId, StorageName, LumaConfig = #luma_config{url = LumaUrl}) ->
    Url = str_utils:format_bin("~s/resolve_acl_user", [LumaUrl]),
    ReqHeaders = luma_proxy:get_request_headers(LumaConfig),
    ReqBody = get_user_request_body_by_name(Name, StorageId, StorageName),
    case ?MODULE:http_client_post(Url, ReqHeaders, ReqBody) of
        {ok, 200, _RespHeaders, RespBody} ->
            Response = json_utils:decode(RespBody),
            Idp = maps:get(<<"idp">>, Response),
            SubjectId = maps:get(<<"subjectId">>, Response),
            UserId = idp_to_onedata_user_id(Idp, SubjectId),
            {ok, UserId};
        {ok, Code, _RespHeaders, RespBody} ->
            {error, {Code, json_utils:decode(RespBody)}};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Queries third party reverse LUMA service for id of group associated
%% with given Gid on storage named StorageId
%% @end
%%--------------------------------------------------------------------
-spec get_group_id(integer(), od_space:id(), od_storage:id(), storage_config:name(),
    luma_config:config()) -> {ok, od_user:id()} |  {error, term()}.
get_group_id(Gid, SpaceId, StorageId, StorageName, LumaConfig = #luma_config{url = LumaUrl}) ->
    Url = str_utils:format_bin("~s/resolve_group", [LumaUrl]),
    ReqHeaders = luma_proxy:get_request_headers(LumaConfig),
    ReqBody = get_group_request_body(Gid, SpaceId, StorageId, StorageName),
    case ?MODULE:http_client_post(Url, ReqHeaders, ReqBody) of
        {ok, 200, _RespHeaders, RespBody} ->
            Response = json_utils:decode(RespBody),
            Idp = maps:get(<<"idp">>, Response),
            IdpGroupId = maps:get(<<"groupId">>, Response),
            case Idp of
                <<"onedata">> ->
                    {ok, IdpGroupId};
                _ ->
                    provider_logic:map_idp_group_to_onedata(Idp, IdpGroupId)
            end;
        {ok, Code, _RespHeaders, RespBody} ->
            {error, {Code, json_utils:decode(RespBody)}};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Queries third party reverse LUMA service for id of group associated
%% with given Gid on storage named StorageId
%% @end
%%--------------------------------------------------------------------
-spec get_group_id_by_name(binary(), od_space:id(), od_storage:id(), storage_config:name(),
    luma_config:config()) -> {ok, od_user:id()} |  {error, term()}.
get_group_id_by_name(Name, SpaceId, StorageId, StorageName,
    LumaConfig = #luma_config{url = LumaUrl}
) ->
    Url = str_utils:format_bin("~s/resolve_acl_group", [LumaUrl]),
    ReqHeaders = luma_proxy:get_request_headers(LumaConfig),
    ReqBody = get_group_request_body_by_name(Name, SpaceId, StorageId, StorageName),
    case ?MODULE:http_client_post(Url, ReqHeaders, ReqBody) of
        {ok, 200, _RespHeaders, RespBody} ->
            Response = json_utils:decode(RespBody),
            Idp = maps:get(<<"idp">>, Response),
            IdpGroupId = maps:get(<<"groupId">>, Response),
            case Idp of
                <<"onedata">> ->
                    {ok, IdpGroupId};
                _ ->
                    provider_logic:map_idp_group_to_onedata(Idp, IdpGroupId)
            end;
        {ok, Code, _RespHeaders, RespBody} ->
            {error, {Code, json_utils:decode(RespBody)}};
        {error, Reason} ->
            {error, Reason}
    end.

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

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Converts idp and user id from idp to onedata user id.
%% @end
%%-------------------------------------------------------------------
-spec idp_to_onedata_user_id(binary(), binary()) -> od_user:id().
idp_to_onedata_user_id(<<"onedata">>, IdpUserId) ->
    IdpUserId;
idp_to_onedata_user_id(Idp, IdpUserId) ->
    % NOTE: legacy key generation must always be used to ensure that user
    % mappings are not lost after system upgrade from version pre 19.02.1
    datastore_key:gen_legacy_key(<<"">>, str_utils:format_bin("~ts:~s", [Idp, IdpUserId])).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable
%% by given storage.
%% @end
%%-------------------------------------------------------------------
-spec get_group_request_body(integer(), od_space:id(), od_storage:id(),
    storage_config:name()) -> binary().
get_group_request_body(Gid, SpaceId, StorageId, StorageName) ->
    Body = get_posix_generic_request_body_part(StorageId, StorageName),
    json_utils:encode(Body#{
        <<"spaceId">> => SpaceId,
        <<"gid">> => Gid
    }).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable
%% by given storage.
%% @end
%%-------------------------------------------------------------------
-spec get_group_request_body_by_name(binary(), od_space:id(), od_storage:id(),
    storage_config:name()) -> binary().
get_group_request_body_by_name(Name, SpaceId, StorageId, StorageName) ->
    Body = get_posix_generic_request_body_part(StorageId, StorageName),
    json_utils:encode(Body#{
        <<"spaceId">> => SpaceId,
        <<"name">> => Name
    }).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Prepares request body for resolving user uid.
%% @end
%%-------------------------------------------------------------------
-spec get_user_request_body(integer(), od_storage:id(), helper:name()) -> binary().
get_user_request_body(Uid, StorageId, StorageName) ->
    Body = get_posix_generic_request_body_part(StorageId, StorageName),
    json_utils:encode(Body#{<<"uid">> => Uid}).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Prepares request body for resolving user aclname.
%% @end
%%-------------------------------------------------------------------
-spec get_user_request_body_by_name(binary(), od_storage:id(), storage_config:name()) -> binary().
get_user_request_body_by_name(Name, StorageId, StorageName) ->
    Body = get_posix_generic_request_body_part(StorageId, StorageName),
    json_utils:encode(Body#{<<"name">> => Name}).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns generic arguments for given POSIX storage in request body.
%% @end
%%-------------------------------------------------------------------
-spec get_posix_generic_request_body_part(od_storage:id(), storage_config:name()) -> map().
get_posix_generic_request_body_part(StorageId, StorageName) ->
    #{
        <<"storageId">> => StorageId,
        <<"storageName">> => StorageName,
        <<"type">> => <<"posix">>
    }.
