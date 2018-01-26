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

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Queries third party reverse LUMA service for id of user associated
%% with given Uid and Gid on storage named StorageId.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(integer(), storage:id(), storage:name(), luma_config:config()) ->
    {ok, od_user:id()}.
get_user_id(Uid, StorageId, StorageName, LumaConfig = #luma_config{url = LumaUrl}) ->
    Url = str_utils:format_bin("~s/resolve_user", [LumaUrl]),
    ReqHeaders = luma_proxy:get_request_headers(LumaConfig),
    ReqBody = get_user_request_body(Uid, StorageId, StorageName),
    {ok, 200, _RespHeaders, RespBody} = http_client:post(Url, ReqHeaders, ReqBody),
    Response = json_utils:decode_map(RespBody),
    Idp = maps:get(<<"idp">>, Response),
    SubjectId = maps:get(<<"subjectId">>, Response),
    UserId = idp_to_onedata_user_id(Idp, SubjectId),
    {ok, UserId}.

%%--------------------------------------------------------------------
%% @doc
%% Queries third party reverse LUMA service for id of user associated
%% with given Uid and Gid on storage named StorageId.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id_by_name(binary(), storage:id(), storage:name(),
    luma_config:config()) -> {ok, od_user:id()}.
get_user_id_by_name(Name, StorageId, StorageName, LumaConfig = #luma_config{url = LumaUrl}) ->
    Url = str_utils:format_bin("~s/resolve_acl_user", [LumaUrl]),
    ReqHeaders = luma_proxy:get_request_headers(LumaConfig),
    ReqBody = get_user_request_body_by_name(Name, StorageId, StorageName),
    {ok, 200, _RespHeaders, RespBody} = http_client:post(Url, ReqHeaders, ReqBody),
    Response = json_utils:decode_map(RespBody),
    Idp = maps:get(<<"idp">>, Response),
    SubjectId = maps:get(<<"subjectId">>, Response),
    UserId = idp_to_onedata_user_id(Idp, SubjectId),
    {ok, UserId}.

%%--------------------------------------------------------------------
%% @doc
%% Queries third party reverse LUMA service for id of group associated
%% with given Gid on storage named StorageId
%% @end
%%--------------------------------------------------------------------
-spec get_group_id(integer(), od_space:id(), storage:id(), storage:name(),
    luma_config:config()) -> {ok, od_user:id()}.
get_group_id(Gid, SpaceId, StorageId, StorageName, LumaConfig = #luma_config{url = LumaUrl}) ->
    Url = str_utils:format_bin("~s/resolve_group", [LumaUrl]),
    ReqHeaders = luma_proxy:get_request_headers(LumaConfig),
    ReqBody = get_group_request_body(Gid, SpaceId, StorageId, StorageName),
    {ok, 200, _RespHeaders, RespBody} = http_client:post(Url, ReqHeaders, ReqBody),
    Response = json_utils:decode_map(RespBody),
    Idp = maps:get(<<"idp">>, Response),
    IdpGroupId = maps:get(<<"groupId">>, Response),
    case Idp of
        <<"onedata">> ->
            {ok, IdpGroupId};
        _ ->
            provider_logic:map_idp_group_to_onedata(Idp, IdpGroupId)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Queries third party reverse LUMA service for id of group associated
%% with given Gid on storage named StorageId
%% @end
%%--------------------------------------------------------------------
-spec get_group_id_by_name(binary(), od_space:id(), storage:id(), storage:name(),
    luma_config:config()) -> {ok, od_user:id()}.
get_group_id_by_name(Name, SpaceId, StorageId, StorageName,
    LumaConfig = #luma_config{url = LumaUrl}
) ->
    Url = str_utils:format_bin("~s/resolve_acl_group", [LumaUrl]),
    ReqHeaders = luma_proxy:get_request_headers(LumaConfig),
    ReqBody = get_group_request_body_by_name(Name, SpaceId, StorageId, StorageName),
    {ok, 200, _RespHeaders, RespBody} = http_client:post(Url, ReqHeaders, ReqBody),
    Response = json_utils:decode_map(RespBody),
    Idp = maps:get(<<"idp">>, Response),
    IdpGroupId = maps:get(<<"groupId">>, Response),
    case Idp of
        <<"onedata">> ->
            {ok, IdpGroupId};
        _ ->
            provider_logic:map_idp_group_to_onedata(Idp, IdpGroupId)
    end.

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
    datastore_utils:gen_key(<<"">>, str_utils:format_bin("~p:~s",
        [Idp, IdpUserId]
    )).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable
%% by given storage.
%% @end
%%-------------------------------------------------------------------
-spec get_group_request_body(integer(), od_space:id(), storage:id(),
    storage:name()) -> binary().
get_group_request_body(Gid, SpaceId, StorageId, StorageName) ->
    Body = get_posix_generic_request_body_part(StorageId, StorageName),
    json_utils:encode_map(Body#{
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
-spec get_group_request_body_by_name(binary(), od_space:id(), storage:id(),
    storage:name()) -> binary().
get_group_request_body_by_name(Name, SpaceId, StorageId, StorageName) ->
    Body = get_posix_generic_request_body_part(StorageId, StorageName),
    json_utils:encode_map(Body#{
        <<"spaceId">> => SpaceId,
        <<"name">> => Name
    }).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Prepares request body for resolving user uid.
%% @end
%%-------------------------------------------------------------------
-spec get_user_request_body(integer(), storage:id(), helper:name()) -> binary().
get_user_request_body(Uid, StorageId, StorageName) ->
    Body = get_posix_generic_request_body_part(StorageId, StorageName),
    json_utils:encode_map(Body#{<<"uid">> => Uid}).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Prepares request body for resolving user aclname.
%% @end
%%-------------------------------------------------------------------
-spec get_user_request_body_by_name(binary(), storage:id(), storage:name()) -> binary().
get_user_request_body_by_name(Name, StorageId, StorageName) ->
    Body = get_posix_generic_request_body_part(StorageId, StorageName),
    json_utils:encode_map(Body#{<<"name">> => Name}).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns generic arguments for given POSIX storage in request body.
%% @end
%%-------------------------------------------------------------------
-spec get_posix_generic_request_body_part(storage:id(), storage:name()) -> map().
get_posix_generic_request_body_part(StorageId, StorageName) ->
    #{
        <<"storageId">> => StorageId,
        <<"storageName">> => StorageName,
        <<"type">> => <<"posix">>
    }.