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


%% API
-export([get_user_id/5, get_group_id/4]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Queries third party reverse LUMA service for id of user associated
%% with given Uid and Gid on storage named StorageName.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(binary(), binary(), storage:name(), helper:name(),
    luma_config:config()) -> {ok, od_user:id()}.
get_user_id(Uid, Gid, StorageName, HelperName, LumaConfig = #luma_config{url = LumaUrl}) ->
    Url = lists:flatten(io_lib:format("~s/resolve_user_identity", [LumaUrl])),
    ReqHeaders = luma_proxy:get_request_headers(LumaConfig),
    ReqBody = get_user_request_body(Uid, Gid, StorageName, HelperName),
    {ok, 200, _RespHeaders, RespBody} = http_client:post(Url, ReqHeaders, ReqBody),
    Response = json_utils:decode_map(RespBody),
    ProviderId = maps:get(<<"providerId">>, Response),
    ProviderUserId = maps:get(<<"userId">>, Response),
    UserId = datastore_utils2:gen_key(<<"">>, str_utils:format_bin("~p:~s",
        [ProviderId, ProviderUserId])),
    {ok, UserId}.

%%--------------------------------------------------------------------
%% @doc
%% Queries third party reverse LUMA service for id of group associated
%% with given Gid on storage named StorageName
%% @end
%%--------------------------------------------------------------------
-spec get_group_id(binary(), storage:name(), helper:name(),
    luma_config:config()) -> {ok, od_user:id()}.
get_group_id(Gid, StorageName, HelperName, LumaConfig = #luma_config{url = LumaUrl}) ->
    Url = lists:flatten(io_lib:format("~s/resolve_group_identity", [LumaUrl])),
    ReqHeaders = luma_proxy:get_request_headers(LumaConfig),
    ReqBody = get_group_request_body(Gid, StorageName, HelperName),
    {ok, 200, _RespHeaders, RespBody} = http_client:post(Url, ReqHeaders, ReqBody),
    Response = json_utils:decode_map(RespBody),
    ProviderId = maps:get(<<"providerId">>, Response),
    ProviderGroupId = maps:get(<<"groupId">>, Response),
    UserId = datastore_utils2:gen_key(<<"">>, str_utils:format_bin("~p:~s",
        [ProviderId, ProviderGroupId])),
    {ok, UserId}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable
%% by given storage.
%% @end
%%-------------------------------------------------------------------
-spec get_group_request_body(binary(), storage:name(), helper:name()) -> binary().
get_group_request_body(Gid, StorageName, ?POSIX_HELPER_NAME) ->
    json_utils:encode_map(get_posix_group_request_body(Gid, StorageName));
get_group_request_body(Gid, StorageName, ?CEPH_HELPER_NAME) ->
    json_utils:encode_map(get_ceph_group_request_body(Gid, StorageName));
get_group_request_body(Gid, StorageName, ?S3_HELPER_NAME) ->
    json_utils:encode_map(get_s3_group_request_body(Gid, StorageName));
get_group_request_body(Gid, StorageName, ?SWIFT_HELPER_NAME) ->
    json_utils:encode_map(get_swift_group_request_body(Gid, StorageName));
get_group_request_body(Gid, StorageName, ?GLUSTERFS_HELPER_NAME) ->
    json_utils:encode_map(get_glusterfs_group_request_body(Gid, StorageName)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by given storage.
%% @end
%%-------------------------------------------------------------------
-spec get_user_request_body(binary(), binary(), storage:name(), helper:name()) -> binary().
get_user_request_body(Uid, Gid, StorageName, ?POSIX_HELPER_NAME) ->
    json_utils:encode_map(get_posix_user_request_body(Uid, Gid, StorageName));
get_user_request_body(Uid, Gid, StorageName, ?CEPH_HELPER_NAME) ->
    json_utils:encode_map(get_ceph_user_request_body(Uid, Gid, StorageName));
get_user_request_body(Uid, Gid, StorageName, ?S3_HELPER_NAME) ->
    json_utils:encode_map(get_s3_user_request_body(Uid, Gid, StorageName));
get_user_request_body(Uid, Gid, StorageName, ?SWIFT_HELPER_NAME) ->
    json_utils:encode_map(get_swift_user_request_body(Uid, Gid, StorageName));
get_user_request_body(Uid, Gid, StorageName, ?GLUSTERFS_HELPER_NAME) ->
    json_utils:encode_map(get_glusterfs_user_request_body(Uid, Gid, StorageName)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable by
%% POSIX storage.
%% @end
%%-------------------------------------------------------------------
-spec get_posix_group_request_body(binary(), storage:name()) -> map().
get_posix_group_request_body(Gid, StorageName) ->
    maps:merge(#{
        <<"gid">> => Gid
    }, get_posix_generic_request_body_part(StorageName)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by POSIX storage.
%% @end
%%-------------------------------------------------------------------
-spec get_posix_user_request_body(binary(), binary(), storage:name()) -> map().
get_posix_user_request_body(Uid, Gid, StorageName) ->
    maps:merge(#{
        <<"uid">> => Uid,
        <<"gid">> => Gid
    }, get_posix_generic_request_body_part(StorageName)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns generic arguments for given POSIX storage in request body.
%% @end
%%-------------------------------------------------------------------
-spec get_posix_generic_request_body_part(storage:name()) -> map().
get_posix_generic_request_body_part(StorageName) ->
    #{
        <<"name">> => StorageName,
        <<"type">> => <<"posix">>
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable
%% by CEPH storage.
%% @end
%%-------------------------------------------------------------------
-spec get_ceph_group_request_body(binary(), storage:name()) -> map().
get_ceph_group_request_body(Gid, StorageName) ->
    {GroupName, Key} = get_ceph_group_credentials(Gid),
    maps:merge(#{
        <<"groupname">> => GroupName,   %todo jk check key name
        <<"key">> => Key
    }, get_ceph_generic_request_body_part(StorageName)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by CEPH storage.
%% @end
%%-------------------------------------------------------------------
-spec get_ceph_user_request_body(binary(), binary(), storage:name()) -> map().
get_ceph_user_request_body(Uid, Gid, StorageName) ->
    {UserName, Key} = get_ceph_user_credentials(Uid, Gid),
    maps:merge(#{
        <<"username">> => UserName,
        <<"key">> => Key
    }, get_ceph_generic_request_body_part(StorageName)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns generic arguments for given CEPH storage in request body.
%% @end
%%-------------------------------------------------------------------
-spec get_ceph_generic_request_body_part(storage:name()) -> map().
get_ceph_generic_request_body_part(StorageName) ->
    #{
        <<"name">> => StorageName,
        <<"type">> => <<"ceph">>
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable
%% by S3 storage.
%% @end
%%-------------------------------------------------------------------
-spec get_s3_group_request_body(binary(), storage:name()) -> map().
get_s3_group_request_body(Gid, StorageName) ->
    {_Key1, _Key2} = get_s3_group_credentials(Gid),
    maps:merge(#{
        %todo jk use abovee credentials
    }, get_s3_generic_request_body_part(StorageName)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by S3 storage.
%% @end
%%-------------------------------------------------------------------
-spec get_s3_user_request_body(binary(), binary(), storage:name()) -> map().
get_s3_user_request_body(Uid, Gid, StorageName) ->
    {AccessKey, SecretKey} = get_s3_user_credentials(Uid, Gid),
    maps:merge(#{
        <<"accessKey">> => AccessKey,
        <<"secretKey">> => SecretKey
    }, get_s3_generic_request_body_part(StorageName)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns generic arguments for given S3 storage in request body.
%% @end
%%-------------------------------------------------------------------
-spec get_s3_generic_request_body_part(storage:name()) -> map().
get_s3_generic_request_body_part(StorageName) ->
    #{
        <<"name">> => StorageName,
        <<"type">> => <<"s3">>
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by SWIFT storage.
%% @end
%%-------------------------------------------------------------------
-spec get_swift_group_request_body(binary(), storage:name()) -> map().
get_swift_group_request_body(Gid, StorageName) ->
    {_Key1, _Key2} = get_swift_group_credentials(Gid),
    maps:merge(#{
        %todo jk use abovee credentials
    }, get_swift_generic_request_body_part(StorageName)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by SWIFT storage.
%% @end
%%-------------------------------------------------------------------
-spec get_swift_user_request_body(binary(), binary(), storage:name()) -> map().
get_swift_user_request_body(Uid, Gid, StorageName) ->
    {Username, Password} = get_swift_user_credentials(Uid, Gid),
    maps:merge(#{
        <<"username">> => Username,
        <<"password">> => Password
    }, get_swift_generic_request_body_part(StorageName)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns generic arguments for given SWIFT storage in request body.
%% @end
%%-------------------------------------------------------------------
-spec get_swift_generic_request_body_part(storage:name()) -> map().
get_swift_generic_request_body_part(StorageName) ->
    #{
        <<"name">> => StorageName,
        <<"type">> => <<"swift">>
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable
%% by GLUSTERFS storage.
%% @end
%%-------------------------------------------------------------------
-spec get_glusterfs_group_request_body(binary(), storage:name()) -> map().
get_glusterfs_group_request_body(Gid, StorageName) ->
    {_Key1, _Key2} = get_glusterfs_group_credentials(Gid),
    maps:merge(#{
        %todo jk use abovee credentials
    }, get_glusterfs_generic_request_body_part(StorageName)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by GLUSTERFS storage.
%% @end
%%-------------------------------------------------------------------
-spec get_glusterfs_user_request_body(binary(), binary(), storage:name()) -> map().
get_glusterfs_user_request_body(Uid, Gid, StorageName) ->
    {_Key1, _Key2} = get_glusterfs_user_credentials(Uid, Gid),
    maps:merge(#{
        %todo jk use abovee credentials
    }, get_glusterfs_generic_request_body_part(StorageName)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns generic arguments for given GLUSTERFS storage in request body.
%% @end
%%-------------------------------------------------------------------
-spec get_glusterfs_generic_request_body_part(storage:name()) -> map().
get_glusterfs_generic_request_body_part(StorageName) ->
    #{
        <<"name">> => StorageName,
        <<"type">> => <<"glusterfs">>
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable
%% by CEPH storage.
%% @end
%%-------------------------------------------------------------------
-spec get_ceph_group_credentials(binary()) -> {binary(), binary()}.
get_ceph_group_credentials(_Gid) ->
    {<<"">>, <<"">>}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by CEPH storage.
%% @end
%%-------------------------------------------------------------------
-spec get_ceph_user_credentials(binary(), binary()) -> {binary(), binary()}.
get_ceph_user_credentials(_Uid, _Gid) ->
    {<<"">>, <<"">>}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable
%% by SWIFT storage.
%% @end
%%-------------------------------------------------------------------
-spec get_swift_group_credentials(binary()) -> {binary(), binary()}.
get_swift_group_credentials(_Gid) ->
    {<<"">>, <<"">>}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by SWIFT storage.
%% @end
%%-------------------------------------------------------------------
-spec get_swift_user_credentials(binary(), binary()) -> {binary(), binary()}.
get_swift_user_credentials(_Uid, _Gid) ->
    {<<"">>, <<"">>}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable
%% by S3 storage.
%% @end
%%-------------------------------------------------------------------
-spec get_s3_group_credentials(binary()) -> {binary(), binary()}.
get_s3_group_credentials(_Gid) ->
    {<<"">>, <<"">>}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by S3 storage.
%% @end
%%-------------------------------------------------------------------
-spec get_s3_user_credentials(binary(), binary()) -> {binary(), binary()}.
get_s3_user_credentials(_Uid, _Gid) ->
    {<<"">>, <<"">>}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable
%% by GLUSTERFS storage.
%% @end
%%-------------------------------------------------------------------
-spec get_glusterfs_group_credentials(binary()) -> {binary(), binary()}.
get_glusterfs_group_credentials(_Gid) ->
    {<<"">>, <<"">>}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by GLUSTERFS storage.
%% @end
%%-------------------------------------------------------------------
-spec get_glusterfs_user_credentials(binary(), binary()) -> {binary(), binary()}.
get_glusterfs_user_credentials(_Uid, _Gid) ->
    {<<"">>, <<"">>}.