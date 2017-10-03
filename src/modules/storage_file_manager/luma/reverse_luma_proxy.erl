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
-export([get_user_id/4, get_group_id/4]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Queries third party reverse LUMA service for id of user associated
%% with given Uid and Gid on storage named StorageId.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(map(), storage:id(), helper:name(),
    luma_config:config()) -> {ok, od_user:id()}.
get_user_id(Args, StorageId, HelperName, LumaConfig = #luma_config{url = LumaUrl}) ->
    Url = str_utils:format_bin("~s/resolve_user", [LumaUrl]),
    ReqHeaders = luma_proxy:get_request_headers(LumaConfig),
    ReqBody = get_user_request_body(Args, StorageId, HelperName),
    {ok, 200, _RespHeaders, RespBody} = http_client:post(Url, ReqHeaders, ReqBody),
    Response = json_utils:decode_map(RespBody),
    ProviderId = maps:get(<<"idp">>, Response),
    ProviderUserId = maps:get(<<"userId">>, Response),
    UserId = datastore_utils:gen_key(<<"">>, str_utils:format_bin("~p:~s",
        [ProviderId, ProviderUserId])),
    {ok, UserId}.

%%--------------------------------------------------------------------
%% @doc
%% Queries third party reverse LUMA service for id of group associated
%% with given Gid on storage named StorageId
%% @end
%%--------------------------------------------------------------------
-spec get_group_id(map(), storage:id(), helper:name(),
    luma_config:config()) -> {ok, od_group:id()}.
get_group_id(Args, StorageId, HelperName, LumaConfig = #luma_config{url = LumaUrl}) ->
    Url = str_utils:format_bin("~s/resolve_group", [LumaUrl]),
    ReqHeaders = luma_proxy:get_request_headers(LumaConfig),
    ReqBody = get_group_request_body(Args, StorageId, HelperName),
    {ok, 200, _RespHeaders, RespBody} = http_client:post(Url, ReqHeaders, ReqBody),
    Response = json_utils:decode_map(RespBody),
    Idp = maps:get(<<"idp">>, Response),
    IdpGroupId = maps:get(<<"groupId">>, Response),
    provider_logic:map_idp_group_to_onedata(Idp, IdpGroupId).

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
-spec get_group_request_body(map(), storage:name(), helper:name()) -> binary().
get_group_request_body(Args, StorageId, ?POSIX_HELPER_NAME) ->
    json_utils:encode_map(get_posix_request_body(Args, StorageId));
get_group_request_body(Args, StorageId, ?CEPH_HELPER_NAME) ->
    json_utils:encode_map(get_ceph_group_request_body(Args, StorageId));
get_group_request_body(Args, StorageId, ?S3_HELPER_NAME) ->
    json_utils:encode_map(get_s3_group_request_body(Args, StorageId));
get_group_request_body(Args, StorageId, ?SWIFT_HELPER_NAME) ->
    json_utils:encode_map(get_swift_group_request_body(Args, StorageId));
get_group_request_body(Args, StorageId, ?GLUSTERFS_HELPER_NAME) ->
    json_utils:encode_map(get_glusterfs_group_request_body(Args, StorageId)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by given storage.
%% @end
%%-------------------------------------------------------------------
-spec get_user_request_body(map(), storage:name(), helper:name()) -> binary().
get_user_request_body(Args, StorageId, ?POSIX_HELPER_NAME) ->
    json_utils:encode_map(get_posix_request_body(Args, StorageId));
get_user_request_body(Args, StorageId, ?CEPH_HELPER_NAME) ->
    json_utils:encode_map(get_ceph_user_request_body(Args, StorageId));
get_user_request_body(Args, StorageId, ?S3_HELPER_NAME) ->
    json_utils:encode_map(get_s3_user_request_body(Args, StorageId));
get_user_request_body(Args, StorageId, ?SWIFT_HELPER_NAME) ->
    json_utils:encode_map(get_swift_user_request_body(Args, StorageId));
get_user_request_body(Args, StorageId, ?GLUSTERFS_HELPER_NAME) ->
    json_utils:encode_map(get_glusterfs_user_request_body(Args, StorageId)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable by
%% POSIX storage.
%% @end
%%-------------------------------------------------------------------
-spec get_posix_request_body(map(), storage:name()) -> map().
get_posix_request_body(Args, StorageId) ->
    maps:merge(Args, get_posix_generic_request_body_part(StorageId)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns generic arguments for given POSIX storage in request body.
%% @end
%%-------------------------------------------------------------------
-spec get_posix_generic_request_body_part(storage:name()) -> map().
get_posix_generic_request_body_part(StorageId) ->
    #{
        <<"id">> => StorageId,
        <<"type">> => <<"posix">>
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable
%% by CEPH storage.
%% @end
%%-------------------------------------------------------------------
-spec get_ceph_group_request_body(map(), storage:name()) -> map().
get_ceph_group_request_body(#{
    <<"gid">> := Gid
}, StorageId) ->
    {GroupName, Key} = get_ceph_group_credentials(Gid),
    maps:merge(#{
        <<"groupname">> => GroupName,   %todo jk check key name
        <<"key">> => Key
    }, get_ceph_generic_request_body_part(StorageId)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by CEPH storage.
%% @end
%%-------------------------------------------------------------------
-spec get_ceph_user_request_body(map(), storage:name()) -> map().
get_ceph_user_request_body(#{
    <<"uid">> := Uid,
    <<"gid">> := Gid
}, StorageId) ->
    {UserName, Key} = get_ceph_user_credentials(Uid, Gid),
    maps:merge(#{
        <<"username">> => UserName,
        <<"key">> => Key
    }, get_ceph_generic_request_body_part(StorageId)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns generic arguments for given CEPH storage in request body.
%% @end
%%-------------------------------------------------------------------
-spec get_ceph_generic_request_body_part(storage:name()) -> map().
get_ceph_generic_request_body_part(StorageId) ->
    #{
        <<"id">> => StorageId,
        <<"type">> => <<"ceph">>
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable
%% by S3 storage.
%% @end
%%-------------------------------------------------------------------
-spec get_s3_group_request_body(map(), storage:name()) -> map().
get_s3_group_request_body(#{
    <<"gid">> := Gid
}, StorageId) ->
    {_Key1, _Key2} = get_s3_group_credentials(Gid),
    maps:merge(#{
        %todo jk use abovee credentials
    }, get_s3_generic_request_body_part(StorageId)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by S3 storage.
%% @end
%%-------------------------------------------------------------------
-spec get_s3_user_request_body(map(), storage:name()) -> map().
get_s3_user_request_body(#{
    <<"uid">> := Uid,
    <<"gid">> := Gid
}, StorageId) ->
    {AccessKey, SecretKey} = get_s3_user_credentials(Uid, Gid),
    maps:merge(#{
        <<"accessKey">> => AccessKey,
        <<"secretKey">> => SecretKey
    }, get_s3_generic_request_body_part(StorageId)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns generic arguments for given S3 storage in request body.
%% @end
%%-------------------------------------------------------------------
-spec get_s3_generic_request_body_part(storage:name()) -> map().
get_s3_generic_request_body_part(StorageId) ->
    #{
        <<"id">> => StorageId,
        <<"type">> => <<"s3">>
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by SWIFT storage.
%% @end
%%-------------------------------------------------------------------
-spec get_swift_group_request_body(map(), storage:name()) -> map().
get_swift_group_request_body(#{
    <<"gid">> := Gid
}, StorageId) ->
    {_Key1, _Key2} = get_swift_group_credentials(Gid),
    maps:merge(#{
        %todo jk use abovee credentials
    }, get_swift_generic_request_body_part(StorageId)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by SWIFT storage.
%% @end
%%-------------------------------------------------------------------
-spec get_swift_user_request_body(map(), storage:name()) -> map().
get_swift_user_request_body(#{
    <<"uid">> := Uid,
    <<"gid">> := Gid
}, StorageId) ->
    {Username, Password} = get_swift_user_credentials(Uid, Gid),
    maps:merge(#{
        <<"username">> => Username,
        <<"password">> => Password
    }, get_swift_generic_request_body_part(StorageId)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns generic arguments for given SWIFT storage in request body.
%% @end
%%-------------------------------------------------------------------
-spec get_swift_generic_request_body_part(storage:name()) -> map().
get_swift_generic_request_body_part(StorageId) ->
    #{
        <<"id">> => StorageId,
        <<"type">> => <<"swift">>
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable
%% by GLUSTERFS storage.
%% @end
%%-------------------------------------------------------------------
-spec get_glusterfs_group_request_body(map(), storage:name()) -> map().
get_glusterfs_group_request_body(#{
    <<"gid">> := Gid
}, StorageId) ->
    {_Key1, _Key2} = get_glusterfs_group_credentials(Gid),
    maps:merge(#{
        %todo jk use abovee credentials
    }, get_glusterfs_generic_request_body_part(StorageId)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by GLUSTERFS storage.
%% @end
%%-------------------------------------------------------------------
-spec get_glusterfs_user_request_body(map(), storage:name()) -> map().
get_glusterfs_user_request_body(#{
    <<"uid">> := Uid,
    <<"gid">> := Gid
}, StorageId) ->
    {_Key1, _Key2} = get_glusterfs_user_credentials(Uid, Gid),
    maps:merge(#{
        %todo jk use above credentials
    }, get_glusterfs_generic_request_body_part(StorageId)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns generic arguments for given GLUSTERFS storage in request body.
%% @end
%%-------------------------------------------------------------------
-spec get_glusterfs_generic_request_body_part(storage:name()) -> map().
get_glusterfs_generic_request_body_part(StorageId) ->
    #{
        <<"id">> => StorageId,
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