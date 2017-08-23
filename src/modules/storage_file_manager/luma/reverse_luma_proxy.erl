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
%% with given Uid and Gid on storage named StorageName.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(map(), storage:name(), helper:name(),
    luma_config:config()) -> {ok, od_user:id()}.
get_user_id(Args, StorageName, HelperName, LumaConfig = #luma_config{url = LumaUrl}) ->
    Url = lists:flatten(io_lib:format("~s/resolve_user_identity", [LumaUrl])),
    ReqHeaders = luma_proxy:get_request_headers(LumaConfig),
    ReqBody = get_user_request_body(Args, StorageName, HelperName),
    {ok, 200, _RespHeaders, RespBody} = http_client:post(Url, ReqHeaders, ReqBody),
    Response = json_utils:decode_map(RespBody),
    ProviderId = maps:get(<<"idp">>, Response),
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
-spec get_group_id(map(), storage:name(), helper:name(),
    luma_config:config()) -> {ok, od_user:id()}.
get_group_id(Args, StorageName, HelperName, LumaConfig = #luma_config{url = LumaUrl}) ->
    Url = lists:flatten(io_lib:format("~s/resolve_group", [LumaUrl])),
    ReqHeaders = luma_proxy:get_request_headers(LumaConfig),
    ReqBody = get_group_request_body(Args, StorageName, HelperName),
    {ok, 200, _RespHeaders, RespBody} = http_client:post(Url, ReqHeaders, ReqBody),
    ?critical("RespBody: ~p", [RespBody]),
    Response = json_utils:decode_map(RespBody),
    ?critical("Response: ~p", [Response]),
    Url2 = oz_plugin:get_oz_rest_endpoint("/provider/test/map_group"),
    ?critical("Url2: ~p", [Url2]),
    Headers = [{<<"Content-Type">>, <<"application/json">>}],
    {ok, 200, _RespHeaders2, RespBody2} = http_client:post(Url2, Headers, Response),
    ?critical("RespBody2: ~p", [RespBody2]),
    Response2 = json_utils:decode_map(RespBody2),
    ?critical("Response2: ~p", [Response2]),
    GroupId = maps:get(<<"groupId">>, Response2),
    {ok, GroupId}.

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
get_group_request_body(Args, StorageName, ?POSIX_HELPER_NAME) ->
    json_utils:encode_map(get_posix_request_body(Args, StorageName));
get_group_request_body(Args, StorageName, ?CEPH_HELPER_NAME) ->
    json_utils:encode_map(get_ceph_group_request_body(Args, StorageName));
get_group_request_body(Args, StorageName, ?S3_HELPER_NAME) ->
    json_utils:encode_map(get_s3_group_request_body(Args, StorageName));
get_group_request_body(Args, StorageName, ?SWIFT_HELPER_NAME) ->
    json_utils:encode_map(get_swift_group_request_body(Args, StorageName));
get_group_request_body(Args, StorageName, ?GLUSTERFS_HELPER_NAME) ->
    json_utils:encode_map(get_glusterfs_group_request_body(Args, StorageName)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by given storage.
%% @end
%%-------------------------------------------------------------------
-spec get_user_request_body(map(), storage:name(), helper:name()) -> binary().
get_user_request_body(Args, StorageName, ?POSIX_HELPER_NAME) ->
    json_utils:encode_map(get_posix_request_body(Args, StorageName));
get_user_request_body(Args, StorageName, ?CEPH_HELPER_NAME) ->
    json_utils:encode_map(get_ceph_user_request_body(Args, StorageName));
get_user_request_body(Args, StorageName, ?S3_HELPER_NAME) ->
    json_utils:encode_map(get_s3_user_request_body(Args, StorageName));
get_user_request_body(Args, StorageName, ?SWIFT_HELPER_NAME) ->
    json_utils:encode_map(get_swift_user_request_body(Args, StorageName));
get_user_request_body(Args, StorageName, ?GLUSTERFS_HELPER_NAME) ->
    json_utils:encode_map(get_glusterfs_user_request_body(Args, StorageName)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Gid from #statbuf to map of credentials understandable by
%% POSIX storage.
%% @end
%%-------------------------------------------------------------------
-spec get_posix_request_body(map(), storage:name()) -> map().
get_posix_request_body(Args, StorageName) ->
    maps:merge(Args, get_posix_generic_request_body_part(StorageName)).

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
-spec get_ceph_group_request_body(map(), storage:name()) -> map().
get_ceph_group_request_body(#{
    <<"gid">> := Gid
}, StorageName) ->
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
-spec get_ceph_user_request_body(map(), storage:name()) -> map().
get_ceph_user_request_body(#{
    <<"uid">> := Uid,
    <<"gid">> := Gid
}, StorageName) ->
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
-spec get_s3_group_request_body(map(), storage:name()) -> map().
get_s3_group_request_body(#{
    <<"gid">> := Gid
}, StorageName) ->
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
-spec get_s3_user_request_body(map(), storage:name()) -> map().
get_s3_user_request_body(#{
    <<"uid">> := Uid,
    <<"gid">> := Gid
}, StorageName) ->
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
-spec get_swift_group_request_body(map(), storage:name()) -> map().
get_swift_group_request_body(#{
    <<"gid">> := Gid
}, StorageName) ->
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
-spec get_swift_user_request_body(map(), storage:name()) -> map().
get_swift_user_request_body(#{
    <<"uid">> := Uid,
    <<"gid">> := Gid
}, StorageName) ->
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
-spec get_glusterfs_group_request_body(map(), storage:name()) -> map().
get_glusterfs_group_request_body(#{
    <<"gid">> := Gid
}, StorageName) ->
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
-spec get_glusterfs_user_request_body(map(), storage:name()) -> map().
get_glusterfs_user_request_body(#{
    <<"uid">> := Uid,
    <<"gid">> := Gid
}, StorageName) ->
    {_Key1, _Key2} = get_glusterfs_user_credentials(Uid, Gid),
    maps:merge(#{
        %todo jk use above credentials
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