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
-export([get_user_id/5]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Queries third party LUMA service for the storage user context.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(binary(), binary(), storage:name(), helper:name(),
    luma_config:config()) -> {ok, od_user:id()}.
get_user_id(Uid, Gid, StorageName, HelperName, LumaConfig = #luma_config{url = LumaUrl}) ->
    Url = lists:flatten(io_lib:format("~s/resolve_user_identity", [LumaUrl])),
    ReqHeaders = luma_proxy:get_request_headers(LumaConfig),
    ReqBody = get_request_body(Uid, Gid, StorageName, HelperName),
    {ok, 200, _RespHeaders, RespBody} = http_client:post(Url, ReqHeaders, ReqBody),
    Response = json_utils:decode_map(RespBody),
    ProviderId = maps:get(<<"providerId">>, Response),
    ProviderUserId = maps:get(<<"userId">>, Response),
    UserId = datastore_utils2:gen_key(<<"">>, str_utils:format_bin("~p:~s",
        [ProviderId, ProviderUserId])),
    {ok, UserId}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by given storage.
%% @end
%%-------------------------------------------------------------------
-spec get_request_body(binary(), binary(), storage:name(), helper:name()) -> binary().
get_request_body(Uid, Gid, StorageName, ?POSIX_HELPER_NAME) ->
    json_utils:encode_map(get_posix_request_body(Uid, Gid, StorageName));
get_request_body(Uid, Gid, StorageName, ?CEPH_HELPER_NAME) ->
    json_utils:encode_map(get_ceph_request_body_(Uid, Gid, StorageName));
get_request_body(Uid, Gid, StorageName, ?S3_HELPER_NAME) ->
    json_utils:encode_map(get_s3_request_body(Uid, Gid, StorageName));
get_request_body(Uid, Gid, StorageName, ?SWIFT_HELPER_NAME) ->
    json_utils:encode_map(get_swift_request_body(Uid, Gid, StorageName));
get_request_body(Uid, Gid, StorageName, ?GLUSTERFS_HELPER_NAME) ->
    json_utils:encode_map(get_glusterfs_request_body(Uid, Gid, StorageName)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by POSIX storage.
%% @end
%%-------------------------------------------------------------------
-spec get_posix_request_body(binary(), binary(), storage:name()) -> map().
get_posix_request_body(Uid, Gid, StorageName) ->
    #{
        <<"name">> => StorageName,
        <<"uid">> => Uid,
        <<"gid">> => Gid,
        <<"type">> => <<"posix">>
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by CEPH storage.
%% @end
%%-------------------------------------------------------------------
-spec get_ceph_request_body_(binary(), binary(), storage:name()) -> map().
get_ceph_request_body_(Uid, Gid, StorageName) ->
    {UserName, Key} = get_ceph_credentials(Uid, Gid),
    #{
        <<"name">> => StorageName,
        <<"username">> => UserName,
        <<"key">> => Key,
        <<"type">> => <<"ceph">>
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by S3 storage.
%% @end
%%-------------------------------------------------------------------
-spec get_s3_request_body(binary(), binary(), storage:name()) -> map().
get_s3_request_body(Uid, Gid, StorageName) ->
    {AccessKey, SecretKey} = get_s3_credentials(Uid, Gid),
    #{
        <<"name">> => StorageName,
        <<"accessKey">> => AccessKey,
        <<"secretKey">> => SecretKey,
        <<"type">> => <<"s3">>
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by SWIFT storage.
%% @end
%%-------------------------------------------------------------------
-spec get_swift_request_body(binary(), binary(), storage:name()) -> map().
get_swift_request_body(Uid, Gid, StorageName) ->
    {Username, Password} = get_swift_credentials(Uid, Gid),
    #{
        <<"name">> => StorageName,
        <<"username">> => Username,
        <<"password">> => Password,
        <<"type">> => <<"swift">>
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by GLUSTERFS storage.
%% @end
%%-------------------------------------------------------------------
-spec get_glusterfs_request_body(binary(), binary(), storage:name()) -> map().
get_glusterfs_request_body(_Uid, _Gid, StorageName) ->
    #{
        <<"name">> => StorageName,
        <<"type">> => <<"glusterfs">>
    }.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by CEPH storage.
%% @end
%%-------------------------------------------------------------------
-spec get_ceph_credentials(binary(), binary()) -> {binary(), binary()}.
get_ceph_credentials(_Uid, _Gid) ->
    {<<"">>, <<"">>}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by SWIFT storage.
%% @end
%%-------------------------------------------------------------------
-spec get_swift_credentials(binary(), binary()) -> {binary(), binary()}.
get_swift_credentials(_Uid, _Gid) ->
    {<<"">>, <<"">>}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Maps Uid and Gid from #statbuf to map of credentials understandable
%% by S3 storage.
%% @end
%%-------------------------------------------------------------------
-spec get_s3_credentials(binary(), binary()) -> {binary(), binary()}.
get_s3_credentials(_Uid, _Gid) ->
    {<<"">>, <<"">>}.