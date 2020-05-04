%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains utility function associated with
%%% Local User Mapping (LUMA).
%%% @end
%%%-------------------------------------------------------------------
-module(luma_utils).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/storage/luma/external_luma.hrl").
-include_lib("ctool/include/http/headers.hrl").


%% API
-export([generate_uid/1, generate_gid/1, do_luma_request/3]).

%% Exported CT tests
-export([generate_posix_identifier/2, http_client_post/3]).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec generate_uid(od_user:id()) -> luma:uid().
generate_uid(UserId) ->
    {ok, UidRange} = application:get_env(?APP_NAME, uid_range),
    generate_posix_identifier(UserId, UidRange).

-spec generate_gid(od_space:id()) -> luma:gid().
generate_gid(SpaceId) ->
    {ok, GidRange} = application:get_env(?APP_NAME, gid_range),
    generate_posix_identifier(SpaceId, GidRange).

-spec do_luma_request(binary(), map(), storage:data()) -> http_client:response().
do_luma_request(Endpoint, Body, Storage) ->
    LumaConfig = storage:get_luma_config(Storage),
    Url = ?LUMA_URL(LumaConfig, Endpoint),
    ReqHeaders = prepare_request_headers(LumaConfig),
    luma_utils:http_client_post(Url, ReqHeaders, json_utils:encode(Body)).

%%%===================================================================
%%% Exported for CT tests
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Generates POSIX storage identifier (UID, GID) as a hash of passed Id.
%% @end
%%--------------------------------------------------------------------
-spec generate_posix_identifier(od_user:id() | od_space:id(),
    Range :: {non_neg_integer(), non_neg_integer()}) -> luma:uid() | luma:gid().
generate_posix_identifier(Id, {Low, High}) ->
    PosixId = crypto:bytes_to_integer(Id),
    Low + (PosixId rem (High - Low)).

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