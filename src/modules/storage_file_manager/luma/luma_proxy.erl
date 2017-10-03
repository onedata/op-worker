%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
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
-module(luma_proxy).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").

%% API
-export([get_user_ctx/5, get_request_headers/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Queries third party LUMA service for the storage user context.
%% @end
%%--------------------------------------------------------------------
-spec get_user_ctx(session:id(), od_user:id(), od_space:id(), storage:doc(), storage:helper()) ->
    {ok, luma:user_ctx()} | {error, Reason :: term()}.
get_user_ctx(SessionId, UserId, SpaceId, StorageDoc = #document{
    value = #storage{
        luma_config = LumaConfig = #luma_config{url = LumaUrl}
}}, Helper) ->
    Url = str_utils:format_bin("~s/map_user_credentials", [LumaUrl]),
    ReqHeaders = get_request_headers(LumaConfig),
    ReqBody = get_request_body(SessionId, UserId, SpaceId, StorageDoc),
    case http_client:post(Url, ReqHeaders, ReqBody) of
        {ok, 200, _RespHeaders, RespBody} ->
            UserCtx = json_utils:decode_map(RespBody),
            case helper:validate_user_ctx(Helper, UserCtx) of
                ok -> {ok, UserCtx};
                {error, Reason} -> {error, Reason}
            end;
        {ok, Code, _RespHeaders, RespBody} ->
            {error, {Code, json_utils:decode(RespBody)}};
        {error, Reason} ->
            {error, Reason}
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns LUMA request headers based on #luma_config.
%% @end
%%-------------------------------------------------------------------
-spec get_request_headers(luma_config:config()) -> map().
get_request_headers(#luma_config{api_key = undefined}) ->
    #{<<"Content-Type">> => <<"application/json">>};
get_request_headers(#luma_config{api_key = APIKey}) ->
    #{
        <<"Content-Type">> => <<"application/json">>,
        <<"X-Auth-Token">> => APIKey
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs user context request that will be sent to the external LUMA service.
%% @end
%%--------------------------------------------------------------------
-spec get_request_body(session:id(), od_user:id(), od_space:id(), storage:doc()) ->
    Body :: binary().
get_request_body(SessionId, UserId, SpaceId, StorageDoc) ->
    Body = #{
        <<"storageId">> => storage:get_id(StorageDoc),
        <<"storageName">> => storage:get_name(StorageDoc),
        <<"spaceId">> => SpaceId,
        <<"userDetails">> => get_user_details(SessionId, UserId)
    },
    json_utils:encode_map(Body).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs user details list.
%% @end
%%--------------------------------------------------------------------
-spec get_user_details(session:id(), od_user:id()) -> UserDetails :: maps:map().
get_user_details(SessionId, UserId) ->
    case user_logic:get_protected_data(SessionId, UserId) of
        {ok, #document{value = User}} ->
            #{
                <<"id">> => UserId,
                <<"name">> => User#od_user.name,
                <<"login">> => User#od_user.login,
                <<"connectedAccounts">> => User#od_user.linked_accounts,
                <<"emailList">> => User#od_user.email_list
            };
        {error, _} ->
            #{
                <<"id">> => UserId,
                <<"name">> => <<>>,
                <<"login">> => <<>>,
                <<"connectedAccounts">> => [],
                <<"emailList">> => []
            }
    end.
