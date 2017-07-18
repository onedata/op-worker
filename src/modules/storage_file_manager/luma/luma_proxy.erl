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
-export([get_user_ctx/4]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Queries third party LUMA service for the storage user context.
%% @end
%%--------------------------------------------------------------------
-spec get_user_ctx(od_user:id(), od_space:id(), storage:doc(), storage:helper()) ->
    {ok, luma:user_ctx()} | {error, Reason :: term()}.
get_user_ctx(UserId, SpaceId, StorageDoc = #document{
    value = #storage{
        luma_config = #luma_config{
            url = LumaUrl,
            api_key = APIKey
}}}, Helper) ->

    Url = lists:flatten(io_lib:format("~s/map_user_credentials", [LumaUrl])),
    ReqHeaders = #{
        <<"X-Auth-Token">> => APIKey,
        <<"Content-Type">> => <<"application/json">>
    },
    ReqBody = get_request_body(UserId, SpaceId, StorageDoc, Helper),

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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs user context request that will be sent to the external LUMA service.
%% @end
%%--------------------------------------------------------------------
-spec get_request_body(od_user:id(), od_space:id(), storage:doc(), storage:helper()) ->
    Body :: binary().
get_request_body(UserId, SpaceId, StorageDoc, Helper) ->
    Body = [
        {<<"storageId">>, storage:get_id(StorageDoc)},
        {<<"storageName">>, storage:get_name(StorageDoc)},
        {<<"storageType">>, helper:get_name(Helper)},
        {<<"userDetails">>, get_user_details(UserId)},
        {<<"spaceId">>, SpaceId}
    ],
    json_utils:encode(Body).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs user details list.
%% @end
%%--------------------------------------------------------------------
-spec get_user_details(od_user:id()) -> UserDetails :: proplists:proplist().
get_user_details(UserId) ->
    case od_user:get(UserId) of
        {ok, #document{value = #od_user{connected_accounts = Accounts} = User}} ->
            [
                {<<"id">>, UserId},
                {<<"name">>, User#od_user.name},
                {<<"alias">>, User#od_user.alias},
                {<<"emailList">>, User#od_user.email_list},
                {<<"connectedAccounts">>, format_user_accounts(Accounts)}
            ];
        {error, _} ->
            [
                {<<"id">>, UserId},
                {<<"name">>, <<>>},
                {<<"alias">>, <<>>},
                {<<"emailList">>, []},
                {<<"connectedAccounts">>, []}
            ]
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Formats user OpenId accounts list.
%% @end
%%--------------------------------------------------------------------
-spec format_user_accounts(Accounts :: [proplists:proplist()]) ->
    FormattedAccounts :: [proplists:proplist()].
format_user_accounts(Accounts) ->
    Keys = [<<"providerId">>, <<"userId">>, <<"login">>, <<"name">>, <<"emailList">>],
    lists:map(fun(Account) ->
        Values = utils:get_values([<<"provider_id">>, <<"user_id">>, <<"login">>,
            <<"name">>, <<"email_list">>], Account),
        lists:zip(Keys, Values)
    end, Accounts).