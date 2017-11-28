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
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_user_ctx/4, get_request_headers/1, get_group_ctx/4]).

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
        luma_config = LumaConfig = #luma_config{url = LumaUrl}
}}, Helper) ->

    Url = lists:flatten(io_lib:format("~s/map_user_credentials", [LumaUrl])),
    ReqHeaders = get_request_headers(LumaConfig),
    ReqBody = get_request_body(UserId, SpaceId, StorageDoc),
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
%% Queries third party LUMA service for the storage GID for given GroupId.
%% @end
%%-------------------------------------------------------------------
-spec get_group_ctx(od_group:id() | undefined, od_space:id(), storage:doc(), helper:name()) ->
    {ok, luma:group_ctx()} | {error, term()}.
get_group_ctx(_GroupId, _SpaceId, _StorageDoc, #helper{name = ?CEPH_HELPER_NAME}) ->
    undefined;
get_group_ctx(_GroupId, _SpaceId, _StorageDoc, #helper{name = ?S3_HELPER_NAME}) ->
    undefined;
get_group_ctx(_GroupId, _SpaceId, _StorageDoc, #helper{name = ?SWIFT_HELPER_NAME}) ->
    undefined;
get_group_ctx(GroupId, SpaceId, StorageDoc = #document{
    value = #storage{
        luma_config = LumaConfig = #luma_config{url = LumaUrl}
}}, Helper) ->
    Url = lists:flatten(io_lib:format("~s/map_group", [LumaUrl])),
    ReqHeaders = get_request_headers(LumaConfig),
    ReqBody = get_group_request_body(GroupId, SpaceId, StorageDoc),
    case http_client:post(Url, ReqHeaders, ReqBody) of
        {ok, 200, _RespHeaders, RespBody} ->
            GroupCtx = json_utils:decode_map(RespBody),
            case helper:validate_group_ctx(Helper, GroupCtx) of
                ok ->
                    {ok, GroupCtx};
                Error = {error, Reason} ->
                    ?error_stacktrace("Invalid group ctx returned from map_group request: ~p", [Reason]),
                    Error
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
-spec get_request_body(od_user:id(), od_space:id(), storage:doc()) ->
    Body :: binary().
get_request_body(UserId, SpaceId, StorageDoc) ->
    Body = [
        {<<"storageId">>, storage:get_id(StorageDoc)},
        {<<"storageName">>, storage:get_name(StorageDoc)},
        {<<"spaceId">>, SpaceId},
        {<<"userDetails">>, get_user_details(UserId)}
    ],
    json_utils:encode(Body).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs user context request that will be sent to the external LUMA service.
%% @end
%%--------------------------------------------------------------------
-spec get_group_request_body(od_group:id() | undefined, od_space:id(), storage:doc()) ->
    Body :: binary().
get_group_request_body(undefined, SpaceId, #document{
    key = StorageId,
    value = #storage{name = StorageName}
}) ->
    Body = [
        {<<"spaceId">>, SpaceId},
        {<<"storageId">>, StorageId},
        {<<"storageName">>, StorageName}
    ],
    json_utils:encode(Body);
get_group_request_body(GroupId, SpaceId, #document{
    key = StorageId,
    value = #storage{name = StorageName}
}) ->
    Body = [
        {<<"groupId">>, GroupId},
        {<<"spaceId">>, SpaceId},
        {<<"storageId">>, StorageId},
        {<<"storageName">>, StorageName}
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
                {<<"connectedAccounts">>, format_user_accounts(Accounts)},
                {<<"login">>, User#od_user.alias},
                {<<"emailList">>, User#od_user.email_list}
            ];
        {error, _} ->
            [
                {<<"id">>, UserId},
                {<<"name">>, <<>>},
                {<<"connectedAccounts">>, []},
                {<<"login">>, <<>>},
                {<<"emailList">>, []}
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
    Keys = [
        <<"idp">>, <<"userId">>, <<"login">>, <<"name">>,
        <<"emailList">>, <<"groups">>
    ],
    lists:map(fun(Account) ->
        Values = utils:get_values([<<"provider_id">>, <<"user_id">>, <<"login">>,
            <<"name">>, <<"email_list">>, <<"groups">>], Account),
        lists:zip(Keys, Values)
    end, Accounts).